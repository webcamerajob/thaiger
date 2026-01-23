import argparse
import logging
import json
import hashlib
import time
import re
import os
import shutil
import html
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Set
import fcntl

from bs4 import BeautifulSoup
# ИСПОЛЬЗУЕМ БЫСТРЫЙ ДВИЖОК
from curl_cffi import requests as cffi_requests, CurlHttpVersion
import translators as ts

# --- ИЗМЕНЕНИЕ: Включаем уровень DEBUG для подробностей ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

OUTPUT_DIR = Path("articles")
CATALOG_PATH = OUTPUT_DIR / "catalog.json"
MAX_RETRIES = 3
BASE_DELAY = 1.0
MAX_POSTED_RECORDS = 100

SCRAPER = cffi_requests.Session(
    impersonate="safari15_5",
    http_version=CurlHttpVersion.V2_0
)

SCRAPER.headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.5 Safari/605.1.15",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/"
}
SCRAPER_TIMEOUT = 30 
BAD_RE = re.compile(r"[\u200b-\u200f\uFEFF\u200E\u00A0]")

# --- ОЧИСТКА ---
def cleanup_old_articles(posted_ids_path: Path, articles_dir: Path):
    if not posted_ids_path.is_file() or not articles_dir.is_dir(): return
    try:
        with open(posted_ids_path, 'r', encoding='utf-8') as f:
            all_posted = json.load(f)
            ids_to_keep = set(str(x) for x in all_posted[-MAX_POSTED_RECORDS:])
        cleaned = 0
        for f in articles_dir.iterdir():
            if f.is_dir() and f.name.split('_', 1)[0].isdigit():
                if f.name.split('_', 1)[0] not in ids_to_keep:
                    shutil.rmtree(f); cleaned += 1
        if cleaned: logging.info(f"🧹 Удалено {cleaned} старых папок.")
    except Exception: pass

# --- ВСПОМОГАТЕЛЬНЫЕ ---
def normalize_text(text: str) -> str:
    for s, v in {'–': '-', '—': '-', '“': '"', '”': '"', '‘': "'", '’': "'"}.items(): text = text.replace(s, v)
    return text

def sanitize_text(text: str) -> str:
    if not text: return ""
    text = html.unescape(text)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'mce_SELRES_[^ ]+', '', text)
    return re.sub(r'\n{3,}', '\n\n', text).strip()

def load_posted_ids(state_file_path: Path) -> Set[str]:
    try:
        if state_file_path.exists():
            with open(state_file_path, 'r', encoding='utf-8') as f:
                fcntl.flock(f, fcntl.LOCK_SH)
                return {str(item) for item in json.load(f)}
        return set()
    except Exception: return set()

def load_stopwords(file_path: Optional[Path]) -> List[str]:
    if not file_path or not file_path.exists(): return []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return [line.strip().lower() for line in f if line.strip()]
    except Exception: return []

# --- УМНЫЙ ПОИСК КАРТИНОК ---
def extract_img_url(img_tag: Any) -> Optional[str]:
    # 1. Srcset
    srcset = img_tag.get("srcset") or img_tag.get("data-srcset")
    if srcset:
        try:
            parts = srcset.split(',')
            links = []
            for p in parts:
                match = re.search(r'(\S+)\s+(\d+)w', p.strip())
                if match: links.append((int(match.group(2)), match.group(1)))
            if links: return sorted(links, key=lambda x: x[0], reverse=True)[0][1]
        except Exception: pass
    
    # 2. Attributes
    for attr in ["data-orig-file", "data-large-file", "data-src", "data-lazy-src", "src"]:
        if val := img_tag.get(attr):
            clean_val = val.split()[0].split(',')[0].split('?')[0]
            # ЛОГ ОТКАЗА (ФИЛЬТР МУСОРА)
            for bad in ["gif", "logo", "banner", "mastercard", "aba-", "payway", "icon", "button", "author"]:
                if bad in clean_val.lower():
                    # logging.info(f"   🗑️ Картинка отброшена (фильтр '{bad}'): {clean_val}") 
                    return None
            return clean_val
    return None

# --- ПЕРЕВОД ---
PROVIDER_LIMITS = {"google": 4800, "bing": 4500, "yandex": 4000}
def chunk_text(text, limit):
    c = []
    while text:
        if len(text)<=limit: c.append(text); break
        sp = text.rfind('\n\n',0,limit)
        if sp==-1: sp=text.rfind('. ',0,limit)
        if sp==-1: sp=limit
        end=max(1,sp+(2 if text[sp:sp+2]=='\n\n' else 1))
        c.append(text[:end]); text=text[end:].lstrip()
    return c

def translate_text(text: str, to_lang: str = "ru") -> Optional[str]:
    if not text: return ""
    norm = normalize_text(text)
    for p in ["yandex", "google", "bing"]:
        try:
            res = []
            for ch in chunk_text(norm, PROVIDER_LIMITS.get(p, 3000)):
                time.sleep(0.5)
                res.append(ts.translate_text(ch, translator=p, from_language="en", to_language=to_lang, timeout=45))
            return "".join(res)
        except Exception: continue
    return None

# --- ЗАПРОСЫ ---
def fetch_cat_id(url, slug):
    r = SCRAPER.get(f"{url}/wp-json/wp/v2/categories?slug={slug}", timeout=SCRAPER_TIMEOUT)
    r.raise_for_status(); data=r.json()
    if not data: raise RuntimeError("Cat not found")
    return data[0]["id"]

def fetch_posts(url, cid, limit):
    logging.info(f"Запрашиваем {limit} последних статей из API...") # Лог
    time.sleep(2)
    try:
        r = SCRAPER.get(f"{url}/wp-json/wp/v2/posts?categories={cid}&per_page={limit}&_embed", timeout=SCRAPER_TIMEOUT)
        if r.status_code==429: time.sleep(20)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logging.error(f"Ошибка получения постов: {e}")
        return []

def save_image(url, folder):
    folder.mkdir(parents=True, exist_ok=True)
    dest = folder / url.rsplit('/',1)[-1].split('?',1)[0]
    try:
        dest.write_bytes(SCRAPER.get(url, timeout=SCRAPER_TIMEOUT).content)
        return str(dest)
    except Exception: return None

# --- ПАРСИНГ ---
def parse_and_save(post, lang, stopwords):
    time.sleep(4)
    aid, slug, link = str(post["id"]), post["slug"], post.get("link")
    
    raw_title = BeautifulSoup(post["title"]["rendered"], "html.parser").get_text(strip=True)
    title = sanitize_text(raw_title)

    # 1. Проверка СТОП-СЛОВ
    if stopwords:
        for ph in stopwords:
            if ph in title.lower():
                logging.info(f"🚫 ID={aid}: Стоп-слово '{ph}' в заголовке.")
                return None

    try:
        html_txt = SCRAPER.get(link, timeout=SCRAPER_TIMEOUT).text
    except Exception: return None

    # 2. Проверка ИЗМЕНЕНИЙ (Хэш)
    meta_path = OUTPUT_DIR / f"{aid}_{slug}" / "meta.json"
    curr_hash = hashlib.sha256(html_txt.encode()).hexdigest()
    if meta_path.exists():
        try:
            m = json.loads(meta_path.read_text(encoding="utf-8"))
            if m.get("hash") == curr_hash and m.get("translated_to", "") == lang:
                logging.info(f"⏭️ ID={aid}: Статья не изменилась. Пропуск.")
                return m
        except: pass

    logging.info(f"Processing ID={aid}: {title[:30]}...")

    soup = BeautifulSoup(html_txt, "html.parser")
    
    # УДАЛЯЕМ МУСОР (включая Related)
    for r in soup.find_all("div", class_="post-widget-thumbnail"): r.decompose()
    for j in soup.find_all(["span", "div", "script", "style", "iframe"]):
        if not hasattr(j, 'attrs') or j.attrs is None: continue # ФИКС ОТ noneType
        c = str(j.get("class", ""))
        if j.get("data-mce-type") or "mce_SELRES" in c or "widget" in c: j.decompose()

    # ТЕКСТ
    paras = []
    if c_div := soup.find("div", class_="entry-content"):
        for r in c_div.find_all(["ul", "ol", "div"], class_=re.compile(r"rp4wp|related|ad-")): r.decompose()
        paras = [sanitize_text(p.get_text(strip=True)) for p in c_div.find_all("p")]
    
    # КАРТИНКИ
    srcs = set()
    if c_div:
        for img in c_div.find_all("img"):
            if u := extract_img_url(img): srcs.add(u)
    
    images = []
    if srcs:
        with ThreadPoolExecutor(5) as ex:
            futs = {ex.submit(save_image, u, OUTPUT_DIR / f"{aid}_{slug}" / "images"): u for u in list(srcs)[:10]}
            for f in as_completed(futs):
                if p:=f.result(): images.append(p)
    
    # FALLBACK НА FEATURED
    if not images and "_embedded" in post and (m:=post["_embedded"].get("wp:featuredmedia")):
        if isinstance(m, list) and (u:=m[0].get("source_url")):
             if p:=save_image(u, OUTPUT_DIR / f"{aid}_{slug}" / "images"): images.append(p)

    # 3. Проверка НАЛИЧИЯ КАРТИНОК
    if not images:
        logging.warning(f"⚠️ ID={aid}: Все картинки отфильтрованы или отсутствуют. Пропуск статьи.")
        return None

    # ПЕРЕВОД И СОХРАНЕНИЕ
    final_title = translate_text(title, lang) if lang else title
    final_title = sanitize_text(final_title)
    
    art_dir = OUTPUT_DIR / f"{aid}_{slug}"
    art_dir.mkdir(parents=True, exist_ok=True)
    
    raw_txt_clean = BAD_RE.sub("", "\n\n".join(paras))
    (art_dir / "content.txt").write_text(raw_txt_clean, encoding="utf-8")
    
    meta = {
        "id": aid, "slug": slug, "date": post.get("date"), "link": link,
        "title": final_title, "text_file": "content.txt",
        "images": sorted([Path(p).name for p in images]), "posted": False,
        "hash": curr_hash, "translated_to": ""
    }

    if lang:
        tr_text = translate_text(raw_txt_clean, lang)
        if tr_text:
            (art_dir / f"content.{lang}.txt").write_text(f"{final_title}\n\n{tr_text}", encoding="utf-8")
            meta.update({"translated_to": lang, "text_file": f"content.{lang}.txt"})

    with open(meta_path, "w", encoding="utf-8") as f: json.dump(meta, f, ensure_ascii=False, indent=2)
    return meta

# --- MAIN ---
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--slug", default="national")
    parser.add_argument("-n", "--limit", type=int, default=10)
    parser.add_argument("-l", "--lang", default="ru")
    parser.add_argument("--posted-state-file", default="articles/posted.json")
    parser.add_argument("--stopwords-file", default="stopwords.txt")
    args = parser.parse_args()

    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        cleanup_old_articles(Path(args.posted_state_file), OUTPUT_DIR)
        
        cid = fetch_cat_id(args.base_url, args.slug)
        
        # --- ВАЖНОЕ ИЗМЕНЕНИЕ ---
        # Запрашиваем 60 постов, чтобы "пробить" слой старых статей
        posts = fetch_posts(args.base_url, cid, 60)
        
        posted = load_posted_ids(Path(args.posted_state_file))
        stop = load_stopwords(Path(args.stopwords_file))
        catalog = []
        if CATALOG_PATH.exists():
            with open(CATALOG_PATH, 'r') as f: catalog=json.load(f)

        processed = []
        count = 0
        
        logging.info(f"Найдено {len(posts)} постов в API (будем искать новые среди них).")
        
        for post in posts:
            if count >= args.limit: 
                logging.info(f"Достигнут лимит обработки ({args.limit}). Остановка."); break
            
            # Если статья уже есть - просто идем дальше
            if str(post["id"]) in posted:
                # logging.info(f"⏭️ ID={post['id']} уже был. Ищем дальше...") # Раскомментируй, если интересно
                continue
                
            if meta := parse_and_save(post, args.lang, stop):
                processed.append(meta)
                count += 1
            else:
                pass # Пропуск по причине фильтров (картинки/стоп-слова)

        if processed:
            for m in processed:
                catalog = [i for i in catalog if i.get("id") != m["id"]]
                catalog.append(m)
            with open(CATALOG_PATH, "w", encoding="utf-8") as f:
                json.dump(catalog, f, ensure_ascii=False, indent=2)
            print("NEW_ARTICLES_STATUS:true")
        else:
            print("NEW_ARTICLES_STATUS:false")

    except Exception:
        logging.exception("Fatal error:")
        exit(1)

if __name__ == "__main__":
    main()



