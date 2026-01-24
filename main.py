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
from curl_cffi import requests as cffi_requests, CurlHttpVersion
import translators as ts

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

OUTPUT_DIR = Path("articles")
CATALOG_PATH = OUTPUT_DIR / "catalog.json"
MAX_RETRIES = 3
BASE_DELAY = 1.0

# Синхронизация: Помним 100, ищем 100 (чтобы не было пропусков)
MAX_POSTED_RECORDS = 100 
FETCH_DEPTH = 100 

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
            if f.is_dir():
                parts = f.name.split('_', 1)
                if parts and parts[0].isdigit():
                    if parts[0] not in ids_to_keep:
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

# --- УМНЫЙ ПОИСК КАРТИНОК (ОБНОВЛЕННЫЙ) ---
def extract_img_url(img_tag: Any) -> Optional[str]:
    """
    Извлекает URL картинки, игнорируя мусор и мелкие миниатюры (300x200 и т.д.)
    """
    # 1. Проверка атрибутов размера (width/height)
    # Если в теге прямо написано width="300" или меньше — выкидываем
    width_attr = img_tag.get("width")
    if width_attr and width_attr.isdigit():
        if int(width_attr) < 400: # Минимальная ширина 400px
            return None

    # Вспомогательная функция проверки URL на "мелкость"
    def is_low_res(url_str: str) -> bool:
        url_lower = url_str.lower()
        # Фильтр мусорных слов
        bad_words = ["gif", "logo", "banner", "mastercard", "aba-", "payway", "icon", "button", "author", "avatar"]
        if any(bw in url_lower for bw in bad_words): return True
        
        # Фильтр размеров WordPress (-300x200, -150x150 и т.д.)
        # Ищем паттерн: тире + 3 цифры + x + цифры
        if re.search(r'-\d{3}x\d{2,3}\.', url_str):
            return True
        return False

    # 2. Поиск в SRCSET (берем самое большое)
    srcset = img_tag.get("srcset") or img_tag.get("data-srcset")
    if srcset:
        try:
            parts = srcset.split(',')
            links = []
            for p in parts:
                # Ищем пары "URL РАЗМЕРw"
                match = re.search(r'(\S+)\s+(\d+)w', p.strip())
                if match: 
                    w_val = int(match.group(2))
                    u_val = match.group(1)
                    if w_val >= 400: # Берем только если ширина >= 400
                        links.append((w_val, u_val))
            
            if links:
                # Сортируем по убыванию ширины и берем самую большую
                best_link = sorted(links, key=lambda x: x[0], reverse=True)[0][1]
                if not is_low_res(best_link):
                    return best_link.split('?')[0] # Чистим query параметры
        except Exception: pass
    
    # 3. Поиск в обычных атрибутах (Fallback)
    attrs = ["data-orig-file", "data-large-file", "data-src", "data-lazy-src", "src"]
    for attr in attrs:
        if val := img_tag.get(attr):
            clean_val = val.split()[0].split(',')[0].split('?')[0]
            if not is_low_res(clean_val):
                return clean_val
                
    return None

# --- ПЕРЕВОД (Функция-заглушка для ai_main) ---
def translate_text(text: str, to_lang: str = "ru") -> Optional[str]:
    # Эта функция будет перезаписана из ai_main.py
    # Оставляем простую реализацию на случай запуска без AI
    if not text: return ""
    try:
        return ts.translate_text(text, translator="google", from_language="en", to_language=to_lang)
    except: return text

# --- ЗАПРОСЫ ---
def fetch_cat_id(url, slug):
    r = SCRAPER.get(f"{url}/wp-json/wp/v2/categories?slug={slug}", timeout=SCRAPER_TIMEOUT)
    r.raise_for_status(); data=r.json()
    if not data: raise RuntimeError("Cat not found")
    return data[0]["id"]

def fetch_posts(url, cid, limit):
    logging.info(f"Запрашиваем {limit} последних статей из API...") 
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
    fn = url.rsplit('/',1)[-1].split('?',1)[0]
    # Защита от дурацких имен файлов
    if len(fn) > 50: fn = hashlib.md5(fn.encode()).hexdigest() + ".jpg"
    dest = folder / fn
    
    try:
        dest.write_bytes(SCRAPER.get(url, timeout=SCRAPER_TIMEOUT).content)
        return str(dest)
    except Exception: return None

# --- ПАРСИНГ ---
def parse_and_save(post, lang, stopwords):
    time.sleep(2) # Небольшая пауза
    aid, slug, link = str(post["id"]), post["slug"], post.get("link")
    
    raw_title = BeautifulSoup(post["title"]["rendered"], "html.parser").get_text(strip=True)
    title = sanitize_text(raw_title)

    if stopwords:
        for ph in stopwords:
            if ph in title.lower():
                logging.info(f"🚫 ID={aid}: Стоп-слово '{ph}'")
                return None

    try:
        html_txt = SCRAPER.get(link, timeout=SCRAPER_TIMEOUT).text
    except Exception: return None

    # Хэш проверка
    meta_path = OUTPUT_DIR / f"{aid}_{slug}" / "meta.json"
    curr_hash = hashlib.sha256(html_txt.encode()).hexdigest()
    if meta_path.exists():
        try:
            m = json.loads(meta_path.read_text(encoding="utf-8"))
            if m.get("hash") == curr_hash:
                logging.info(f"⏭️ ID={aid}: Без изменений.")
                return m
        except: pass

    logging.info(f"Processing ID={aid}: {title[:30]}...")

    soup = BeautifulSoup(html_txt, "html.parser")
    
    # ЧИСТКА ОТ МУСОРА
    for r in soup.find_all("div", class_="post-widget-thumbnail"): r.decompose()
    for j in soup.find_all(["span", "div", "script", "style", "iframe"]):
        if not hasattr(j, 'attrs') or j.attrs is None: continue 
        c = str(j.get("class", ""))
        if j.get("data-mce-type") or "mce_SELRES" in c or "widget" in c: j.decompose()

    # СБОР КОНТЕНТА
    paras = []
    if c_div := soup.find("div", class_="entry-content"):
        for r in c_div.find_all(["ul", "ol", "div"], class_=re.compile(r"rp4wp|related|ad-")): r.decompose()
        paras = [sanitize_text(p.get_text(strip=True)) for p in c_div.find_all("p")]
    
    # СБОР КАРТИНОК (С ФИЛЬТРАЦИЕЙ)
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
    
    # FALLBACK НА FEATURED IMAGE (Если в тексте пусто)
    if not images and "_embedded" in post and (m:=post["_embedded"].get("wp:featuredmedia")):
        if isinstance(m, list) and (u:=m[0].get("source_url")):
             # Тут тоже проверяем, не подсовывают ли нам мелочь (хотя featured обычно ок)
             if "300x200" not in u and "150x150" not in u:
                if p:=save_image(u, OUTPUT_DIR / f"{aid}_{slug}" / "images"): images.append(p)

    if not images:
        logging.warning(f"⚠️ ID={aid}: Нет норм картинок (все отсеяны). Skip.")
        return None

    # ПЕРЕВОД
    # Примечание: Если запущен ai_main, translate_text будет ИИ-функцией
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

    # Если мы в режиме ИИ, текст переводится сразу при обработке translate_with_ai (как Summary)
    # Если мы в обычном режиме, переводим тут
    if lang:
        # Проверяем, не перевели ли мы уже текст внутри функции (для AI_Main это актуально)
        # Но для простоты: переводим чистый текст
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
        posts = fetch_posts(args.base_url, cid, FETCH_DEPTH)
        
        posted = load_posted_ids(Path(args.posted_state_file))
        stop = load_stopwords(Path(args.stopwords_file))
        catalog = []
        if CATALOG_PATH.exists():
            with open(CATALOG_PATH, 'r') as f: catalog=json.load(f)

        processed = []
        count = 0
        
        logging.info(f"В API {len(posts)} постов. Ищем новые...")
        
        for post in posts:
            if count >= args.limit: 
                logging.info(f"Лимит {args.limit} достигнут."); break
            
            if str(post["id"]) in posted: continue
                
            if meta := parse_and_save(post, args.lang, stop):
                processed.append(meta)
                count += 1

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
