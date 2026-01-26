import argparse
import logging
import json
import hashlib
import time
import re
import os
import shutil
import html
import fcntl
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Set

# Используем стандартный requests для перевода (стабильно)
import requests 
from bs4 import BeautifulSoup
from curl_cffi import requests as cffi_requests, CurlHttpVersion

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

OUTPUT_DIR = Path("articles")
CATALOG_PATH = OUTPUT_DIR / "catalog.json"
MAX_RETRIES = 3
BASE_DELAY = 1.0

# Синхронизация: Помним 100, ищем 100
MAX_POSTED_RECORDS = 100 
FETCH_DEPTH = 100 

# --- НАСТРОЙКИ СЕТИ (PARSER) ---
# Chrome + HTTP/1.1 = Максимальная стабильность для WordPress
SCRAPER = cffi_requests.Session(
    impersonate="chrome110",
    http_version=CurlHttpVersion.V1_1
)

SCRAPER.headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/"
}
SCRAPER_TIMEOUT = 30 
BAD_RE = re.compile(r"[\u200b-\u200f\uFEFF\u200E\u00A0]")

# --- ПРЯМОЙ ПЕРЕВОДЧИК (GOOGLE GTX) ---
def translate_text(text: str, to_lang: str = "ru") -> str:
    """
    Переводит текст через Google API, разбивая на безопасные чанки (~1800 символов).
    """
    if not text: return ""
    
    # Разбиваем текст на куски, чтобы URL не превысил лимит
    chunks = []
    current_chunk = ""
    for paragraph in text.split('\n'):
        # Если сам абзац гигантский, режем его (редкий случай)
        if len(paragraph) > 1800:
            if current_chunk:
                chunks.append(current_chunk)
                current_chunk = ""
            chunks.append(paragraph)
            continue

        if len(current_chunk) + len(paragraph) < 1800:
            current_chunk += paragraph + "\n"
        else:
            chunks.append(current_chunk)
            current_chunk = paragraph + "\n"
    if current_chunk: chunks.append(current_chunk)
    
    translated_parts = []
    url = "https://translate.googleapis.com/translate_a/single"
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"}
    
    for chunk in chunks:
        if not chunk.strip():
            translated_parts.append("")
            continue
        try:
            params = {
                "client": "gtx", "sl": "en", "tl": to_lang, "dt": "t", "q": chunk.strip()
            }
            # Таймаут 10 сек на кусок
            r = requests.get(url, params=params, headers=headers, timeout=10)
            
            if r.status_code == 200:
                data = r.json()
                # Склеиваем ответ (Google возвращает массив массивов)
                text_part = "".join([item[0] for item in data[0] if item and item[0]])
                translated_parts.append(text_part)
            else:
                logging.warning(f"⚠️ Ошибка перевода куска: {r.status_code}")
                translated_parts.append(chunk) # Возвращаем оригинал при сбое
            
            time.sleep(0.3) # Микро-пауза
        except Exception as e:
            logging.error(f"⚠️ Сбой перевода: {e}")
            translated_parts.append(chunk)
            
    return "\n".join(translated_parts)

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

# --- ПОИСК КАРТИНОК (Классический метод) ---
def extract_img_url(img_tag: Any) -> Optional[str]:
    attributes_to_check = [
        "data-brsrcset", "data-breeze", "data-src", "data-lazy-src",
        "data-original", "srcset", "src",
    ]
    for attr in attributes_to_check:
        if src_val := img_tag.get(attr):
            return src_val.split(',')[0].split()[0]
    return None

def save_image(url, folder):
    folder.mkdir(parents=True, exist_ok=True)
    fn = url.rsplit('/',1)[-1].split('?',1)[0]
    if len(fn) > 50: fn = hashlib.md5(fn.encode()).hexdigest() + ".jpg"
    dest = folder / fn
    try:
        dest.write_bytes(SCRAPER.get(url, timeout=SCRAPER_TIMEOUT).content)
        return str(dest)
    except Exception: return None

# --- ЗАПРОСЫ К API ---
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

# --- ОСНОВНОЙ ПАРСИНГ ---
def parse_and_save(post, lang, stopwords):
    time.sleep(2)
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
    
    for r in soup.find_all("div", class_="post-widget-thumbnail"): r.decompose()
    for j in soup.find_all(["span", "div", "script", "style", "iframe"]):
        if not hasattr(j, 'attrs') or j.attrs is None: continue 
        c = str(j.get("class", ""))
        if j.get("data-mce-type") or "mce_SELRES" in c or "widget" in c: j.decompose()

    paras = []
    if c_div := soup.find("div", class_="entry-content"):
        for r in c_div.find_all(["ul", "ol", "div"], class_=re.compile(r"rp4wp|related|ad-")): r.decompose()
        paras = [sanitize_text(p.get_text(strip=True)) for p in c_div.find_all("p")]
    
    # Собираем чистый текст для сохранения и перевода
    raw_txt_clean = BAD_RE.sub("", "\n\n".join(paras))
    
    # СБОР КАРТИНОК
    srcs = set()
    # 1. Lightbox
    for link_tag in soup.find_all("a", class_="ci-lightbox", limit=10):
        if h := link_tag.get("href"): srcs.add(h)
    # 2. Content img
    if c_div:
        for img in c_div.find_all("img"):
            if u := extract_img_url(img): srcs.add(u)
    
    images = []
    if srcs:
        with ThreadPoolExecutor(5) as ex:
            futs = {ex.submit(save_image, u, OUTPUT_DIR / f"{aid}_{slug}" / "images"): u for u in list(srcs)[:10]}
            for f in as_completed(futs):
                if p:=f.result(): images.append(p)
    
    # 3. Featured fallback
    if not images and "_embedded" in post and (m:=post["_embedded"].get("wp:featuredmedia")):
        if isinstance(m, list) and (u:=m[0].get("source_url")):
             if "300x200" not in u and "150x150" not in u:
                if p:=save_image(u, OUTPUT_DIR / f"{aid}_{slug}" / "images"): images.append(p)

    if not images:
        logging.warning(f"⚠️ ID={aid}: Нет норм картинок (все отсеяны). Skip.")
        return None

    # --- ПЕРЕВОД: СКЛЕИВАЕМ ЗАГОЛОВОК И ТЕКСТ ---
    final_title = title
    final_text = raw_txt_clean
    translated_lang = ""

    if lang:
        # Используем разделитель, чтобы отправить контекстный запрос в Google
        DELIMITER = " ||| " 
        combined_text = f"{title}{DELIMITER}{raw_txt_clean}"
        
        # Переводим всё вместе
        translated_combined = translate_text(combined_text, lang)
        
        if translated_combined:
            # Разрезаем обратно
            if DELIMITER in translated_combined:
                parts = translated_combined.split(DELIMITER, 1)
                final_title = parts[0].strip()
                final_text = parts[1].strip()
            elif "|||" in translated_combined: # Если Google съел пробелы
                parts = translated_combined.split("|||", 1)
                final_title = parts[0].strip()
                final_text = parts[1].strip()
            else:
                # Если разделитель пропал, пробуем разбить по переносу строки
                parts = translated_combined.split('\n', 1)
                final_title = parts[0].strip()
                final_text = parts[1].strip() if len(parts) > 1 else ""
            
            translated_lang = lang

    final_title = sanitize_text(final_title)
    
    art_dir = OUTPUT_DIR / f"{aid}_{slug}"
    art_dir.mkdir(parents=True, exist_ok=True)
    
    # Сохраняем оригинал
    (art_dir / "content.txt").write_text(raw_txt_clean, encoding="utf-8")
    
    meta = {
        "id": aid, "slug": slug, "date": post.get("date"), "link": link,
        "title": final_title, "text_file": "content.txt",
        "images": sorted([Path(p).name for p in images]), "posted": False,
        "hash": curr_hash, "translated_to": ""
    }

    # Сохраняем перевод
    if translated_lang:
        # Структура: Заголовок + 2 энтера + Текст
        (art_dir / f"content.{lang}.txt").write_text(f"{final_title}\n\n{final_text}", encoding="utf-8")
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
