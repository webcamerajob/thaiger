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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

OUTPUT_DIR = Path("articles")
CATALOG_PATH = OUTPUT_DIR / "catalog.json"
MAX_RETRIES = 3
BASE_DELAY = 1.0
MAX_POSTED_RECORDS = 100 

# --- НАСТРОЙКА (HTTP/2 + Safari) ---
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

# --- ОЧИСТКА СТАРЫХ ПАПОК ---
def cleanup_old_articles(posted_ids_path: Path, articles_dir: Path):
    if not posted_ids_path.is_file() or not articles_dir.is_dir(): return
    logging.info("🧹 Проверка на очистку старых папок...")
    try:
        with open(posted_ids_path, 'r', encoding='utf-8') as f:
            all_posted = json.load(f)
            ids_to_keep = set(str(x) for x in all_posted[-MAX_POSTED_RECORDS:])
        
        cleaned_count = 0
        for article_folder in articles_dir.iterdir():
            if article_folder.is_dir():
                parts = article_folder.name.split('_', 1)
                if parts and parts[0].isdigit():
                    dir_id = parts[0]
                    if dir_id not in ids_to_keep:
                        shutil.rmtree(article_folder)
                        cleaned_count += 1
        if cleaned_count > 0:
            logging.info(f"🧹 Удалено {cleaned_count} старых папок.")
    except Exception as e:
        logging.error(f"Ошибка очистки: {e}")

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

def normalize_text(text: str) -> str:
    replacements = {'–': '-', '—': '-', '“': '"', '”': '"', '‘': "'", '’': "'"}
    for s, v in replacements.items(): text = text.replace(s, v)
    return text

def sanitize_text(text: str) -> str:
    if not text: return ""
    text = html.unescape(text)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'mce_SELRES_[^ ]+', '', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()

def load_posted_ids(state_file_path: Path) -> Set[str]:
    try:
        if state_file_path.exists():
            with open(state_file_path, 'r', encoding='utf-8') as f:
                fcntl.flock(f, fcntl.LOCK_SH)
                data = json.load(f)
                return {str(item) for item in data}
        return set()
    except Exception: return set()

def load_stopwords(file_path: Optional[Path]) -> List[str]:
    if not file_path or not file_path.exists(): 
        logging.info("Файл стоп-слов не найден.")
        return []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            words = [line.strip().lower() for line in f if line.strip()]
            logging.info(f"Загружено {len(words)} стоп-слов.")
            return words
    except Exception: return []

# --- УМНАЯ ФИЛЬТРАЦИЯ КАРТИНОК ---
def extract_img_url(img_tag: Any) -> Optional[str]:
    # 1. Srcset (качество)
    srcset = img_tag.get("srcset") or img_tag.get("data-srcset")
    if srcset:
        try:
            parts = srcset.split(',')
            links = []
            for p in parts:
                match = re.search(r'(\S+)\s+(\d+)w', p.strip())
                if match: links.append((int(match.group(2)), match.group(1)))
            if links: 
                return sorted(links, key=lambda x: x[0], reverse=True)[0][1]
        except Exception: pass
    
    # 2. Обычные атрибуты
    attrs = ["data-orig-file", "data-large-file", "data-src", "data-lazy-src", "src"]
    for attr in attrs:
        if val := img_tag.get(attr):
            clean_val = val.split()[0].split(',')[0].split('?')[0]
            # Фильтр по имени файла
            if any(x in clean_val.lower() for x in ["gif", "logo", "banner", "mastercard", "aba-", "payway", "icon", "button", "author"]): 
                continue
            return clean_val
    return None

# --- ЛОГИКА ПЕРЕВОДА ---

PROVIDER_LIMITS = {"google": 4800, "bing": 4500, "yandex": 4000}

def chunk_text_by_limit(text: str, limit: int) -> List[str]:
    chunks = []
    while text:
        if len(text) <= limit:
            chunks.append(text); break
        split_pos = text.rfind('\n\n', 0, limit)
        if split_pos == -1: split_pos = text.rfind('. ', 0, limit)
        if split_pos == -1: split_pos = limit
        chunk_end = max(1, split_pos + (2 if text[split_pos:split_pos+2] == '\n\n' else 1))
        chunks.append(text[:chunk_end])
        text = text[chunk_end:].lstrip()
    return chunks

def translate_text(text: str, to_lang: str = "ru") -> Optional[str]:
    if not text: return ""
    providers = ["yandex", "google", "bing"]
    normalized_text = normalize_text(text)
    
    for provider in providers:
        limit = PROVIDER_LIMITS.get(provider, 3000)
        try:
            chunks = chunk_text_by_limit(normalized_text, limit)
            translated_chunks = []
            for i, chunk in enumerate(chunks):
                if i > 0: time.sleep(0.5)
                res = ts.translate_text(
                    chunk, translator=provider, from_language="en", to_language=to_lang, timeout=45
                )
                if res: translated_chunks.append(res)
                else: raise ValueError("Empty chunk")
            return "".join(translated_chunks)
        except Exception as e:
            if "resolve" in str(e).lower() or "name" in str(e).lower(): break
            continue
    return None

# --- РАБОТА С САЙТОМ ---

def fetch_category_id(base_url: str, slug: str) -> int:
    logging.info(f"Fetching category ID for {slug}...")
    r = SCRAPER.get(f"{base_url}/wp-json/wp/v2/categories?slug={slug}", timeout=SCRAPER_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    if not data: raise RuntimeError(f"Category '{slug}' not found")
    return data[0]["id"]

def fetch_posts(base_url: str, cat_id: int, per_page: int = 10) -> List[Dict[str, Any]]:
    time.sleep(2)
    endpoint = f"{base_url}/wp-json/wp/v2/posts?categories={cat_id}&per_page={per_page}&_embed"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SCRAPER.get(endpoint, timeout=SCRAPER_TIMEOUT)
            if r.status_code == 429: time.sleep(20); continue
            r.raise_for_status()
            return r.json()
        except Exception: time.sleep(BASE_DELAY * attempt)
    return []

def save_image(src_url: str, folder: Path) -> Optional[str]:
    folder.mkdir(parents=True, exist_ok=True)
    fn = src_url.rsplit('/', 1)[-1].split('?', 1)[0]
    dest = folder / fn
    try:
        r = SCRAPER.get(src_url, timeout=SCRAPER_TIMEOUT)
        r.raise_for_status()
        dest.write_bytes(r.content)
        return str(dest)
    except Exception: return None

# --- ОБРАБОТКА СТАТЬИ ---

def parse_and_save(post: Dict[str, Any], translate_to: str, stopwords: List[str]) -> Optional[Dict[str, Any]]:
    time.sleep(6) # Анти-бан

    aid = str(post["id"])
    slug = post["slug"]
    link = post.get("link")
    if not link: return None

    art_dir = OUTPUT_DIR / f"{aid}_{slug}"
    art_dir.mkdir(parents=True, exist_ok=True)
    meta_path = art_dir / "meta.json"

    raw_title = BeautifulSoup(post["title"]["rendered"], "html.parser").get_text(strip=True)
    orig_title = sanitize_text(raw_title)
    
    if stopwords:
        title_lower = orig_title.lower()
        for phrase in stopwords:
            if phrase in title_lower:
                logging.info(f"🚫 Stopword '{phrase}' found in ID={aid}. Skipping.")
                return None

    logging.info(f"Processing ID={aid}: {link}")
    try:
        r = SCRAPER.get(link, timeout=SCRAPER_TIMEOUT)
        r.raise_for_status()
        page_html = r.text
    except Exception as e:
        logging.error(f"Fetch error ID={aid}: {e}"); return None

    current_hash = hashlib.sha256(page_html.encode()).hexdigest()
    if meta_path.exists():
        try:
            existing_meta = json.loads(meta_path.read_text(encoding="utf-8"))
            if existing_meta.get("hash") == current_hash and existing_meta.get("translated_to", "") == translate_to:
                return existing_meta
        except Exception: pass

    title = translate_text(orig_title, translate_to) if translate_to else orig_title
    title = sanitize_text(title)

    soup = BeautifulSoup(page_html, "html.parser")

    # -----------------------------------------------------------
    # 🔥 ФИКС: УДАЛЯЕМ БЛОК "RELATED ARTICLES" (post-widget-thumbnail)
    # Это удаляет HTML-код с мусорными картинками ДО их поиска
    # -----------------------------------------------------------
    for related in soup.find_all("div", class_="post-widget-thumbnail"):
        related.decompose()

    # Чистка остального мусора
    for junk in soup.find_all(["span", "div", "script", "style", "iframe"]):
        # Если класс содержит widget, related или mce_SELRES - удаляем
        cls_str = str(junk.get("class", ""))
        if junk.get("data-mce-type") or "mce_SELRES" in cls_str or "widget" in cls_str:
            junk.decompose()
            
    content_div = soup.find("div", class_="entry-content")
    paras = []
    if content_div:
        for rel in content_div.find_all(["ul", "ol", "div"], class_=re.compile(r"rp4wp|related|ad-")): rel.decompose()
        for p in content_div.find_all("p"):
            p_text = sanitize_text(p.get_text(strip=True))
            if p_text: paras.append(p_text)
    
    raw_text = "\n\n".join(paras)
    raw_text = BAD_RE.sub("", raw_text)

    img_dir = art_dir / "images"
    srcs = set()
    
    # Сбор картинок (теперь без related)
    if content_div:
        for img in content_div.find_all("img"):
            if u := extract_img_url(img): srcs.add(u)

    images = []
    if srcs:
        with ThreadPoolExecutor(max_workers=5) as ex:
            futures = {ex.submit(save_image, url, img_dir): url for url in list(srcs)[:10]}
            for fut in as_completed(futures):
                if path := fut.result(): images.append(path)

    # Fallback
    if not images and "_embedded" in post and (media := post["_embedded"].get("wp:featuredmedia")):
        if isinstance(media, list) and (u := media[0].get("source_url")):
            if path := save_image(u, img_dir): images.append(path)

    if not images: return None
    
    (art_dir / "content.txt").write_text(raw_text, encoding="utf-8")
    meta = {
        "id": aid, "slug": slug, "date": post.get("date"), "link": link,
        "title": title, "text_file": "content.txt",
        "images": sorted([Path(p).name for p in images]), "posted": False,
        "hash": current_hash, "translated_to": ""
    }

    if translate_to:
        trans_text = translate_text(raw_text, to_lang=translate_to)
        if trans_text:
            trans_text = sanitize_text(trans_text)
            (art_dir / f"content.{translate_to}.txt").write_text(f"{title}\n\n{trans_text}", encoding="utf-8")
            meta.update({"translated_to": translate_to, "text_file": f"content.{translate_to}.txt"})

    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    return meta

# --- УПРАВЛЕНИЕ КАТАЛОГОМ ---

def load_catalog() -> List[Dict[str, Any]]:
    if not CATALOG_PATH.exists(): return []
    try:
        with open(CATALOG_PATH, "r", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            return [item for item in json.load(f) if isinstance(item, dict) and "id" in item]
    except Exception: return []

def save_catalog(catalog: List[Dict[str, Any]]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    minimal = [{"id": item["id"], "hash": item.get("hash", ""), "translated_to": item.get("translated_to", "")}
               for item in catalog if isinstance(item, dict) and "id" in item]
    try:
        with open(CATALOG_PATH, "w", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            json.dump(minimal, f, ensure_ascii=False, indent=2)
    except IOError: pass

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
        # Очистка
        cleanup_old_articles(Path(args.posted_state_file), OUTPUT_DIR)

        cid = fetch_category_id(args.base_url, args.slug)
        fetch_limit = min(args.limit + 5, 20)
        posts = fetch_posts(args.base_url, cid, per_page=fetch_limit)
        
        catalog = load_catalog()
        posted_ids = load_posted_ids(Path(args.posted_state_file))
        sw_file = Path(args.stopwords_file)
        stopwords = load_stopwords(sw_file)
        
        processed = []
        count = 0
        for post in posts:
            if count >= args.limit: break
            if str(post["id"]) not in posted_ids:
                if meta := parse_and_save(post, args.lang, stopwords):
                    processed.append(meta)
                    count += 1
        
        if processed:
            for m in processed:
                catalog = [i for i in catalog if i.get("id") != m["id"]]
                catalog.append(m)
            save_catalog(catalog)
            print("NEW_ARTICLES_STATUS:true")
        else:
            print("NEW_ARTICLES_STATUS:false")

    except Exception:
        logging.exception("Fatal error:")
        exit(1)

if __name__ == "__main__":
    main()
