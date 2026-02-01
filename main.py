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

import requests
from bs4 import BeautifulSoup
from curl_cffi import requests as cffi_requests, CurlHttpVersion

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# --- КОНФИГУРАЦИЯ ---
OUTPUT_DIR = Path("articles")
CATALOG_PATH = OUTPUT_DIR / "catalog.json"
MAX_RETRIES = 3
BASE_DELAY = 1.0
FETCH_DEPTH = 30

# Константа для порта WARP (Socks5 с удаленным DNS)
WARP_PROXY = "socks5h://127.0.0.1:40000"

# --- НАСТРОЙКИ СЕТИ ---
SCRAPER = cffi_requests.Session(
    impersonate="chrome110",
    proxies={"http": WARP_PROXY, "https": WARP_PROXY},
    http_version=CurlHttpVersion.V1_1
)

IPHONE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Referer": "https://www.google.com/",
    "Upgrade-Insecure-Requests": "1"
}

SCRAPER.headers = IPHONE_HEADERS
SCRAPER_TIMEOUT = 60 
BAD_RE = re.compile(r"[\u200b-\u200f\uFEFF\u200E\u00A0]")

# --- ПРЯМОЙ ПЕРЕВОДЧИК ---
def translate_text(text: str, to_lang: str = "ru") -> str:
    if not text or len(text.strip()) < 2: return text
    url = "https://translate.googleapis.com/translate_a/single"
    headers = {"User-Agent": "Mozilla/5.0"}
    try:
        params = {"client": "gtx", "sl": "en", "tl": to_lang, "dt": "t", "q": text.strip()}
        r = requests.get(url, params=params, headers=headers, timeout=10)
        if r.status_code == 200:
            data = r.json()
            return "".join([item[0] for item in data[0] if item and item[0]])
    except Exception:
        pass
    return text

# --- ВСПОМОГАТЕЛЬНЫЕ ---
def sanitize_text(text: str) -> str:
    if not text: return ""
    text = html.unescape(text)
    text = re.sub(r'<[^>]+>', '', text)
    return re.sub(r'\n{3,}', '\n\n', text).strip()

def load_posted_ids(state_file_path: Path) -> Set[str]:
    try:
        if state_file_path.exists():
            with open(state_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return {str(item) for item in data}
        return set()
    except Exception: return set()

def load_stopwords(file_path: Optional[Path]) -> List[str]:
    if not file_path or not file_path.exists(): return []
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return [line.strip().lower() for line in f if line.strip()]
    except Exception: return []

def extract_img_url(img_tag: Any) -> Optional[str]:
    srcset = img_tag.get("srcset") or img_tag.get("data-srcset")
    if srcset:
        try:
            parts = srcset.split(',')
            links = []
            for p in parts:
                match = re.search(r'(\S+)\s+(\d+)w', p.strip())
                if match: links.append((int(match.group(2)), match.group(1)))
            if links: return sorted(links, key=lambda x: x[0], reverse=True)[0][1].split('?')[0]
        except: pass
    for attr in ["data-orig-file", "data-large-file", "src"]:
        if val := img_tag.get(attr): return val.split()[0].split('?')[0]
    return None

def save_image(url, folder):
    folder.mkdir(parents=True, exist_ok=True)
    fn = hashlib.md5(url.encode()).hexdigest() + ".jpg"
    dest = folder / fn
    try:
        r = SCRAPER.get(url, timeout=30)
        r.raise_for_status()
        dest.write_bytes(r.content)
        return str(dest)
    except: return None

def make_request(method, url, **kwargs):
    kwargs.setdefault("timeout", SCRAPER_TIMEOUT)
    return SCRAPER.request(method, url, **kwargs)

def fetch_cat_id(url, slug):
    r = make_request("GET", f"{url}/wp-json/wp/v2/categories?slug={slug}")
    return r.json()[0]["id"]

def fetch_posts(url, cid, limit):
    all_posts, page = [], 1
    while len(all_posts) < limit:
        r = make_request("GET", f"{url}/wp-json/wp/v2/posts", 
                         params={"categories": cid, "per_page": 100, "page": page, "_embed": "true"})
        data = r.json()
        if not data: break
        all_posts.extend(data)
        page += 1
        if page > 5: break # Ограничение безопасности
    return all_posts[:limit]

# --- ПАРСИНГ (ИНТЕГРИРОВАННАЯ ВЕРСИЯ) ---
def parse_and_save(post: Dict[str, Any], translate_to: str, stopwords: List[str]) -> Optional[Dict[str, Any]]:
    aid, slug = str(post["id"]), post["slug"]
    art_dir = OUTPUT_DIR / f"{aid}_{slug}"
    art_dir.mkdir(parents=True, exist_ok=True)
    meta_path = art_dir / "meta.json"

    # 1. Проверка хэша контента (чтобы не переводить заново)
    content_html = post["content"]["rendered"]
    current_hash = hashlib.sha256(content_html.encode()).hexdigest()
    
    if meta_path.exists():
        try:
            existing_meta = json.loads(meta_path.read_text(encoding="utf-8"))
            if existing_meta.get("hash") == current_hash and existing_meta.get("translated_to") == translate_to:
                logging.info(f"Skipping article ID={aid} (cache hit).")
                return existing_meta
        except: pass

    # 2. Обработка заголовка
    orig_title = BeautifulSoup(post["title"]["rendered"], "html.parser").get_text(strip=True)
    title = orig_title
    if any(s in title.lower() for s in stopwords):
        logging.info(f"🚫 Stopword found in ID={aid}. Skipping.")
        return None

    if translate_to:
        title = translate_text(orig_title, translate_to)

    # 3. Извлечение и чистка текста
    soup = BeautifulSoup(content_html, "html.parser")
    # Удаляем Recent/Related блоки
    for junk in soup.select(".related-posts, .ad-container, script, style"):
        junk.decompose()

    paras = [p.get_text(strip=True) for p in soup.find_all("p") if p.get_text(strip=True)]
    raw_text = "\n\n".join(paras)
    raw_text = BAD_RE.sub("", raw_text)
    raw_text = re.sub(r"[ \t]+", " ", raw_text)
    raw_text = re.sub(r"\n{3,}", "\n\n", raw_text)

    # 4. Загрузка изображений
    img_dir = art_dir / "images"
    srcs = [extract_img_url(img) for img in soup.find_all("img")[:10]]
    srcs = [s for s in srcs if s]
    
    if not srcs and "_embedded" in post:
        media = post["_embedded"].get("wp:featuredmedia")
        if media and media[0].get("source_url"): 
            srcs.append(media[0]["source_url"])

    images = []
    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {ex.submit(save_image, url, img_dir): url for url in srcs}
        for fut in as_completed(futures):
            if path := fut.result():
                images.append(Path(path).name) # Сохраняем ТОЛЬКО имя файла

    if not images:
        logging.warning(f"No images for ID={aid}. Skipping.")
        return None

    # 5. Сохранение и Перевод
    final_text_file = "content.txt"
    (art_dir / "content.txt").write_text(raw_text, encoding="utf-8")

    if translate_to:
        logging.info(f"🌐 Translating content ID={aid}...")
        try:
            clean_paras = [BAD_RE.sub("", p) for p in paras]
            trans = [translate_text(p, translate_to) for p in clean_paras]
            
            trans_file = f"content.{translate_to}.txt"
            trans_content = f"{title}\n\n\n" + "\n\n".join(trans)
            (art_dir / trans_file).write_text(trans_content, encoding="utf-8")
            
            final_text_file = trans_file
        except Exception as e:
            logging.error(f"Translation error: {e}")

    meta = {
        "id": aid, "slug": slug, "date": post.get("date"), "link": post.get("link"),
        "title": title, "text_file": final_text_file,
        "images": sorted(images), "posted": False,
        "hash": current_hash, "translated_to": translate_to
    }

    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)
    return meta

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--slug", default="national")
    parser.add_argument("-n", "--limit", type=int, default=10)
    parser.add_argument("-l", "--lang", default="ru")
    parser.add_argument("--posted-state-file", default="articles/posted.json")
    parser.add_argument("--stopwords-file", default="stopwords.txt")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    try:
        cid = fetch_cat_id(args.base_url, args.slug)
        posts = fetch_posts(args.base_url, cid, FETCH_DEPTH)
        
        posted = load_posted_ids(Path(args.posted_state_file))
        stop = load_stopwords(Path(args.stopwords_file))
        
        new_posts = [p for p in posts if str(p["id"]) not in posted]
        logging.info(f"Total: {len(posts)}, New: {len(new_posts)}")

        if not new_posts:
            print("NEW_ARTICLES_STATUS:false")
            return

        processed_count = 0
        for post in new_posts[:args.limit]:
            if parse_and_save(post, args.lang, stop):
                processed_count += 1

        print(f"NEW_ARTICLES_STATUS:{'true' if processed_count > 0 else 'false'}")
    except Exception as e:
        logging.exception("Fatal error:")
        exit(1)

if __name__ == "__main__":
    main()
