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
import subprocess  # Нужно для управления WARP
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
MAX_RETRIES = 5  # Количество попыток пробива
BASE_DELAY = 1.0
FETCH_DEPTH = 30
SCRAPER_TIMEOUT = 60
BAD_RE = re.compile(r"[\u200b-\u200f\uFEFF\u200E\u00A0]")

# Константа для порта WARP (Socks5 с удаленным DNS)
WARP_PROXY = "socks5h://127.0.0.1:40000"

IPHONE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Referer": "https://www.google.com/",
    "Upgrade-Insecure-Requests": "1"
}

# --- УПРАВЛЕНИЕ СЕТЬЮ И WARP ---

def rotate_warp():
    """Переподключает WARP для смены IP"""
    logging.info("♻️ WARP: Ротация IP...")
    try:
        # Разрываем соединение
        subprocess.run(["warp-cli", "disconnect"], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(2)
        # Подключаем снова
        subprocess.run(["warp-cli", "connect"], check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        
        # Ждем стабилизации (WARP иногда тупит пару секунд после коннекта)
        time.sleep(5)
        logging.info("✅ WARP: Переподключено.")
    except Exception as e:
        logging.error(f"❌ Ошибка ротации WARP: {e}")

def init_scraper():
    """Создает новую сессию SCRAPER с чистым TLS-отпечатком"""
    s = cffi_requests.Session(
        impersonate="chrome120", # Версия браузера для маскировки
        proxies={"http": WARP_PROXY, "https": WARP_PROXY},
        http_version=CurlHttpVersion.V1_1
    )
    s.headers = IPHONE_HEADERS
    return s

# Инициализируем глобальную переменную сессии
SCRAPER = init_scraper()

def make_request(method, url, **kwargs):
    """
    Выполняет запрос с автоматической ротацией IP и сессии при блокировках Cloudflare.
    """
    global SCRAPER
    kwargs.setdefault("timeout", SCRAPER_TIMEOUT)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = SCRAPER.request(method, url, **kwargs)
            
            # --- ПРОВЕРКА НА БЛОКИРОВКУ ---
            is_blocked = False
            
            # 1. Проверка по статус кодам
            if resp.status_code in [403, 429, 503]:
                is_blocked = True
                logging.warning(f"⚠️ Block detected (Status {resp.status_code}) for {url}")
            
            # 2. Проверка контента (если мы ждем API, а пришел HTML с капчей)
            # Для WP API ответ должен быть JSON, если пришел HTML - это Cloudflare Challenge
            content_type = resp.headers.get("Content-Type", "")
            if "wp-json" in url and ("text/html" in content_type or "<!DOCTYPE html>" in resp.text[:100]):
                is_blocked = True
                logging.warning(f"⚠️ Cloudflare Challenge detected (HTML instead of JSON) for {url}")

            if is_blocked:
                # Если это последняя попытка - падаем или возвращаем что есть
                if attempt == MAX_RETRIES:
                    logging.error("💀 Retries exhausted.")
                    return resp
                
                # Иначе - РОТАЦИЯ
                logging.warning(f"🔄 Rotating WARP and Session (Attempt {attempt}/{MAX_RETRIES})...")
                rotate_warp()
                logging.info("🛠 Resetting Scraper Session...")
                SCRAPER = init_scraper()
                time.sleep(3)
                continue # Пробуем снова

            # Если все ок (200 OK или просто ошибка 404, которая не блокировка)
            return resp

        except Exception as e:
            logging.warning(f"⚠️ Request error ({e}). Rotating... (Attempt {attempt}/{MAX_RETRIES})")
            rotate_warp()
            SCRAPER = init_scraper()
            time.sleep(3)
    
    return None

# --- ПРЯМОЙ ПЕРЕВОДЧИК ---

def translate_text(text: str, to_lang: str = "ru", source_lang: str = "auto") -> str:
    if not text or len(text.strip()) < 2: 
        return text
        
    url = "https://translate.googleapis.com/translate_a/single"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    }
    params = {
        "client": "gtx", 
        "sl": source_lang, 
        "tl": to_lang, 
        "dt": "t", 
        "q": text.strip()
    }
    
    try:
        r = requests.get(url, params=params, headers=headers, timeout=10)
        if r.status_code == 200:
            data = r.json()
            return "".join([item[0] for item in data[0] if item and item[0]])
        else:
            print(f"Ошибка перевода: статус код {r.status_code}")
    except Exception as e:
        print(f"Исключение при запросе к переводчику: {e}")
        
    return text

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ---

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
        # Используем наш умный make_request, чтобы не падать на картинках
        r = make_request("GET", url, timeout=30)
        if r and r.status_code == 200:
            dest.write_bytes(r.content)
            return str(dest)
    except: return None
    return None

# --- ОСНОВНАЯ ЛОГИКА ---

def fetch_cat_id(url, slug):
    r = make_request("GET", f"{url}/wp-json/wp/v2/categories?slug={slug}")
    if r and r.status_code == 200:
        try:
            data = r.json()
            if data and isinstance(data, list):
                return data[0]["id"]
        except: pass
    
    logging.warning(f"Could not fetch category ID for '{slug}'. Using fallback if implemented.")
    # Если нужно падать - раскомментируй:
    # raise Exception(f"Failed to fetch category ID for {slug}")
    return 19 # Fallback (как в прошлых версиях)

def fetch_posts(url, cid, limit):
    all_posts, page = [], 1
    while len(all_posts) < limit:
        logging.info(f"📄 Fetching page {page}...")
        r = make_request("GET", f"{url}/wp-json/wp/v2/posts", 
                         params={"categories": cid, "per_page": 20, "page": page, "_embed": "true"})
        
        if not r or r.status_code != 200:
            logging.warning("⚠️ Failed to fetch posts page.")
            break
            
        try:
            data = r.json()
        except:
            logging.error("❌ Failed to parse JSON posts.")
            break
            
        if not data or not isinstance(data, list): 
            break
            
        all_posts.extend(data)
        page += 1
        if page > 5: break # Ограничение безопасности
    return all_posts[:limit]

def parse_and_save(post: Dict[str, Any], translate_to: str, stopwords: List[str]) -> Optional[Dict[str, Any]]:
    aid, slug = str(post["id"]), post["slug"]
    art_dir = OUTPUT_DIR / f"{aid}_{slug}"
    art_dir.mkdir(parents=True, exist_ok=True)
    meta_path = art_dir / "meta.json"

    # 1. Проверка хэша
    content_html = post["content"]["rendered"]
    current_hash = hashlib.sha256(content_html.encode()).hexdigest()
    
    if meta_path.exists():
        try:
            existing_meta = json.loads(meta_path.read_text(encoding="utf-8"))
            if existing_meta.get("hash") == current_hash and existing_meta.get("translated_to") == translate_to:
                logging.info(f"Skipping article ID={aid} (cache hit).")
                return existing_meta
        except: pass

    # 2. Подготовка оригинального текста
    orig_title = BeautifulSoup(post["title"]["rendered"], "html.parser").get_text(strip=True)
    
    if any(s in orig_title.lower() for s in stopwords):
        logging.info(f"🚫 Stopword found in ID={aid}. Skipping.")
        return None

    soup = BeautifulSoup(content_html, "html.parser")
    for junk in soup.select(".related-posts, .ad-container, script, style, .jp-relatedposts"):
        junk.decompose()

    paras = [p.get_text(strip=True) for p in soup.find_all("p") if p.get_text(strip=True)]
    clean_paras = [p for p in paras if not any(x in p.lower() for x in ["read also", "also read", "related stories"])]
    raw_body = "\n\n".join(clean_paras)

    # 3. КАРТИНКИ (Улучшенный поиск Featured Image)
    srcs = []
    link = post.get("link")
    full_soup = None
    try:
        r = make_request("GET", link)
        if r and r.status_code == 200:
            full_soup = BeautifulSoup(r.text, "html.parser")
    except: pass

    if full_soup:
        feat = full_soup.find("div", class_="featured-area") or full_soup.find("figure", class_="single-featured-image")
        if feat and (m_img := feat.find("img")):
            if u := extract_img_url(m_img): srcs.append(u)

    for img in soup.find_all("img")[:10]:
        u = extract_img_url(img)
        if u and u not in srcs: srcs.append(u)

    images = []
    if srcs:
        with ThreadPoolExecutor(max_workers=5) as ex:
            futures = {ex.submit(save_image, url, art_dir / "images"): url for url in srcs}
            for fut in as_completed(futures):
                if path := fut.result(): images.append(Path(path).name)

    if not images:
        logging.warning(f"No images for ID={aid}. Skipping.")
        return None

    # 4. ПЕРЕВОД ЧАНКАМИ С СОХРАНЕНИЕМ КОНТЕКСТА ЗАГОЛОВКА
    final_title = orig_title
    final_body = raw_text = raw_body
    final_text_file = "content.txt"
    (art_dir / "content.txt").write_text(raw_body, encoding="utf-8")

    if translate_to:
        logging.info(f"🌐 Translating ID={aid} (Title + Body context)...")
        # Соединяем заголовок и тело через разделитель
        full_text_to_translate = f"{orig_title}\n|||\n{raw_body}"
        
        # Разбиваем на чанки по 4500 символов (лимит Google ~5000)
        chunks = []
        while full_text_to_translate:
            if len(full_text_to_translate) <= 4500:
                chunks.append(full_text_to_translate)
                break
            # Ищем ближайший перенос строки, чтобы не резать по живому
            split_idx = full_text_to_translate.rfind('\n', 0, 4500)
            if split_idx == -1: split_idx = 4500
            chunks.append(full_text_to_translate[:split_idx])
            full_text_to_translate = full_text_to_translate[split_idx:].lstrip()

        translated_chunks = []
        for chunk in chunks:
            translated_chunks.append(translate_text(chunk, translate_to))
        
        translated_full = "\n\n".join(translated_chunks)

        # Отделяем заголовок от тела
        if "|||" in translated_full:
            parts = translated_full.split("|||", 1)
            final_title = parts[0].strip()
            final_body = parts[1].strip()
        else:
            # Fallback если разделитель съеден
            parts = translated_full.split('\n', 1)
            final_title = parts[0].strip()
            final_body = parts[1].strip() if len(parts) > 1 else ""

        trans_file = f"content.{translate_to}.txt"
        (art_dir / trans_file).write_text(f"{final_title}\n\n{final_body}", encoding="utf-8")
        final_text_file = trans_file

    # 5. Метаданные
    meta = {
        "id": aid, "slug": slug, "date": post.get("date"), "link": link,
        "title": final_title, "text_file": final_text_file,
        "images": sorted(list(set(images))), "posted": False,
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
