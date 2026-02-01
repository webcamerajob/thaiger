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
MAX_POSTED_RECORDS = 500 # Увеличили память
FETCH_DEPTH = 500

# Константа для порта WARP (Socks5 с удаленным DNS)
WARP_PROXY = "socks5h://127.0.0.1:40000"

# --- НАСТРОЙКИ СЕТИ ---
SCRAPER = cffi_requests.Session(
    impersonate="chrome110",
    proxies={
        "http": WARP_PROXY,
        "https": WARP_PROXY
    },
    http_version=CurlHttpVersion.V1_1
)

IPHONE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com/",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "cross-site",
    "Upgrade-Insecure-Requests": "1"
}

SCRAPER.headers = IPHONE_HEADERS
SCRAPER_TIMEOUT = 60 
BAD_RE = re.compile(r"[\u200b-\u200f\uFEFF\u200E\u00A0]")

# --- ПРЯМОЙ ПЕРЕВОДЧИК ---
def translate_text(text: str, to_lang: str = "ru") -> str:
    if not text: return ""

    chunks = []
    current_chunk = ""
    for paragraph in text.split('\n'):
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
    # Для гугла используем обычный requests без прокси, так надежнее
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36"}

    for chunk in chunks:
        if not chunk.strip():
            translated_parts.append("")
            continue
        try:
            params = {
                "client": "gtx", "sl": "en", "tl": to_lang, "dt": "t", "q": chunk.strip()
            }
            r = requests.get(url, params=params, headers=headers, timeout=10)
            if r.status_code == 200:
                data = r.json()
                text_part = "".join([item[0] for item in data[0] if item and item[0]])
                translated_parts.append(text_part)
            else:
                translated_parts.append(chunk)
            time.sleep(0.3)
        except Exception:
            translated_parts.append(chunk)

    return "\n".join(translated_parts)

# --- ВСПОМОГАТЕЛЬНЫЕ ---
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

# --- КАРТИНКИ ---
def extract_img_url(img_tag: Any) -> Optional[str]:
    width_attr = img_tag.get("width")
    if width_attr and width_attr.isdigit():
        if int(width_attr) < 400: return None

    def is_junk(url_str: str) -> bool:
        u = url_str.lower()
        bad = ["gif", "logo", "banner", "icon", "avatar", "button", "share", "pixel", "tracker"]
        if any(b in u for b in bad): return True
        if re.search(r'-\d{2,3}x\d{2,3}\.', u): return True
        return False

    srcset = img_tag.get("srcset") or img_tag.get("data-srcset")
    if srcset:
        try:
            parts = srcset.split(',')
            links = []
            for p in parts:
                match = re.search(r'(\S+)\s+(\d+)w', p.strip())
                if match: 
                    w_val = int(match.group(2))
                    u_val = match.group(1)
                    if w_val >= 400: links.append((w_val, u_val))
            if links:
                best_link = sorted(links, key=lambda x: x[0], reverse=True)[0][1]
                if not is_junk(best_link): 
                    return best_link.split('?')[0]
        except Exception: pass

    attrs = ["data-orig-file", "data-large-file", "data-src", "data-lazy-src", "src"]
    for attr in attrs:
        if val := img_tag.get(attr):
            clean_val = val.split()[0].split(',')[0].split('?')[0]
            if not is_junk(clean_val): return clean_val

    return None

def save_image(url, folder):
    folder.mkdir(parents=True, exist_ok=True)
    fn = url.rsplit('/',1)[-1].split('?',1)[0]
    if len(fn) > 50: fn = hashlib.md5(fn.encode()).hexdigest() + ".jpg"
    dest = folder / fn
    try:
        resp = make_request("GET", url) 
        dest.write_bytes(resp.content)
        return str(dest)
    except Exception: return None

# --- ЗАПРОСЫ (С ПОВТОРАМИ И WARP) ---
def make_request(method: str, url: str, **kwargs):
    retries = 3
    for i in range(retries):
        try:
            kwargs.setdefault("timeout", SCRAPER_TIMEOUT)
            
            if method.upper() == "GET":
                response = SCRAPER.get(url, **kwargs)
            else:
                response = SCRAPER.request(method, url, **kwargs)

            if response.status_code in [403, 429]:
                logging.warning(f"⚠️ Блок или лимит ({response.status_code}). Ждем 20с... (WARP Proxy)")
                time.sleep(20)
                continue
            
            response.raise_for_status()
            return response

        except Exception as e:
            logging.warning(f"⚠️ Ошибка сети (WARP): {e}. Попытка {i+1}/{retries}")
            time.sleep(10 * (i + 1))
    
    raise Exception(f"❌ Не удалось получить данные с {url} через прокси {WARP_PROXY}")

def fetch_cat_id(url, slug):
    logging.info(f"Получение ID категории '{slug}'...")
    r = make_request("GET", f"{url}/wp-json/wp/v2/categories?slug={slug}")
    data = r.json()
    if not data: raise RuntimeError("Cat not found")
    return data[0]["id"]

def fetch_posts(url, cid, limit):
    logging.info(f"Запрос {limit} постов (постранично) через WARP...") 
    all_posts = []
    page = 1
    max_per_page = 100 # WordPress не дает больше 100 за раз

    while len(all_posts) < limit:
        try:
            # Вычисляем сколько осталось скачать
            remaining = limit - len(all_posts)
            current_batch = min(remaining, max_per_page)

            logging.info(f"   📥 Скачиваем страницу {page} ({current_batch} шт.)...")
            
            r = make_request("GET", f"{url}/wp-json/wp/v2/posts", 
                             params={
                                 "categories": cid, 
                                 "per_page": current_batch, 
                                 "page": page, 
                                 "_embed": "true"
                             })
            
            data = r.json()
            if not data:
                logging.info("   Посты закончились раньше лимита.")
                break
            
            all_posts.extend(data)
            page += 1
            time.sleep(1) # Пауза между страницами, чтобы не грузить сервер

        except Exception as e:
            logging.error(f"Ошибка получения страницы {page}: {e}")
            break
            
    return all_posts

# --- ПАРСИНГ (ЧИСТАЯ ВЕРСИЯ) ---
def parse_and_save(post, lang, stopwords):
    time.sleep(2)
    aid, slug, link = str(post["id"]), post["slug"], post.get("link")

    # 1. Заголовок
    try:
        raw_title = BeautifulSoup(post["title"]["rendered"], "html.parser").get_text(strip=True)
    except:
        raw_title = "No Title"
    title = sanitize_text(raw_title)

    if stopwords:
        for ph in stopwords:
            if ph in title.lower():
                logging.info(f"🚫 ID={aid}: Стоп-слово '{ph}'")
                return None

    # 2. Скачивание
    try:
        html_txt = make_request("GET", link).text
    except Exception: return None

    # 3. Хэш (защита от повторов)
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

    # 4. ПОДГОТОВКА HTML (Удаляем всё лишнее ДО извлечения текста)
    
    # Удаляем технические теги
    for tag in soup.find_all(["script", "style", "iframe", "noscript", "form", "button", "input", "meta", "link", "svg"]):
        tag.decompose()

    # Ищем основной контент
    c_div = soup.find("div", class_="entry-content")
    if not c_div:
        # Fallback для некоторых тем WP
        c_div = soup.find("div", class_="td-post-content")

    if c_div:
        # СПИСОК МУСОРА: классы, которые точно не нужны
        junk_classes = [
            "post-widget-thumbnail", "related-posts", "ad-container", "share-buttons", 
            "meta-info", "jp-relatedposts", "mc_embed_signup", "widget_text", 
            "sharedaddy", "td-a-rec", "td-g-rec", "addthis_tool", "rp4wp-related-posts",
            "zeen-10-related-posts", "yarpp-related"
        ]
        
        # Удаляем по классам
        for tag in c_div.find_all(class_=True):
            classes = tag.get("class", [])
            if any(j in c for c in classes for j in junk_classes):
                tag.decompose()

        # Удаляем блоки "Читайте также" по тексту (Thaiger часто вставляет их просто текстом)
        for tag in c_div.find_all(["p", "h3", "h4", "div", "span"]):
            text = tag.get_text(strip=True).lower()
            if text.startswith("also read:") or text.startswith("read also:") or text.startswith("related:") or text == "advertisement":
                tag.decompose()
            # Удаляем абзацы, где одна ссылка (обычно это перелинковка)
            if tag.name == 'p' and tag.find('a') and len(tag.get_text(strip=True)) < 100:
                # Если длина текста ссылки почти равна длине всего текста -> это просто ссылка
                a_tag = tag.find('a')
                if len(a_tag.get_text(strip=True)) >= len(text) - 5:
                    tag.decompose()

    # 5. СБОР ТЕКСТА (Только полезные теги)
    blocks = []
    if c_div:
        # Проходим только по значимым тегам верхнего уровня
        for tag in c_div.find_all(["p", "h2", "h3", "ul", "ol", "blockquote"], recursive=False):
            
            # Обработка заголовков
            if tag.name in ["h2", "h3"]:
                t = tag.get_text(strip=True)
                if t and len(t) > 3: # Игнорируем совсем короткие заголовки-мусор
                    blocks.append(f"\n<b>{t}</b>") # Жирным в Telegram
            
            # Обработка списков
            elif tag.name in ["ul", "ol"]:
                lis = [f"• {li.get_text(strip=True)}" for li in tag.find_all("li") if li.get_text(strip=True)]
                if lis:
                    blocks.append("\n".join(lis))
            
            # Обработка цитат
            elif tag.name == "blockquote":
                t = tag.get_text(separator=" ", strip=True)
                if t: blocks.append(f"<i>{t}</i>")

            # Обработка параграфов
            elif tag.name == "p":
                # Убираем ссылки "Photo: ..." в конце статей
                t = tag.get_text(separator=" ", strip=True)
                if t.lower().startswith("photo:") or t.lower().startswith("source:"):
                    continue
                if t: blocks.append(t)

    # Собираем всё вместе через двойной перенос строки
    raw_txt_clean = "\n\n".join(blocks)
    raw_txt_clean = BAD_RE.sub("", raw_txt_clean)

    # 6. КАРТИНКИ
    srcs = set()
    # Lightbox (обычно лучшие фото)
    for link_tag in soup.find_all("a", class_="ci-lightbox", limit=10):
        if h := link_tag.get("href"): 
            if "gif" not in h.lower(): srcs.add(h)

    # Если контент найден, ищем картинки внутри
    if c_div:
        for img in c_div.find_all("img"):
            if u := extract_img_url(img): srcs.add(u)

    images = []
    if srcs:
        with ThreadPoolExecutor(5) as ex:
            futs = {ex.submit(save_image, u, OUTPUT_DIR / f"{aid}_{slug}" / "images"): u for u in list(srcs)[:10]}
            for f in as_completed(futs):
                if p:=f.result(): images.append(p)

    # Fallback на Featured Image
    if not images and "_embedded" in post and (m:=post["_embedded"].get("wp:featuredmedia")):
        if isinstance(m, list) and (u:=m[0].get("source_url")):
             if "300x200" not in u and "150x150" not in u and "logo" not in u.lower():
                if p:=save_image(u, OUTPUT_DIR / f"{aid}_{slug}" / "images"): images.append(p)

    # Если совсем нет картинок и текста мало — скорее всего мусор, пропускаем
    if not images and len(raw_txt_clean) < 100:
        logging.warning(f"⚠️ ID={aid}: Пустой контент/нет картинок. Skip.")
        return None

    # 7. ПЕРЕВОД
    final_title = title
    final_text = raw_txt_clean
    translated_lang = ""

    if lang and raw_txt_clean:
        DELIMITER = "\n|||\n" # Надежный разделитель
        text_to_translate = raw_txt_clean[:4500] 
        combined_text = f"{title}{DELIMITER}{text_to_translate}"
        
        translated_combined = translate_text(combined_text, lang)

        if translated_combined:
            if "|||" in translated_combined:
                parts = translated_combined.split("|||", 1)
                final_title = parts[0].strip()
                final_text = parts[1].strip()
            else:
                # Эвристика: первая строка - заголовок, остальное текст
                parts = translated_combined.split('\n', 1)
                final_title = parts[0].strip()
                final_text = parts[1].strip() if len(parts) > 1 else ""
            translated_lang = lang
            
            if len(raw_txt_clean) > 4500:
                final_text += "\n\n..."

    final_title = sanitize_text(final_title)

    # 8. СОХРАНЕНИЕ
    art_dir = OUTPUT_DIR / f"{aid}_{slug}"
    art_dir.mkdir(parents=True, exist_ok=True)

    (art_dir / "content.txt").write_text(raw_txt_clean, encoding="utf-8")

    meta = {
        "id": aid, "slug": slug, "date": post.get("date"), "link": link,
        "title": final_title, "text_file": "content.txt",
        "images": sorted([Path(p).name for p in images]), "posted": False,
        "hash": curr_hash, "translated_to": ""
    }

    if translated_lang:
        (art_dir / f"content.{lang}.txt").write_text(f"{final_title}\n\n{final_text}", encoding="utf-8")
        meta.update({"translated_to": lang, "text_file": f"content.{lang}.txt"})

    with open(meta_path, "w", encoding="utf-8") as f: json.dump(meta, f, ensure_ascii=False, indent=2)
    return meta

# --- MAIN ---
def main():
    # Вот здесь определяются аргументы для main.py (--base-url и т.д.)
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
        # Очистку старых папок можно закомментировать, если мешает, но она полезна
        # cleanup_old_articles(Path(args.posted_state_file), OUTPUT_DIR)

        cid = fetch_cat_id(args.base_url, args.slug)
        posts = fetch_posts(args.base_url, cid, FETCH_DEPTH)

        posted = load_posted_ids(Path(args.posted_state_file))
        stop = load_stopwords(Path(args.stopwords_file))
        catalog = []
        if CATALOG_PATH.exists():
            with open(CATALOG_PATH, 'r') as f: catalog=json.load(f)

        new_posts = [p for p in posts if str(p["id"]) not in posted]
        logging.info(f"Всего постов: {len(posts)}, новых: {len(new_posts)}")

        if not new_posts:
            print("NEW_ARTICLES_STATUS:false")
            return

######        new_posts.reverse()
        posts_to_process = new_posts[:args.limit]
        processed = []
        count = 0

        logging.info(f"Будем обрабатывать {len(posts_to_process)} постов...")

        for post in posts_to_process:
            if count >= args.limit: break
            if meta := parse_and_save(post, args.lang, stop):
                processed.append(meta)
                count += 1

        if processed:
            for m in processed:
                catalog = [i for i in catalog if i.get("id") != m["id"]]
                catalog.append(m)
            try:
                catalog.sort(key=lambda x: int(x.get("id", 0)))
            except: pass
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
