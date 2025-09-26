#!/usr/bin/env python3
import argparse
import logging
import json
import hashlib
import time
import re
import os
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Set

# Убедитесь, что эти импорты у вас есть, если они используются
from bs4 import BeautifulSoup
import cloudscraper # Для fetch_category_id, fetch_posts, save_image
import translators as ts # Для translate_text
import fcntl # Для блокировки файлов в load_catalog и save_catalog

# Настройка переменной окружения (должна быть в начале, один раз)
os.environ["translators_default_region"] = "EN"

# Настройки логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

# Предполагаемые константы
OUTPUT_DIR = Path("articles")
CATALOG_PATH = OUTPUT_DIR / "catalog.json"
MAX_RETRIES = 3
BASE_DELAY = 1.0 # Базовая задержка для ретраев

# cloudscraper для обхода Cloudflare
SCRAPER = cloudscraper.create_scraper()
SCRAPER_TIMEOUT = (10.0, 60.0)      # (connect_timeout, read_timeout) в секундах

# Регулярные выражения для очистки текста
bad_re = re.compile(r"[\u200b-\u200f\uFEFF\u200E\u00A0]") # Пример

# --- Вспомогательные функции ---

def load_posted_ids(state_file_path: Path) -> Set[str]:
    """
    Загружает множество ID из файла состояния (например, posted.json).
    Используется блокировка файла для безопасного чтения.
    """
    try:
        if state_file_path.exists():
            with open(state_file_path, 'r', encoding='utf-8') as f:
                fcntl.flock(f, fcntl.LOCK_SH) # Блокировка для чтения
                return {str(item) for item in json.load(f)}
        return set()
    except (FileNotFoundError, json.JSONDecodeError, IOError) as e:
        logging.warning(f"Could not load posted IDs from {state_file_path}: {e}. Assuming empty set.")
        return set()

def extract_img_url(img_tag: Any) -> Optional[str]:
    """Извлекает URL изображения из тега <img>."""
    for attr in ("data-src", "data-lazy-src", "data-srcset", "srcset", "src"):
        val = img_tag.get(attr)
        if not val:
            continue
        parts = val.split()
        if parts:
            return parts[0]
    return None

def fetch_category_id(base_url: str, slug: str) -> int:
    """Получает ID категории по ее 'slug'."""
    logging.info(f"Fetching category ID for {slug} from {base_url}...")
    endpoint = f"{base_url}/wp-json/wp/v2/categories?slug={slug}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SCRAPER.get(endpoint, timeout=SCRAPER_TIMEOUT)
            r.raise_for_status()
            data = r.json()
            if not data:
                raise RuntimeError(f"Category '{slug}' not found on {base_url}")
            return data[0]["id"]
        except (ReqTimeout, RequestException) as e:
            delay = BASE_DELAY * 2 ** (attempt - 1)
            logging.warning(
                "Timeout fetching category (try %s/%s): %s; retry in %.1fs",
                attempt, MAX_RETRIES, e, delay
            )
            time.sleep(delay)
        except json.JSONDecodeError as e:
            logging.error("JSON decode error for categories: %s", e)
            break
    raise RuntimeError("Failed fetching category id")

def fetch_posts(base_url: str, cat_id: int, per_page: int = 10) -> List[Dict[str, Any]]:
    """Получает список постов из указанной категории."""
    logging.info(f"Fetching posts for category {cat_id} from {base_url}, per_page={per_page}...")
    endpoint = f"{base_url}/wp-json/wp/v2/posts?categories={cat_id}&per_page={per_page}&_embed"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SCRAPER.get(endpoint, timeout=SCRAPER_TIMEOUT)
            r.raise_for_status()
            return r.json()
        except (ReqTimeout, RequestException) as e:
            delay = BASE_DELAY * 2 ** (attempt - 1)
            logging.warning(
                "Timeout fetching posts (try %s/%s): %s; retry in %.1fs",
                attempt, MAX_RETRIES, e, delay
            )
            time.sleep(delay)
        except json.JSONDecodeError as e:
            logging.error("JSON decode error for posts: %s", e)
            break
    logging.error("Giving up fetching posts")
    return []

def save_image(src_url: str, folder: Path) -> Optional[str]:
    """Сохраняет изображение по URL в указанную папку."""
    logging.info(f"Saving image from {src_url} to {folder}...")
    folder.mkdir(parents=True, exist_ok=True)
    fn = src_url.rsplit('/', 1)[-1].split('?', 1)[0]
    dest = folder / fn
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SCRAPER.get(src_url, timeout=SCRAPER_TIMEOUT)
            r.raise_for_status()
            dest.write_bytes(r.content)
            return str(dest)
        except (ReqTimeout, RequestException) as e:
            delay = BASE_DELAY * 2 ** (attempt - 1)
            logging.warning(
                "Timeout saving image %s (try %s/%s): %s; retry in %.1fs",
                fn, attempt, MAX_RETRIES, e, delay
            )
            time.sleep(delay)
    logging.error("Failed saving image %s after %s attempts", fn, MAX_RETRIES)
    return None

def load_catalog() -> List[Dict[str, Any]]:
    """Загружает каталог статей из catalog.json с блокировкой файла."""
    if not CATALOG_PATH.exists():
        return []
    try:
        with open(CATALOG_PATH, "r", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_SH)  # Блокировка для чтения
            data = json.load(f)
            return [item for item in data if isinstance(item, dict) and "id" in item]
    except (json.JSONDecodeError, UnicodeDecodeError) as e:
        logging.error("Catalog JSON decode error: %s", e)
        return []
    except IOError as e:
        logging.error("Catalog read error: %s", e)
        return []

def save_catalog(catalog: List[Dict[str, Any]]) -> None:
    """Сохраняет каталог статей в catalog.json с блокировкой файла."""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    minimal = []
    for item in catalog:
        if isinstance(item, dict) and "id" in item:
            minimal.append({
                "id": item["id"],
                "hash": item.get("hash", ""),
                "translated_to": item.get("translated_to", "")
            })
        else:
            logging.warning(f"Skipping malformed catalog entry: {item}")
    try:
        with open(CATALOG_PATH, "w", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_EX) # Блокировка для записи
            json.dump(minimal, f, ensure_ascii=False, indent=2)
    except IOError as e:
        logging.error("Failed to save catalog: %s", e)

def translate_text(text: str, to_lang: str = "ru", provider: str = "yandex") -> str:
    """Перевод текста через translators с защитой от ошибок."""
    if not text or not isinstance(text, str):
        return ""
    try:
        # Не логируем каждый мелкий перевод, чтобы не засорять вывод
        # logging.info(f"Translating text (provider: {provider}) to {to_lang}...")
        translated = ts.translate_text(text, translator=provider, from_language="en", to_language=to_lang)
        if isinstance(translated, str):
            return translated
        logging.warning("Translator returned non-str for text: %s", text[:50])
    except Exception as e:
        logging.warning("Translation error [%s -> %s]: %s", provider, to_lang, e)
    return text

def translate_in_chunks(paragraphs: List[str], to_lang: str, provider: str = "yandex", chunk_size: int = 4500) -> List[str]:
    """Переводит список абзацев, объединяя их в чанки для сохранения контекста."""
    logging.info(f"Translating {len(paragraphs)} paragraphs in chunks to '{to_lang}'...")
    full_text = "\n\n".join(paragraphs)
    if len(full_text) <= chunk_size:
        logging.info("Entire article is within chunk size, translating all at once.")
        translated_full_text = translate_text(full_text, to_lang=to_lang, provider=provider)
        return translated_full_text.split("\n\n")

    translated_paragraphs = []
    current_chunk = []
    current_len = 0
    for p in paragraphs:
        if current_len + len(p) + 2 > chunk_size and current_chunk:
            text_to_translate = "\n\n".join(current_chunk)
            translated_chunk_text = translate_text(text_to_translate, to_lang=to_lang, provider=provider)
            translated_paragraphs.extend(translated_chunk_text.split("\n\n"))
            current_chunk = [p]
            current_len = len(p)
        else:
            current_chunk.append(p)
            current_len += len(p) + 2

    if current_chunk:
        text_to_translate = "\n\n".join(current_chunk)
        translated_chunk_text = translate_text(text_to_translate, to_lang=to_lang, provider=provider)
        translated_paragraphs.extend(translated_chunk_text.split("\n\n"))

    return translated_paragraphs


def parse_and_save(post: Dict[str, Any], translate_to: str, base_url: str) -> Optional[Dict[str, Any]]:
    """Парсит и сохраняет статью, включая перевод и загрузку изображений."""
    aid, slug = str(post["id"]), post["slug"]
    art_dir = OUTPUT_DIR / f"{aid}_{slug}"
    art_dir.mkdir(parents=True, exist_ok=True)

    meta_path = art_dir / "meta.json"
    current_hash = hashlib.sha256(post["content"]["rendered"].encode()).hexdigest()
    if meta_path.exists():
        try:
            existing_meta = json.loads(meta_path.read_text(encoding="utf-8"))
            if existing_meta.get("hash") == current_hash and existing_meta.get("translated_to", "") == translate_to:
                logging.info(f"Skipping unchanged article ID={aid} (content and translation match local cache).")
                return existing_meta
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logging.warning(f"Failed to read existing meta for ID={aid}: {e}. Reparsing.")

    orig_title = BeautifulSoup(post["title"]["rendered"], "html.parser").get_text(strip=True)
    title = orig_title
    if translate_to:
        title = translate_text(orig_title, to_lang=translate_to, provider="yandex")

    soup = BeautifulSoup(post["content"]["rendered"], "html.parser")
    paras = [p.get_text(strip=True) for p in soup.find_all("p")]
    raw_text = "\n\n".join(paras)
    raw_text = bad_re.sub("", raw_text)
    raw_text = re.sub(r"[ \t]+", " ", raw_text)
    raw_text = re.sub(r"\n{3,}", "\n\n", raw_text)

    img_dir = art_dir / "images"
    images: List[str] = []
    srcs = {extract_img_url(img) for img in soup.find_all("img")[:10]}
    srcs.discard(None) # Удаляем None, если extract_img_url ничего не вернул

    with ThreadPoolExecutor(max_workers=5) as ex:
        futures = {ex.submit(save_image, url, img_dir): url for url in srcs}
        for fut in as_completed(futures):
            if path := fut.result():
                images.append(path)

    if not images and "_embedded" in post:
        media = post["_embedded"].get("wp:featuredmedia")
        if media and media[0].get("source_url"):
            if path := save_image(media[0]["source_url"], img_dir):
                images.append(path)

    if not images:
        logging.warning("No images for ID=%s; skipping article parsing and saving.", aid)
        return None

    meta = {
        "id": aid, "slug": slug,
        "date": post.get("date"), "link": post.get("link"),
        "title": title,
        "text_file": str(art_dir / "content.txt"),
        "images": sorted(images), # Сортируем для консистентности
        "posted": False,
        "hash": current_hash,
        "translated_to": ""
    }
    (art_dir / "content.txt").write_text(raw_text, encoding="utf-8")

    if translate_to:
        clean_paras = [bad_re.sub("", p) for p in paras if p]
        trans_paras = []
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                trans_paras = translate_in_chunks(clean_paras, to_lang=translate_to, provider="yandex")
                break
            except Exception as e:
                delay = BASE_DELAY * 2 ** (attempt - 1)
                logging.warning("Translate content attempt %s failed: %s; retry in %.1fs", attempt, e, delay)
                time.sleep(delay)
        else:
             logging.error("Translation failed after max retries for ID=%s.", aid)
             return meta # Возвращаем метаданные без перевода

        trans_txt = "\n\n".join(trans_paras)
        txt_t = art_dir / f"content.{translate_to}.txt"
        header_t = f"{title}\n\n\n"
        txt_t.write_text(header_t + trans_txt, encoding="utf-8")
        meta.update({
            "translated_to": translate_to,
            "translated_paras": trans_paras,
            "translated_file": str(txt_t),
            "text_file": str(txt_t)
        })

    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    return meta


def main():
    """Основная функция для запуска парсера."""
    parser = argparse.ArgumentParser(description="Parser with translation")
    parser.add_argument("--base-url", type=str, default="https://www.thethaiger.com", help="WP site base URL")
    parser.add_argument("--slug", type=str, default="national", help="Category slug")
    parser.add_argument("-n", "--limit", type=int, default=10, help="Max posts to parse")
    parser.add_argument("-l", "--lang", type=str, default="", help="Translate to language code")
    parser.add_argument("--posted-state-file", type=str, default="articles/posted.json", help="Path to state file with published article IDs")
    args = parser.parse_args()

    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        cid = fetch_category_id(args.base_url, args.slug)
        # Увеличиваем per_page, чтобы получить больше кандидатов для фильтрации
        posts = fetch_posts(args.base_url, cid, per_page=(args.limit or 10) * 2)

        catalog = load_catalog()
        existing_ids_in_catalog = {article["id"] for article in catalog}
        posted_ids_from_repo = load_posted_ids(Path(args.posted_state_file))
        logging.info(f"Loaded {len(posted_ids_from_repo)} posted IDs from {args.posted_state_file}.")

        new_articles_processed_in_run = 0
        processed_count = 0
        
        for post in posts:
            if args.limit and processed_count >= args.limit:
                logging.info(f"Reached processing limit of {args.limit} articles.")
                break
            
            post_id = str(post["id"])
            if post_id in posted_ids_from_repo:
                logging.info(f"Skipping article ID={post_id} as it's already in {args.posted_state_file}.")
                continue

            if meta := parse_and_save(post, args.lang, args.base_url):
                processed_count += 1
                if post_id not in existing_ids_in_catalog:
                    new_articles_processed_in_run += 1
                    logging.info(f"Processed new article ID={post_id} and added to local catalog.")
                else:
                    logging.info(f"Updated article ID={post_id} in local catalog.")
                
                catalog = [item for item in catalog if item.get("id") != post_id]
                catalog.append(meta)
                existing_ids_in_catalog.add(post_id)

        save_catalog(catalog)
        if new_articles_processed_in_run > 0:
            logging.info(f"Added {new_articles_processed_in_run} new articles. Total in catalog: {len(catalog)}")
            print("NEW_ARTICLES_STATUS:true")
        else:
            logging.info("No new articles found. Local catalog might have been updated.")
            print("NEW_ARTICLES_STATUS:false")

    except Exception as e:
        logging.exception("Fatal error in main:")
        exit(1)

if __name__ == "__main__":
    main()
