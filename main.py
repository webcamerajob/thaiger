import argparse
import logging
import time
import json
import re
import fcntl
import hashlib
from pathlib import Path
from typing import List, Dict, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed

import cloudscraper
from bs4 import BeautifulSoup
from requests.exceptions import RequestException, Timeout as ReqTimeout
import translators as ts

# ─── Настройки ────────────────────────────────────────────────────────────────
SCRAPER = cloudscraper.create_scraper()
SCRAPER_TIMEOUT = (10.0, 60.0)
MAX_RETRIES = 3
BASE_DELAY  = 2.0

OUTPUT_DIR   = Path("articles")
CATALOG_PATH = OUTPUT_DIR / "catalog.json"

bad_re = re.compile(r"\[\s*.*?\]")

# ─── Вспомогательные функции ──────────────────────────────────────────────────

def extract_img_url(img_tag):
    """Берёт первый src из тегов lazy-load, srcset и т.д."""
    for attr in ("data-src", "data-lazy-src", "data-srcset", "srcset", "src"):
        val = img_tag.get(attr)
        if not val:
            continue
        parts = val.replace(",", " ").split()
        if parts:
            return parts[0]
    return None

def fetch_post_urls(base_url: str, slug: str, limit: int = 10) -> List[str]:
    """
    Скрапит страницу /category/{slug}/, достаёт первые limit ссылок на статьи.
    """
    url = f"{base_url.rstrip('/')}/category/{slug}/"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SCRAPER.get(url, timeout=SCRAPER_TIMEOUT)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")

            # Забираем и из h2.entry-title, и из h3.post-block__title
            els = soup.select("h2.entry-title a") + soup.select("h3.post-block__title a")
            links = [a["href"] for a in els if a.get("href")]
            return links[:limit]
        except (ReqTimeout, RequestException) as e:
            delay = BASE_DELAY * 2**(attempt-1)
            logging.warning("Timeout fetching category page (%s/%s): %s; retry in %.1fs",
                            attempt, MAX_RETRIES, e, delay)
            time.sleep(delay)
    logging.error("Can’t fetch category page %s after %s tries", url, MAX_RETRIES)
    return []

def fetch_posts(base_url: str, urls: List[str]) -> List[Dict[str, Any]]:
    """
    По каждой ссылке качает страницу, вытягивает title, content (HTML) и собирает <img> теги.
    """
    posts = []
    for url in urls:
        try:
            r = SCRAPER.get(url, timeout=SCRAPER_TIMEOUT)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, "html.parser")

            title_el = soup.select_one("h1.entry-title")
            content_el = soup.select_one("div.entry-content")
            if not content_el or not title_el:
                continue

            post_id = int(hashlib.sha256(url.encode()).hexdigest(), 16) % (10**8)
            slug = url.rstrip("/").split("/")[-1]

            posts.append({
                "post_id": post_id,
                "slug": slug,
                "title": title_el.get_text(strip=True),
                "content": str(content_el),
                "link": url,
                "images": content_el.find_all("img")
            })
        except Exception as e:
            logging.warning("Failed to fetch post %s: %s", url, e)
    return posts

def save_image(src_url: str, folder: Path) -> Optional[str]:
    folder.mkdir(parents=True, exist_ok=True)
    fn = src_url.rsplit('/', 1)[-1].split('?', 1)[0]
    dest = folder / fn
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            r = SCRAPER.get(src_url, timeout=SCRAPER_TIMEOUT)
            r.raise_for_status()
            dest.write_bytes(r.content)
            return str(dest)
        except Exception as e:
            delay = BASE_DELAY * 2**(attempt-1)
            logging.warning("Timeout saving image %s (%s/%s): %s; retry in %.1fs",
                            fn, attempt, MAX_RETRIES, e, delay)
            time.sleep(delay)
    logging.error("Failed saving image %s after %s attempts", fn, MAX_RETRIES)
    return None

def load_catalog() -> List[Dict[str, Any]]:
    if not CATALOG_PATH.exists():
        return []
    try:
        with open(CATALOG_PATH, "r", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            data = json.load(f)
            return [i for i in data if isinstance(i, dict) and "post_id" in i]
    except Exception as e:
        logging.error("Catalog load error: %s", e)
        return []

def save_catalog(catalog: List[Dict[str, Any]]) -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    minimal = [{"post_id": i["post_id"], "hash": i["hash"], "translated_to": i.get("translated_to","")}
               for i in catalog]
    try:
        with open(CATALOG_PATH, "w", encoding="utf-8") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            json.dump(minimal, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.error("Catalog save error: %s", e)

def translate_text(text: str, to_lang: str = "ru", provider: str = "yandex") -> str:
    if not text or not isinstance(text, str):
        return ""
    try:
        return ts.translate_text(text, translator=provider,
                                 from_language="en", to_language=to_lang)
    except Exception as e:
        logging.warning("Translate error [%s→%s]: %s", provider, to_lang, e)
        return text

def parse_and_save(post: Dict[str, Any], translate_to: str, base_url: str) -> Optional[Dict[str, Any]]:
    aid, slug = post["post_id"], post["slug"]
    art_dir = OUTPUT_DIR / f"{aid}_{slug}"
    art_dir.mkdir(parents=True, exist_ok=True)

    meta_path = art_dir / "meta.json"
    current_hash = hashlib.sha256(
        BeautifulSoup(post["content"], "html.parser").get_text().encode()
    ).hexdigest()

    # дедупликация
    if meta_path.exists():
        try:
            old = json.loads(meta_path.read_text("utf-8"))
            if old.get("hash")==current_hash and old.get("translated_to","")==translate_to:
                logging.info("Skip unchanged %s", aid)
                return old
        except Exception:
            pass

    # текст + перевод
    orig_title = post["title"]
    title = translate_text(orig_title, to_lang=translate_to) if translate_to else orig_title

    soup = BeautifulSoup(post["content"], "html.parser")
    paras = [p.get_text(strip=True) for p in soup.find_all("p")]
    raw = "\n\n".join(paras)
    raw = bad_re.sub("", raw)
    raw = re.sub(r"[ \t]+"," ", raw)
    raw = re.sub(r"\n{3,}", "\n\n", raw)
    raw = f"**{title}**\n\n{raw}"
    (art_dir / "content.txt").write_text(raw, encoding="utf-8")

    # картинки
    img_dir = art_dir / "images"
    imgs = []
    urls = [extract_img_url(tag) for tag in post["images"]]
    with ThreadPoolExecutor(5) as ex:
        for fut in as_completed({ ex.submit(save_image,u,img_dir):u for u in urls if u }):
            if p := fut.result():
                imgs.append(p)
    if not imgs:
        logging.warning("No images for %s", aid)
        return None

    meta = {
        "post_id": aid,
        "slug": slug,
        "date": None,
        "link": post["link"],
        "title": title,
        "text_file": str(art_dir/"content.txt"),
        "images": imgs,
        "posted": False,
        "hash": current_hash
    }

    if translate_to:
        trans = [translate_text(p, to_lang=translate_to) for p in paras]
        tfile = art_dir/f"content.{translate_to}.txt"
        tfile.write_text(title+"\n\n\n"+"\n\n".join(trans), encoding="utf-8")
        meta.update({
            "translated_to": translate_to,
            "translated_paras": trans,
            "translated_file": str(tfile),
            "text_file": str(tfile)
        })

    meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")
    return meta

# ─── Точка входа ──────────────────────────────────────────────────────────────

def main():
    logging.basicConfig(level=logging.INFO,
                        format="%(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", type=str,
                        default="https://www.thethaiger.com",
                        help="Site base URL")
    parser.add_argument("--slug", type=str,
                        default="news",
                        help="Category slug")
    parser.add_argument("-n","--limit", type=int, default=10,
                        help="Max posts to parse")
    parser.add_argument("-l","--lang", type=str, default="ru",
                        help="Translate to lang")
    args = parser.parse_args()

    try:
        OUTPUT_DIR.mkdir(exist_ok=True)
        urls = fetch_post_urls(args.base_url, args.slug, args.limit)
        posts = fetch_posts(args.base_url, urls)

        catalog = load_catalog()
        seen = {i["post_id"] for i in catalog}
        added = 0

        for post in posts:
            if post["post_id"] in seen:
                continue
            if m := parse_and_save(post, args.lang, args.base_url):
                catalog.append(m)
                seen.add(post["post_id"])
                added += 1
                logging.info("Processed %s", post["post_id"])

        if added:
            save_catalog(catalog)
            logging.info("Added %s new articles (total %s)", added, len(catalog))
        else:
            logging.info("No new articles")

    except Exception:
        logging.exception("Fatal error")
        exit(1)

if __name__=="__main__":
    main()
