#!/usr/bin/env python3
# coding: utf-8

import os
import json
import argparse
import asyncio
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from io import BytesIO

import httpx
from httpx import HTTPStatusError, ReadTimeout, Timeout
from PIL import Image

# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
# ──────────────────────────────────────────────────────────────────────────────

HTTPX_TIMEOUT = Timeout(connect=10.0, read=60.0, write=10.0, pool=5.0)
MAX_RETRIES   = 3
RETRY_DELAY   = 5.0
DEFAULT_DELAY = 5.0


def escape_markdown(text: str) -> str:
    """
    Экранирует спецсимволы для MarkdownV2.
    """
    markdown_chars = r'\_*[]()~`>#+-=|{}.!'
    return re.sub(r'([%s])' % re.escape(markdown_chars), r'\\\1', text)


def chunk_text(text: str, size: int = 4096) -> List[str]:
    """
    Делит текст на чанки длиной <= size, сохраняя абзацы.
    """
    norm = text.replace('\r\n', '\n')
    paras = [p for p in norm.split('\n\n') if p.strip()]
    chunks, curr = [], ""

    def split_long(p: str) -> List[str]:
        parts, sub = [], ""
        for w in p.split(" "):
            if len(sub) + len(w) + 1 > size:
                parts.append(sub)
                sub = w
            else:
                sub = (sub + " " + w).lstrip()
        if sub:
            parts.append(sub)
        return parts

    for p in paras:
        if len(p) > size:
            if curr:
                chunks.append(curr)
                curr = ""
            chunks.extend(split_long(p))
        else:
            if not curr:
                curr = p
            elif len(curr) + 2 + len(p) <= size:
                curr += "\n\n" + p
            else:
                chunks.append(curr)
                curr = p

    if curr:
        chunks.append(curr)
    return chunks


def apply_watermark(img_path: Path, scale: float = 0.45) -> bytes:
    """
    Накладывает watermark.png в правый верхний угол изображения.
    """
    base = Image.open(img_path).convert("RGBA")
    wm   = Image.open("watermark.png").convert("RGBA")
    filt = getattr(Image.Resampling, "LANCZOS", Image.LANCZOS)
    ratio = base.width * scale / wm.width
    wm = wm.resize((int(wm.width * ratio), int(wm.height * ratio)), resample=filt)
    base.paste(wm, (base.width - wm.width, 0), wm)
    buf = BytesIO()
    base.convert("RGB").save(buf, "PNG")
    return buf.getvalue()


async def _post_with_retry(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    data: Dict[str, Any],
    files: Optional[Dict[str, Any]] = None
) -> bool:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = await client.request(method, url, data=data, files=files, timeout=HTTPX_TIMEOUT)
            resp.raise_for_status()
            return True

        except ReadTimeout:
            logging.warning("⏱ Timeout %s/%s for %s", attempt, MAX_RETRIES, url)

        except HTTPStatusError as e:
            code = e.response.status_code
            text = e.response.text
            if code == 429:
                # Telegram присылает retry_after в JSON-параметрах
                info = e.response.json().get("parameters", {})
                wait = info.get("retry_after", RETRY_DELAY)
                logging.warning("🐢 Rate limited %s/%s: retry after %s seconds", attempt, MAX_RETRIES, wait)
                await asyncio.sleep(wait)
                continue
            if 400 <= code < 500:
                logging.error("❌ %s %s: %s", method, code, text)
                return False
            logging.warning("⚠️ %s %s, retry %s/%s", method, code, attempt, MAX_RETRIES)

        await asyncio.sleep(RETRY_DELAY)

    logging.error("☠️ Failed %s after %s attempts", url, MAX_RETRIES)
    return False

async def send_media_group(client, token: str, chat_id: str, images: list) -> bool:
    # Оставляем только первые 10 картинок
    images = images[:10]
    if not images:
        logging.warning("Нет изображений для отправки")
        return True

    files: Dict[str, Tuple[str, Any, str]] = {}
    media: List[Dict[str, Any]] = []
    for idx, img in enumerate(images):
        key = f"photo{idx}"
        files[key] = (img.name, apply_watermark(img), "image/png")
        media.append({"type": "photo", "media": f"attach://{key}"})

    resp = await client.post(
        f"https://api.telegram.org/bot{token}/sendMediaGroup",
        data={"chat_id": chat_id, "media": json.dumps(media)},
        files=files
    )
    # httpx.Response и aiohttp.ClientResponse не имеют .ok
    status = getattr(resp, "status_code", None) or getattr(resp, "status", None)
    text   = await resp.text()
    if status != 200:
        logging.error("❌ POST %s → %s: %s", resp.url, status, text)
        return False
    return True


async def send_message(
    client: httpx.AsyncClient,
    token: str,
    chat_id: str,
    text: str
) -> bool:
    """
    Отправляет текстовое сообщение с разбором MarkdownV2.
    """
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {
        "chat_id": chat_id,
        "text": escape_markdown(text),
        "parse_mode": "MarkdownV2"
    }
    return await _post_with_retry(client, "POST", url, data)


def validate_article(
    art: Dict[str, Any],
    article_dir: Path
) -> Optional[Tuple[str, Path, List[Path]]]:
    """
    Проверяет структуру статьи:
      - title → caption
      - наличе текстового файла
      - сбор картинок
    Возвращает (caption, text_path, images).
    """
    aid      = art.get("id")
    title    = art.get("title", "").strip()
    txt_name = Path(art.get("text_file", "")).name
    imgs     = art.get("images", [])

    if not title:
        logging.error("Invalid title for %s", aid)
        return None

    text_path = article_dir / txt_name
    if not text_path.is_file():
        candidates = list(article_dir.glob("*.txt"))
        if not candidates:
            logging.error("No text file in %s for %s", article_dir, aid)
            return None
        text_path = candidates[0]

    valid_imgs: List[Path] = []
    for name in imgs:
        p = article_dir / Path(name).name
        if not p.is_file():
            p = article_dir / "images" / Path(name).name
        if p.is_file():
            valid_imgs.append(p)

    if not valid_imgs:
        imgs_dir = article_dir / "images"
        if imgs_dir.is_dir():
            valid_imgs = [
                p for p in imgs_dir.iterdir()
                if p.suffix.lower() in (".jpg", ".jpeg", ".png")
            ]
        if not valid_imgs:
            logging.error("No images in %s for %s", article_dir, aid)
            return None

    cap = title if len(title) <= 1024 else title[:1023] + "…"
    return cap, text_path, valid_imgs


def load_posted_ids(state_file: Path) -> Set[int]:
    """
    Читает state-файл и возвращает set опубликованных ID.
    Поддерживает:
      - отсутствующий или пустой файл
      - список чисел [1,2,3]
      - список объектов [{"id":1}, {"id":2}]
    """
    if not state_file.is_file():
        return set()

    text = state_file.read_text(encoding="utf-8").strip()
    if not text:
        return set()

    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        logging.warning("State file not JSON: %s", state_file)
        return set()

    if not isinstance(data, list):
        logging.warning("State file is not a list: %s", state_file)
        return set()

    ids: Set[int] = set()
    for item in data:
        if isinstance(item, dict) and "id" in item:
            try:
                ids.add(int(item["id"]))
            except (ValueError, TypeError):
                pass
        elif isinstance(item, (int, str)) and str(item).isdigit():
            ids.add(int(item))
    return ids


def save_posted_ids(ids: Set[int], state_file: Path) -> None:
    """
    Сохраняет отсортированный список ID в state-файл.
    """
    state_file.parent.mkdir(parents=True, exist_ok=True)
    arr = sorted(ids)
    state_file.write_text(
        json.dumps(arr, ensure_ascii=False, indent=2),
        encoding="utf-8"
    )
    logging.info("Saved %d IDs to %s", len(arr), state_file)


async def main(parsed_dir: str, state_path: str, limit: Optional[int]):
    token       = os.getenv("TELEGRAM_TOKEN")
    chat_id     = os.getenv("TELEGRAM_CHANNEL")
    if not token or not chat_id:
        logging.error("TELEGRAM_TOKEN or TELEGRAM_CHANNEL not set")
        return

    delay       = float(os.getenv("POST_DELAY", DEFAULT_DELAY))
    parsed_root = Path(parsed_dir)
    state_file  = Path(state_path)

    if not parsed_root.is_dir():
        logging.error("Parsed directory %s does not exist", parsed_root)
        return

    # 1) Загрузка уже опубликованных ID
    posted_ids_old = load_posted_ids(state_file)
    logging.info("Loaded %d published IDs", len(posted_ids_old))

    # 2) Сбор папок со статьями
    parsed: List[Tuple[Dict[str, Any], Path]] = []
    for d in sorted(parsed_root.iterdir()):
        meta = d / "meta.json"
        if d.is_dir() and meta.is_file():
            try:
                art = json.loads(meta.read_text(encoding="utf-8"))
                parsed.append((art, d))
            except Exception as e:
                logging.warning("Cannot load meta %s: %s", d.name, e)

    # 2.1) Проверка: есть ли что публиковать?
    new_candidates = [
        art.get("id") for art, _ in parsed
        if art.get("id") not in posted_ids_old
    ]
    if not new_candidates:
        logging.info("🔍 No new articles to post (total known IDs: %d)", len(posted_ids_old))
        return

    client   = httpx.AsyncClient(timeout=HTTPX_TIMEOUT)
    sent     = 0
    new_ids: Set[int] = set()

    # 3) Публикация каждой статьи
    for art, article_dir in parsed:
        aid = art.get("id")
        if aid in posted_ids_old:
            continue
        if limit and sent >= limit:
            break

        validated = validate_article(art, article_dir)
        if not validated:
            continue

        caption, text_path, images = validated

        # 3.1) Альбом фото без подписи
        if not await send_media_group(client, token, chat_id, images):
            continue

        # 3.2) Подпись отдельным сообщением
        # await send_message(client, token, chat_id, caption)

        # 3.3) Тело статьи по чанкам
        raw    = text_path.read_text(encoding="utf-8")
        chunks = chunk_text(raw)
        for part in chunks:
            await send_message(client, token, chat_id, part)

        new_ids.add(aid)
        sent += 1
        logging.info("✅ Posted ID=%s", aid)
        await asyncio.sleep(delay)

    await client.aclose()

    # 4) Сохраняем обновлённый список ID
    all_ids = posted_ids_old.union(new_ids)
    save_posted_ids(all_ids, state_file)
    logging.info("State updated with %d total IDs", len(all_ids))
    logging.info("📢 Done: sent %d articles", sent)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Poster: публикует статьи пакетами в Telegram"
    )
    parser.add_argument(
        "--parsed-dir",
        type=str,
        default="articles",
        help="директория с распарсенными статьями"
    )
    parser.add_argument(
        "--state-file",
        type=str,
        default="articles/posted.json",
        help="путь к state-файлу"
    )
    parser.add_argument(
        "-n", "--limit",
        type=int,
        default=None,
        help="максимальное число статей для отправки"
    )

    args = parser.parse_args()
    asyncio.run(main(
        parsed_dir=args.parsed_dir,
        state_path=args.state_file,
        limit=args.limit
    ))
