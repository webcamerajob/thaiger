import os
import json
import argparse
import asyncio
import logging
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from io import BytesIO
from collections import deque
import httpx
from httpx import HTTPStatusError, ReadTimeout, Timeout
from PIL import Image

# ──────────────────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
# --- Константа для ограничения количества записей в posted.json ---
MAX_POSTED_RECORDS = 100

# --- Настройка размера вотермарки ---
WATERMARK_SCALE = 0.35  # 35% от ширины изображения

# ──────────────────────────────────────────────────────────────────────────────
HTTPX_TIMEOUT = Timeout(connect=10.0, read=60.0, write=10.0, pool=5.0)
MAX_RETRIES   = 3
RETRY_DELAY   = 5.0
DEFAULT_DELAY = 10.0

def escape_html(text: str) -> str:
    """
    Экранирует спецсимволы HTML (<, >, &, ") для корректного отображения.
    """
    text = text.replace("&", "&amp;")
    text = text.replace("<", "&lt;")
    text = text.replace(">", "&gt;")
    text = text.replace('"', "&quot;")
    return text

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


def apply_watermark(img_path: Path, scale: float = 0.35) -> bytes:
    """
    Накладывает watermark.png и возвращает изображение в виде байтов JPEG.
    """
    try:
        base_img = Image.open(img_path).convert("RGBA")
        base_width, _ = base_img.size

        watermark_path = Path(__file__).parent / "watermark.png"
        if not watermark_path.exists():
            logging.warning("Файл watermark.png не найден. Возвращаем оригинальное изображение.")
            img_byte_arr = BytesIO()
            base_img.convert("RGB").save(img_byte_arr, format='JPEG', quality=90)
            return img_byte_arr.getvalue()

        watermark_img = Image.open(watermark_path).convert("RGBA")
        
        # Масштабирование водяного знака
        wm_width, wm_height = watermark_img.size
        new_wm_width = int(base_width * scale)
        new_wm_height = int(wm_height * (new_wm_width / wm_width))
        resample_filter = getattr(Image.Resampling, "LANCZOS", Image.LANCZOS)
        watermark_img = watermark_img.resize((new_wm_width, new_wm_height), resample=resample_filter)
        
        # Создаём пустой слой РАЗМЕРОМ С ОСНОВНОЕ ИЗОБРАЖЕНИЕ
        overlay = Image.new("RGBA", base_img.size, (0, 0, 0, 0))

        # Наносим вотермарку на этот слой в нужную позицию
        padding = int(base_width * 0.02)
        position = (base_width - new_wm_width - padding, padding)
        overlay.paste(watermark_img, position, watermark_img) # Используем маску для прозрачности

        # Совмещаем основное изображение со СЛОЕМ (overlay), а не с маленькой вотермаркой
        composite_img = Image.alpha_composite(base_img, overlay).convert("RGB")

        # Сохраняем в байты как JPEG для экономии места
        img_byte_arr = BytesIO()
        composite_img.save(img_byte_arr, format='JPEG', quality=90)
        return img_byte_arr.getvalue()
        
    except Exception as e:
        logging.error(f"Не удалось наложить водяной знак на {img_path}: {e}")
        # Возвращаем оригинал в случае ошибки
        try:
            with open(img_path, 'rb') as f:
                return f.read()
        except Exception as e_orig:
            logging.error(f"Не удалось даже прочитать оригинальное изображение {img_path}: {e_orig}")
            return b""
async def _post_with_retry(
    client: httpx.AsyncClient,
    method: str,
    url: str,
    data: Dict[str, Any],
    files: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Выполняет HTTP POST-запрос с повторными попытками.
    """
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
                info = e.response.json().get("parameters", {})
                wait = info.get("retry_after", RETRY_DELAY)
                logging.warning("🐢 Rate limited %s/%s: retry after %s seconds", attempt, MAX_RETRIES, wait)
                await asyncio.sleep(wait)
                continue
            if 400 <= code < 500:
                logging.error("❌ %s %s: %s", method, code, text)
                return False
            logging.warning("⚠️ %s %s, retry %s/%s", method, code, attempt, MAX_RETRIES)
        except httpx.RequestError as e:
            logging.warning(f"Request error on attempt {attempt + 1}/{MAX_RETRIES}: {e}")
        except Exception as e:
            logging.error(f"An unexpected error occurred on attempt {attempt + 1}/{MAX_RETRIES}: {e}")

        await asyncio.sleep(RETRY_DELAY)

    logging.error("☠️ Failed %s after %s attempts", url, MAX_RETRIES)
    return False


async def send_media_group(
    client: httpx.AsyncClient,
    token: str,
    chat_id: str,
    images: List[Path]
) -> bool:
    """
    Отправляет альбом фотографий без подписи.
    """
    url = f"https://api.telegram.org/bot{token}/sendMediaGroup"
    media = []
    files = {}

    if not images:
        logging.warning("No images provided for media group.")
        return False

    for idx, img_path in enumerate(images[:10]): # Ограничение Telegram на 10 фото
        try:
            image_bytes = apply_watermark(img_path, scale=WATERMARK_SCALE)
            if not image_bytes:
                logging.warning(f"Skipping image {img_path} due to empty bytes.")
                continue

            key = f"file{idx}"
            files[key] = (img_path.name, image_bytes, "image/jpeg")
            media.append({"type": "photo", "media": f"attach://{key}"})
        except Exception as e:
            logging.error(f"Error processing image {img_path} for media group: {e}")

    if not media:
        logging.warning("No valid images to send in media group.")
        return False

    data = {
        "chat_id": chat_id,
        "media": json.dumps(media, ensure_ascii=False)
    }
    return await _post_with_retry(client, "POST", url, data, files)


async def send_message(
    client: httpx.AsyncClient,
    token: str,
    chat_id: str,
    text: str,
    reply_markup: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Отправляет текстовое сообщение с разбором HTML.
    """
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True
    }
    if reply_markup:
        data["reply_markup"] = json.dumps(reply_markup, ensure_ascii=False)
    return await _post_with_retry(client, "POST", url, data)


def validate_article(
    art: Dict[str, Any],
    article_dir: Path
) -> Optional[Tuple[str, Path, List[Path], str]]:
    """
    Проверяет структуру папки статьи и возвращает подготовленные данные.
    """
    aid = art.get("id")
    title = art.get("title", "").strip()
    txt_name = Path(art.get("text_file", "")).name if art.get("text_file") else None

    if not all([aid, title, txt_name]):
        logging.error("Invalid meta.json in %s (missing id, title, or text_file).", article_dir)
        return None

    text_path = article_dir / txt_name
    if not text_path.is_file():
        logging.error("Text file %s not found for article in %s. Skipping.", text_path, article_dir)
        return None

    valid_imgs = sorted([
        article_dir / "images" / Path(p).name for p in art.get("images", [])
        if (article_dir / "images" / Path(p).name).is_file()
    ])

    html_title = f"<b>{escape_html(title)}</b>"
    return html_title, text_path, valid_imgs, title


def load_posted_ids(state_file: Path) -> Set[str]:
    """
    Читает state-файл, возвращает set из ID в виде СТРОК.
    Обрезает список до MAX_POSTED_RECORDS, сохраняя самые новые.
    """
    if not state_file.is_file():
        return set()
    
    ids_from_file: List[str] = []
    try:
        data = json.loads(state_file.read_text(encoding="utf-8"))
        if isinstance(data, list):
            # Преобразуем все элементы в строки для консистентности
            ids_from_file = [str(item) for item in data if item is not None]
    except (json.JSONDecodeError, Exception) as e:
        logging.warning(f"Error reading or parsing state file {state_file}: {e}. Returning empty set.")

    # Обрезаем список, сохраняя самые новые (в конце списка)
    if len(ids_from_file) > MAX_POSTED_RECORDS:
        start_index = len(ids_from_file) - MAX_POSTED_RECORDS
        ids_from_file = ids_from_file[start_index:]
        logging.info(f"State file trimmed to last {MAX_POSTED_RECORDS} records during load.")

    return set(ids_from_file)


def save_posted_ids(all_ids_to_save: Set[str], state_file: Path) -> None:
    """
    Сохраняет отсортированный список ID (как строки) в файл состояния.
    """
    state_file.parent.mkdir(parents=True, exist_ok=True)
    try:
        # Преобразуем в список и сортируем. Сортировка строк '10', '2' -> ['10', '2'].
        # Для числовой сортировки: sorted(list(all_ids_to_save), key=int)
        final_list_to_save = sorted(list(all_ids_to_save), key=int)

        with state_file.open("w", encoding="utf-8") as f:
            json.dump(final_list_to_save, f, ensure_ascii=False, indent=2)
        logging.info(f"Saved {len(final_list_to_save)} IDs to state file {state_file}")
    except Exception as e:
        logging.error(f"Failed to save state file {state_file}: {e}")


async def main(parsed_dir: str, state_path: str, limit: Optional[int]):
    """
    Основная функция для запуска постера.
    """
    token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHANNEL")
    if not token or not chat_id:
        logging.error("TELEGRAM_TOKEN and TELEGRAM_CHANNEL environment variables must be set.")
        return

    delay = float(os.getenv("POST_DELAY", DEFAULT_DELAY))
    parsed_root = Path(parsed_dir)
    state_file = Path(state_path)

    if not parsed_root.is_dir():
        logging.error(f"Directory {parsed_root} does not exist. Exiting.")
        return

    posted_ids = load_posted_ids(state_file)
    logging.info(f"Loaded {len(posted_ids)} previously posted IDs from {state_file.name}.")

    articles_to_post: List[Dict[str, Any]] = []
    for d in sorted(parsed_root.iterdir()):
        meta_file = d / "meta.json"
        if d.is_dir() and meta_file.is_file():
            try:
                art_meta = json.loads(meta_file.read_text(encoding="utf-8"))
                # Работаем с ID как со строками
                article_id = str(art_meta.get("id"))
                if article_id and article_id != 'None' and article_id not in posted_ids:
                    if validated_data := validate_article(art_meta, d):
                        html_title, text_path, image_paths, original_title = validated_data
                        articles_to_post.append({
                            "id": article_id,
                            "html_title": html_title,
                            "text_path": text_path,
                            "image_paths": image_paths,
                            "original_title": original_title
                        })
            except (json.JSONDecodeError, Exception) as e:
                logging.warning(f"Could not process meta.json in {d.name}: {e}. Skipping.")
    
    # Сортируем статьи по ID (числовому)
    articles_to_post.sort(key=lambda x: int(x["id"]))

    if not articles_to_post:
        logging.info("🔍 No new articles to post. Exiting.")
        return

    logging.info(f"Found {len(articles_to_post)} new articles to consider for posting.")

    client = httpx.AsyncClient()
    sent_count = 0
    newly_posted_ids: Set[str] = set()

    for article in articles_to_post:
        if limit is not None and sent_count >= limit:
            logging.info(f"Posting limit of {limit} reached. Stopping.")
            break

        aid = article["id"]
        logging.info(f"Attempting to post article ID={aid}")
        
        posted_successfully = False
        try:
            # Сначала отправляем медиа группу, если есть изображения
            if article["image_paths"]:
                if not await send_media_group(client, token, chat_id, article["image_paths"]):
                    logging.warning(f"Failed to send media group for ID={aid}. Continuing to send text.")

            raw_text = article["text_path"].read_text(encoding="utf-8")
            # Убираем заголовок из начала текста, если он там есть
            cleaned_text = raw_text.lstrip()
            if cleaned_text.startswith(article["original_title"]):
                 cleaned_text = cleaned_text[len(article["original_title"]):].lstrip()

            full_html_content = f"{article['html_title']}\n\n{escape_html(cleaned_text)}"
            chunks = chunk_text(full_html_content)

            all_chunks_sent = True
            for i, part in enumerate(chunks):
                reply_markup = None
                if i == len(chunks) - 1: # Добавляем кнопки к последнему сообщению
                    reply_markup = {
                        "inline_keyboard": [[
                            {"text": "Обмен валют", "url": "https://t.me/mister1dollar"},
                            {"text": "Отзывы", "url": "https://t.me/feedback1dollar"}
                        ]]
                    }
                
                if not await send_message(client, token, chat_id, part, reply_markup=reply_markup):
                    logging.error(f"Failed to send text chunk for ID={aid}. Skipping article.")
                    all_chunks_sent = False
                    break
            
            if all_chunks_sent:
                posted_successfully = True

        except Exception as e:
            logging.error(f"❌ An error occurred while posting article ID={aid}: {e}", exc_info=True)

        if posted_successfully:
            newly_posted_ids.add(aid)
            sent_count += 1
            logging.info(f"✅ Successfully posted ID={aid}")
        
        await asyncio.sleep(delay)

    await client.aclose()

    if newly_posted_ids:
        all_ids_to_save = posted_ids.union(newly_posted_ids)
        save_posted_ids(all_ids_to_save, state_file)
    
    logging.info(f"📢 Finished: Sent {sent_count} articles in this run.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Poster: posts articles in batches to Telegram"
    )
    parser.add_argument(
        "--parsed-dir",
        type=str,
        default="articles",
        help="directory with parsed articles"
    )
    parser.add_argument(
        "--state-file",
        type=str,
        default="articles/posted.json",
        help="path to the state file"
    )
    parser.add_argument(
        "-n", "--limit",
        type=int,
        default=None,
        help="maximum number of articles to send"
    )
    args = parser.parse_args()
    asyncio.run(main(
        parsed_dir=args.parsed_dir,
        state_path=args.state_file,
        limit=args.limit
    ))

