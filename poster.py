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

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

MAX_POSTED_RECORDS = 100
WATERMARK_SCALE = 0.35

# --- ИСПРАВЛЕНИЕ 1: Увеличенные таймауты ---
# write=120.0 (было 10.0) - критично для отправки фото через VPN/WARP
HTTPX_TIMEOUT = Timeout(connect=20.0, read=60.0, write=120.0, pool=10.0)

MAX_RETRIES   = 3
RETRY_DELAY   = 5.0
DEFAULT_DELAY = 10.0

def escape_html(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")

def chunk_text(text: str, size: int = 4096) -> List[str]:
    paras = [p for p in text.replace('\r\n', '\n').split('\n\n') if p.strip()]
    chunks, current_chunk = [], ""
    for p in paras:
        if len(p) > size:
            if current_chunk: chunks.append(current_chunk)
            parts, sub_part = [], ""
            for word in p.split():
                if len(sub_part) + len(word) + 1 > size:
                    parts.append(sub_part)
                    sub_part = word
                else:
                    sub_part = f"{sub_part} {word}".lstrip()
            if sub_part: parts.append(sub_part)
            chunks.extend(parts)
            current_chunk = ""
        else:
            if not current_chunk: current_chunk = p
            elif len(current_chunk) + len(p) + 2 <= size: current_chunk += f"\n\n{p}"
            else:
                chunks.append(current_chunk)
                current_chunk = p
    if current_chunk: chunks.append(current_chunk)
    return chunks

# Эта функция тяжелая для CPU, поэтому будем запускать её в executor'е
def apply_watermark(img_path: Path, scale: float) -> bytes:
    try:
        base_img = Image.open(img_path).convert("RGBA")
        base_width, _ = base_img.size
        
        # Пытаемся найти watermark рядом со скриптом
        watermark_path = Path(__file__).parent / "watermark.png"
        
        # Если нет водяного знака, возвращаем оригинал (сжатый)
        if not watermark_path.exists():
            img_byte_arr = BytesIO()
            base_img.convert("RGB").save(img_byte_arr, format='JPEG', quality=90)
            return img_byte_arr.getvalue()

        watermark_img = Image.open(watermark_path).convert("RGBA")
        wm_width, wm_height = watermark_img.size
        
        new_wm_width = int(base_width * scale)
        if new_wm_width <= 0: new_wm_width = 1
        new_wm_height = int(wm_height * (new_wm_width / wm_width))
        
        resample_filter = getattr(Image.Resampling, "LANCZOS", Image.LANCZOS)
        watermark_img = watermark_img.resize((new_wm_width, new_wm_height), resample=resample_filter)
        
        overlay = Image.new("RGBA", base_img.size, (0, 0, 0, 0))
        padding = int(base_width * 0.02)
        position = (base_width - new_wm_width - padding, padding)
        
        overlay.paste(watermark_img, position, watermark_img)
        composite_img = Image.alpha_composite(base_img, overlay).convert("RGB")
        
        img_byte_arr = BytesIO()
        composite_img.save(img_byte_arr, format='JPEG', quality=90)
        return img_byte_arr.getvalue()
    except Exception as e:
        logging.error(f"Не удалось наложить водяной знак на {img_path}: {e}")
        return b""

async def _post_with_retry(client: httpx.AsyncClient, method: str, url: str, data: Dict[str, Any], files: Optional[Dict[str, Any]] = None) -> bool:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = await client.request(method, url, data=data, files=files, timeout=HTTPX_TIMEOUT)
            resp.raise_for_status()
            return True
        except HTTPStatusError as e:
            if e.response.status_code == 429:
                retry_after = int(e.response.json().get("parameters", {}).get("retry_after", RETRY_DELAY))
                logging.warning(f"🐢 Rate limited. Ждем {retry_after} сек...")
                await asyncio.sleep(retry_after)
            elif 400 <= e.response.status_code < 500:
                logging.error(f"❌ Ошибка клиента {e.response.status_code}: {e.response.text}")
                return False
            else:
                logging.warning(f"⚠️ Ошибка сервера {e.response.status_code}. Попытка {attempt}/{MAX_RETRIES}...")
                await asyncio.sleep(RETRY_DELAY * attempt)
        except (ReadTimeout, httpx.RequestError) as e:
            logging.warning(f"⏱️ Ошибка сети (возможно WARP тормозит): {e}. Попытка {attempt}/{MAX_RETRIES}...")
            await asyncio.sleep(RETRY_DELAY * attempt)
    logging.error(f"☠️ Не удалось выполнить запрос к {url} после {MAX_RETRIES} попыток.")
    return False

async def send_media_group(client: httpx.AsyncClient, token: str, chat_id: str, images: List[Path], watermark_scale: float) -> bool:
    url = f"https://api.telegram.org/bot{token}/sendMediaGroup"
    media, files = [], {}
    
    # --- ИСПРАВЛЕНИЕ 2: Обработка изображений без блокировки Event Loop ---
    loop = asyncio.get_running_loop()
    
    # Ограничиваемся 10 картинками (лимит Telegram)
    processed_count = 0
    for idx, img_path in enumerate(images[:10]):
        # Запускаем тяжелую задачу в отдельном потоке
        image_bytes = await loop.run_in_executor(None, apply_watermark, img_path, watermark_scale)
        
        if image_bytes:
            key = f"photo{idx}"
            # Важно: имя файла должно быть латиницей или простым, чтобы не ломать multipart
            files[key] = (f"img_{idx}.jpg", image_bytes, "image/jpeg")
            media.append({"type": "photo", "media": f"attach://{key}"})
            processed_count += 1
            
    if not media: 
        return False
        
    logging.info(f"📤 Отправка альбома из {processed_count} фото...")
    data = {"chat_id": chat_id, "media": json.dumps(media)}
    return await _post_with_retry(client, "POST", url, data, files)

async def send_message(client: httpx.AsyncClient, token: str, chat_id: str, text: str, **kwargs) -> bool:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if kwargs.get("reply_markup"):
        data["reply_markup"] = json.dumps(kwargs["reply_markup"])
    return await _post_with_retry(client, "POST", url, data)

def validate_article(art: Dict[str, Any], article_dir: Path) -> Optional[Tuple[str, Path, List[Path], str]]:
    aid = art.get("id")
    title = art.get("title", "").strip()
    text_filename = art.get("text_file")
    if not all([aid, title, text_filename]):
        return None
    text_path = article_dir / text_filename
    if not text_path.is_file():
        return None
    images_dir = article_dir / "images"
    valid_imgs: List[Path] = []
    if images_dir.is_dir():
        valid_imgs = sorted([p for p in images_dir.iterdir() if p.suffix.lower() in {".jpg", ".jpeg", ".png"}])
    html_title = f"<b>{escape_html(title)}</b>"
    return html_title, text_path, valid_imgs, title

def load_posted_ids(state_file: Path) -> Set[str]:
    if not state_file.is_file(): return set()
    try:
        data = json.loads(state_file.read_text(encoding="utf-8"))
        if not isinstance(data, list): return set()
        if len(data) > MAX_POSTED_RECORDS:
            data = data[-MAX_POSTED_RECORDS:]
        return {str(item) for item in data if item is not None}
    except Exception:
        return set()

def save_posted_ids(all_ids_to_save: Set[str], state_file: Path) -> None:
    state_file.parent.mkdir(parents=True, exist_ok=True)
    try:
        sorted_ids = sorted([int(i) for i in all_ids_to_save])
        if len(sorted_ids) > MAX_POSTED_RECORDS:
            sorted_ids = sorted_ids[-MAX_POSTED_RECORDS:]
        with state_file.open("w", encoding="utf-8") as f:
            json.dump(sorted_ids, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logging.error(f"Ошибка сохранения состояния: {e}")

async def main(parsed_dir: str, state_path: str, limit: Optional[int], watermark_scale: float):
    token, chat_id = os.getenv("TELEGRAM_TOKEN"), os.getenv("TELEGRAM_CHANNEL")
    if not token or not chat_id:
        logging.error("❌ Не заданы TELEGRAM_TOKEN или TELEGRAM_CHANNEL")
        return

    parsed_root, state_file = Path(parsed_dir), Path(state_path)
    if not parsed_root.is_dir():
        logging.error(f"❌ Папка {parsed_root} не найдена")
        return

    posted_ids = load_posted_ids(state_file)
    articles_to_post = []
    
    # Сбор статей
    for d in sorted(parsed_root.iterdir()):
        meta_file = d / "meta.json"
        if d.is_dir() and meta_file.is_file():
            try:
                art_meta = json.loads(meta_file.read_text(encoding="utf-8"))
                article_id = str(art_meta.get("id"))
                if article_id and article_id != 'None' and article_id not in posted_ids:
                    if validated_data := validate_article(art_meta, d):
                        _, text_path, image_paths, original_title = validated_data
                        articles_to_post.append({
                            "id": article_id, "html_title": f"<b>{escape_html(original_title)}</b>",
                            "text_path": text_path, "image_paths": image_paths,
                            "original_title": original_title
                        })
            except Exception: pass

    # Сортировка (старые первыми)
    articles_to_post.sort(key=lambda x: int(x["id"]))
    
    if not articles_to_post:
        logging.info("🔍 Нет новых статей.")
        return

    logging.info(f"Найдено {len(articles_to_post)} новых статей. Начинаем публикацию...")

    async with httpx.AsyncClient() as client:
        sent_count = 0
        newly_posted_ids: Set[str] = set()

        for article in articles_to_post:
            if limit is not None and sent_count >= limit:
                logging.info(f"🛑 Лимит {limit} достигнут.")
                break

            logging.info(f"🚀 Публикация ID={article['id']} ({len(article['image_paths'])} фото)...")
            try:
                # Отправка фото
                if article["image_paths"]:
                    success = await send_media_group(client, token, chat_id, article["image_paths"], watermark_scale)
                    if not success:
                        logging.warning(f"⚠️ Фото для ID={article['id']} не отправлены (или пропущены), но пробуем текст.")

                # Подготовка текста
                raw_text = article["text_path"].read_text(encoding="utf-8")
                cleaned_text = raw_text.lstrip()
                if cleaned_text.startswith(article["original_title"]):
                    cleaned_text = cleaned_text[len(article["original_title"]):].lstrip()

                full_html = f"{article['html_title']}\n\n{escape_html(cleaned_text)}"
                full_html = re.sub(r'\n{3,}', '\n\n', full_html).strip()
                chunks = chunk_text(full_html)

                # Отправка частей текста
                for i, chunk in enumerate(chunks):
                    is_last_chunk = (i == len(chunks) - 1)
                    reply_markup = { "inline_keyboard": [[ {"text": "Обмен валют", "url": "https://t.me/mister1dollar"}, {"text": "Отзывы", "url": "https://t.me/feedback1dollar"} ]]} if is_last_chunk else None
                    
                    if not await send_message(client, token, chat_id, chunk, reply_markup=reply_markup):
                        raise Exception("Ошибка отправки текста")
                    
                    # Небольшая пауза между сообщениями одной статьи, чтобы сохранить порядок
                    await asyncio.sleep(0.5)

                logging.info(f"✅ Успешно: ID={article['id']}")
                newly_posted_ids.add(article['id'])
                sent_count += 1

            except Exception as e:
                logging.error(f"❌ Сбой публикации ID={article['id']}: {e}")

            # Задержка между статьями
            await asyncio.sleep(float(os.getenv("POST_DELAY", DEFAULT_DELAY)))

    if newly_posted_ids:
        all_ids_to_save = posted_ids.union(newly_posted_ids)
        save_posted_ids(all_ids_to_save, state_file)

    logging.info(f"🏁 Работа завершена. Опубликовано: {sent_count}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--parsed-dir", type=str, default="articles")
    parser.add_argument("--state-file", type=str, default="articles/posted.json")
    parser.add_argument("-n", "--limit", type=int, default=None)
    parser.add_argument("--watermark-scale", type=float, default=WATERMARK_SCALE)
    args = parser.parse_args()
    asyncio.run(main(args.parsed_dir, args.state_file, args.limit, args.watermark_scale))
