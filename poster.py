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

# --- КОНФИГУРАЦИЯ ---
# Лимит записей в истории (храним 5000 последних для защиты от дублей)
MAX_POSTED_RECORDS = 5000 
WATERMARK_SCALE = 0.35

# Увеличенные таймауты для стабильной работы через прокси/WARP
HTTPX_TIMEOUT = Timeout(connect=30.0, read=60.0, write=120.0, pool=10.0)

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

def apply_watermark(img_path: Path, scale: float) -> bytes:
    try:
        base_img = Image.open(img_path).convert("RGBA")
        base_width, _ = base_img.size
        watermark_path = Path(__file__).parent / "watermark.png"
        
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
                logging.warning(f"🐢 Rate limit. Ждем {retry_after} сек...")
                await asyncio.sleep(retry_after)
            elif 400 <= e.response.status_code < 500:
                logging.error(f"❌ Ошибка Telegram {e.response.status_code}: {e.response.text}")
                return False
            else:
                logging.warning(f"⚠️ Ошибка сервера {e.response.status_code}. Попытка {attempt}/{MAX_RETRIES}...")
                await asyncio.sleep(RETRY_DELAY * attempt)
        except (ReadTimeout, httpx.RequestError) as e:
            logging.warning(f"⏱️ Ошибка сети: {e}. Попытка {attempt}/{MAX_RETRIES}...")
            await asyncio.sleep(RETRY_DELAY * attempt)
    return False

async def send_media_group(client: httpx.AsyncClient, token: str, chat_id: str, images: List[Path], watermark_scale: float) -> bool:
    url = f"https://api.telegram.org/bot{token}/sendMediaGroup"
    media, files = [], {}
    loop = asyncio.get_running_loop()
    
    for idx, img_path in enumerate(images[:10]):
        image_bytes = await loop.run_in_executor(None, apply_watermark, img_path, watermark_scale)
        if image_bytes:
            key = f"photo{idx}"
            files[key] = (f"img_{idx}.jpg", image_bytes, "image/jpeg")
            media.append({"type": "photo", "media": f"attach://{key}"})
            
    if not media: return False
    data = {"chat_id": chat_id, "media": json.dumps(media)}
    return await _post_with_retry(client, "POST", url, data, files)

async def send_message(client: httpx.AsyncClient, token: str, chat_id: str, text: str, **kwargs) -> bool:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    data = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    if kwargs.get("reply_markup"):
        data["reply_markup"] = json.dumps(kwargs["reply_markup"])
    return await _post_with_retry(client, "POST", url, data)

def load_posted_ids(state_file: Path) -> Set[str]:
    if not state_file.is_file(): return set()
    try:
        data = json.loads(state_file.read_text(encoding="utf-8"))
        if not isinstance(data, list): return set()
        return {str(item) for item in data if item is not None}
    except Exception: return set()

def save_posted_ids(all_ids_to_save: Set[str], state_file: Path) -> None:
    """Атомарно сохраняет ID в файл, обрезая историю до лимита."""
    state_file.parent.mkdir(parents=True, exist_ok=True)
    try:
        # Преобразуем в int для правильной сортировки (99 < 100)
        sorted_ids = sorted([int(i) for i in all_ids_to_save])
        if len(sorted_ids) > MAX_POSTED_RECORDS:
            sorted_ids = sorted_ids[-MAX_POSTED_RECORDS:]
        
        # Сохраняем через временный файл, чтобы не повредить основной при сбое
        temp_file = state_file.with_suffix(".tmp")
        with temp_file.open("w", encoding="utf-8") as f:
            json.dump(sorted_ids, f, ensure_ascii=False, indent=2)
        temp_file.replace(state_file)
        
        logging.info(f"💾 История обновлена: {len(sorted_ids)} ID сохранено.")
    except Exception as e:
        logging.error(f"Ошибка при сохранении posted.json: {e}")

async def main(parsed_dir: str, state_path: str, limit: Optional[int], watermark_scale: float):
    token, chat_id = os.getenv("TELEGRAM_TOKEN"), os.getenv("TELEGRAM_CHANNEL")
    if not token or not chat_id:
        logging.error("❌ TELEGRAM_TOKEN или TELEGRAM_CHANNEL не найдены в переменных окружения.")
        return

    parsed_root, state_file = Path(parsed_dir), Path(state_path)
    posted_ids = load_posted_ids(state_file)
    logging.info(f"Загружено {len(posted_ids)} опубликованных ID из истории.")
    
    articles_to_post = []
    if parsed_root.is_dir():
        for d in sorted(parsed_root.iterdir()):
            meta_file = d / "meta.json"
            if d.is_dir() and meta_file.is_file():
                try:
                    art_meta = json.loads(meta_file.read_text(encoding="utf-8"))
                    article_id = str(art_meta.get("id"))
                    
                    if article_id and article_id not in posted_ids:
                        text_file = art_meta.get("text_file")
                        if text_file and (d / text_file).is_file():
                            # Ищем картинки в папке images
                            images_dir = d / "images"
                            valid_imgs = []
                            if images_dir.is_dir():
                                valid_imgs = sorted([p for p in images_dir.iterdir() if p.suffix.lower() in {".jpg", ".jpeg", ".png"}])
                            
                            articles_to_post.append({
                                "id": article_id,
                                "html_title": f"<b>{escape_html(art_meta.get('title', '').strip())}</b>",
                                "text_path": d / text_file,
                                "image_paths": valid_imgs,
                                "original_title": art_meta.get('title', '').strip()
                            })
                except Exception as e:
                    logging.warning(f"Ошибка чтения метаданных в {d}: {e}")

    # Сортируем: сначала публикуем более старые (меньший ID)
    articles_to_post.sort(key=lambda x: int(x["id"]))
    
    if not articles_to_post:
        logging.info("🔍 Новых статей для публикации не найдено.")
        return

    logging.info(f"Найдено {len(articles_to_post)} статей для отправки.")

    async with httpx.AsyncClient() as client:
        sent_count = 0
        for article in articles_to_post:
            if limit is not None and sent_count >= limit:
                logging.info(f"🛑 Достигнут лимит пакета ({limit} статей).")
                break

            logging.info(f"🚀 Публикация ID={article['id']}...")
            try:
                # 1. Сначала отправляем альбом с картинками
                if article["image_paths"]:
                    await send_media_group(client, token, chat_id, article["image_paths"], watermark_scale)
                
                # 2. Подготовка и отправка текста
                raw_text = article["text_path"].read_text(encoding="utf-8")
                # Очищаем текст от заголовка, если он там продублирован
                cleaned_text = raw_text.lstrip()
                if cleaned_text.startswith(article["original_title"]):
                    cleaned_text = cleaned_text[len(article["original_title"]):].lstrip()

                full_html = f"{article['html_title']}\n\n{escape_html(cleaned_text)}"
                full_html = re.sub(r'\n{3,}', '\n\n', full_html).strip()
                chunks = chunk_text(full_html)

                for i, chunk in enumerate(chunks):
                    is_last = (i == len(chunks) - 1)
                    # Добавляем кнопки только к последнему куску текста
                    reply_markup = { 
                        "inline_keyboard": [[ 
                            {"text": "💰 Обмен валют", "url": "https://t.me/mister1dollar"}, 
                            {"text": "⭐️ Отзывы", "url": "https://t.me/feedback1dollar"} 
                        ]]
                    } if is_last else None
                    
                    if not await send_message(client, token, chat_id, chunk, reply_markup=reply_markup):
                        raise Exception(f"Не удалось отправить текст статьи {article['id']}")
                    await asyncio.sleep(0.5)

                # --- КЛЮЧЕВОЙ МОМЕНТ: Сохраняем прогресс сразу после успеха ---
                logging.info(f"✅ Статья ID={article['id']} успешно опубликована.")
                posted_ids.add(article['id'])
                save_posted_ids(posted_ids, state_file)
                # -------------------------------------------------------------
                
                sent_count += 1
                # Задержка между статьями
                await asyncio.sleep(float(os.getenv("POST_DELAY", DEFAULT_DELAY)))

            except Exception as e:
                logging.error(f"❌ Сбой при публикации ID={article['id']}: {e}")
                # Если одна статья упала, продолжаем следующую (или можно выйти, если ошибка критична)
                continue

    logging.info(f"🏁 Сессия завершена. Опубликовано статей: {sent_count}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--parsed-dir", type=str, default="articles")
    parser.add_argument("--state-file", type=str, default="articles/posted.json")
    parser.add_argument("-n", "--limit", type=int, default=None)
    parser.add_argument("--watermark-scale", type=float, default=WATERMARK_SCALE)
    args = parser.parse_args()
    asyncio.run(main(args.parsed_dir, args.state_file, args.limit, args.watermark_scale))
