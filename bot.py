import asyncio
import json
import logging
import os
import re
import secrets
import shutil
import tempfile
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any, Optional

import aiohttp
import asyncpg
from aiohttp import web
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatAction, ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import CommandStart, Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    FSInputFile,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application
from PIL import Image, ImageDraw, ImageFont, ImageOps

# Telethon for large file downloads and login
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import RPCError, ChatAdminRequiredError, ChannelPrivateError, SessionPasswordNeededError, PhoneCodeInvalidError, PhoneCodeExpiredError
from telethon.tl.functions.messages import GetMessagesRequest

try:
    import imageio_ffmpeg
except Exception:
    imageio_ffmpeg = None

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

CONFIG_FILE = Path(__file__).with_name("config.json")


def _load_local_config() -> dict[str, Any]:
    if not CONFIG_FILE.exists():
        return {}
    try:
        data = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


LOCAL_CONFIG = _load_local_config()


def _cfg(name: str, default: str = "") -> str:
    value = os.getenv(name)
    if value is None or str(value).strip() == "":
        value = LOCAL_CONFIG.get(name, default)
    return str(value).strip()


ADMIN_ID = int(_cfg("ADMIN_ID", "8883527571"))
STORAGE_CHANNEL = int(_cfg("STORAGE_CHANNEL", "-1003890591020"))
BOT_TOKEN = _cfg("BOT_TOKEN")
DATABASE_URL = _cfg("DATABASE_URL")
BOT_USERNAME_ENV = _cfg("BOT_USERNAME")
WEBHOOK_BASE_URL = _cfg("WEBHOOK_BASE_URL") or _cfg("RENDER_EXTERNAL_URL")
WEBHOOK_PATH = _cfg("WEBHOOK_PATH", "/webhook")
PORT = int(_cfg("PORT", "8080"))

# Telethon credentials (for userbot session)
API_ID = int(_cfg("API_ID", "0"))
API_HASH = _cfg("API_HASH")
USER_SESSION_STRING = _cfg("USER_SESSION_STRING", "")

if not API_ID or not API_HASH:
    logging.warning("API_ID and API_HASH are not set. Telethon will not work.")
if not USER_SESSION_STRING:
    logging.warning("USER_SESSION_STRING is not set. Large file downloads will fall back to Bot API.")

MAX_WATERMARK_BYTES = 5 * 1024 * 1024
MAX_VIDEO_BYTES = 20 * 1024 * 1024
PREVIEW_TILE_SIZE = (640, 360)
PREVIEW_SIZES = [18, 26, 34, 42]
WATERMARK_MARGIN = 20
OUTPUT_MAX_WIDTH = 1280

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger("watermark-bot")

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is required")


# -----------------------------------------------------------------------------
# Globals
# -----------------------------------------------------------------------------

router = Router()
dp = Dispatcher(storage=MemoryStorage())
dp.include_router(router)

db_pool: Optional[asyncpg.Pool] = None
BOT_USERNAME: Optional[str] = BOT_USERNAME_ENV or None
PROCESS_LOCK = asyncio.Lock()

# Telethon client instance (the userbot for downloading)
telethon_client: Optional[TelegramClient] = None


@dataclass
class Job:
    job_id: str
    admin_id: int
    source_path: str
    source_filename: str
    frame_path: str
    preview_path: str
    frame_width: int
    frame_height: int
    media_type: str = "video"
    status: str = "pending"
    chosen_index: Optional[int] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


JOBS: dict[str, Job] = {}


class WatermarkState(StatesGroup):
    waiting_for_png = State()


class JoinState(StatesGroup):
    waiting_for_links = State()


# ========== Login States for 2FA ==========
class LoginState(StatesGroup):
    waiting_for_api_id = State()
    waiting_for_api_hash = State()
    waiting_for_phone = State()
    waiting_for_code = State()
    waiting_for_password = State()


# -----------------------------------------------------------------------------
# FFmpeg helpers
# -----------------------------------------------------------------------------

def _which_ffmpeg() -> str:
    if imageio_ffmpeg is not None:
        try:
            return imageio_ffmpeg.get_ffmpeg_exe()
        except Exception:
            pass
    for candidate in ("ffmpeg", "/usr/bin/ffmpeg", "/usr/local/bin/ffmpeg"):
        if shutil.which(candidate) or Path(candidate).exists():
            return candidate
    raise RuntimeError("ffmpeg is not available.")


FFMPEG = _which_ffmpeg()


async def run_cmd(cmd: list[str]) -> tuple[int, str, str]:
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, err = await proc.communicate()
    return proc.returncode, out.decode(errors="ignore"), err.decode(errors="ignore")


async def check_video_file(file_path: str) -> tuple[bool, str]:
    # Lightweight stub: keep the function for compatibility, but avoid ffmpeg checks.
    return True, ""


async def repair_video_file(input_path: str, output_path: str) -> tuple[bool, str]:
    # Disabled for speed. Keep the function for compatibility.
    return False, "disabled for speed"


async def extract_frame(video_path: str, out_path: str, at_seconds: float = 1.0) -> None:
    cmd = [FFMPEG, "-y", "-ss", str(at_seconds), "-i", video_path, "-frames:v", "1", "-q:v", "2", out_path]
    code, _, err = await run_cmd(cmd)
    if code != 0:
        raise RuntimeError(f"Frame extraction failed: {err[-1000:]}")


async def render_video_with_watermark(
    input_path: str,
    watermark_path: str,
    output_path: str,
    selected_percent: int,
    frame_width: int,
) -> None:
    target_width = min(frame_width, OUTPUT_MAX_WIDTH)
    watermark_width = max(64, int(target_width * selected_percent / 100))
    margin = WATERMARK_MARGIN

    cmd = [
        FFMPEG, "-y",
        "-i", input_path,
        "-loop", "1",
        "-i", watermark_path,
        "-filter_complex",
        f"[0:v]scale={target_width}:-2,format=yuv420p[base];[1:v]scale={watermark_width}:-1,format=rgba[wm];[base][wm]overlay=W-w-{margin}:H-h-{margin}:format=auto[v]",
        "-map", "[v]",
        "-map", "0:a?",
        "-map_metadata", "-1",
        "-map_chapters", "-1",
        "-c:v", "libx264",
        "-preset", "veryfast",
        "-crf", "28",
        "-pix_fmt", "yuv420p",
        "-c:a", "aac",
        "-b:a", "128k",
        "-movflags", "+faststart",
        "-shortest",
        output_path,
    ]
    code, _, err = await run_cmd(cmd)
    if code != 0:
        raise RuntimeError(f"Video rendering failed: {err[-1500:]}")


# -----------------------------------------------------------------------------
# Image / preview helpers
# -----------------------------------------------------------------------------

def _load_font(size: int = 24) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    try:
        return ImageFont.truetype("DejaVuSans.ttf", size=size)
    except Exception:
        return ImageFont.load_default()


def _resize_watermark(wm: Image.Image, target_w: int) -> Image.Image:
    target_w = max(1, target_w)
    ratio = target_w / wm.width
    target_h = max(1, int(wm.height * ratio))
    return wm.resize((target_w, target_h), Image.Resampling.LANCZOS)


def _overlay_watermark(
    frame: Image.Image,
    watermark: Image.Image,
    percent: int,
    margin: int = WATERMARK_MARGIN,
) -> Image.Image:
    base = frame.convert("RGBA")
    wm = watermark.convert("RGBA")
    target_w = max(64, int(base.width * percent / 100))
    target_w = min(target_w, max(64, base.width - 2 * margin))
    wm = _resize_watermark(wm, target_w)
    x = base.width - wm.width - margin
    y = base.height - wm.height - margin
    overlay = Image.new("RGBA", base.size, (0, 0, 0, 0))
    overlay.paste(wm, (x, y), wm)
    out = Image.alpha_composite(base, overlay)
    return out.convert("RGB")


def build_preview_collage(
    frame_path: str,
    watermark_bytes: bytes,
    out_path: str,
    title: str = "Choose watermark size for this video",
) -> tuple[int, int]:
    frame = Image.open(frame_path).convert("RGB")
    watermark = Image.open(BytesIO(watermark_bytes)).convert("RGBA")
    base_w, base_h = frame.size

    tiles: list[Image.Image] = []
    labels = ["Small", "Medium", "Large", "XL"]
    for idx, percent in enumerate(PREVIEW_SIZES):
        tile = ImageOps.fit(frame, PREVIEW_TILE_SIZE, method=Image.Resampling.LANCZOS)
        tile = _overlay_watermark(tile, watermark, percent=percent)
        draw = ImageDraw.Draw(tile)
        font = _load_font(22)
        label = f"{labels[idx]}  ({percent}%)"
        pad = 12
        text_bbox = draw.textbbox((0, 0), label, font=font)
        text_w = text_bbox[2] - text_bbox[0]
        text_h = text_bbox[3] - text_bbox[1]
        rect_h = text_h + pad * 2
        rect_y0 = tile.height - rect_h
        draw.rectangle((0, rect_y0, tile.width, tile.height), fill=(0, 0, 0))
        draw.text(
            ((tile.width - text_w) // 2, rect_y0 + pad),
            label,
            font=font,
            fill=(255, 255, 255),
        )
        tiles.append(tile)

    gap = 18
    canvas_w = PREVIEW_TILE_SIZE[0] * 2 + gap * 3
    canvas_h = PREVIEW_TILE_SIZE[1] * 2 + gap * 3
    canvas = Image.new("RGB", (canvas_w, canvas_h), (245, 245, 245))

    positions = [
        (gap, gap),
        (gap * 2 + PREVIEW_TILE_SIZE[0], gap),
        (gap, gap * 2 + PREVIEW_TILE_SIZE[1]),
        (gap * 2 + PREVIEW_TILE_SIZE[0], gap * 2 + PREVIEW_TILE_SIZE[1]),
    ]
    for tile, (x, y) in zip(tiles, positions):
        canvas.paste(tile, (x, y))

    draw = ImageDraw.Draw(canvas)
    font = _load_font(26)
    bbox = draw.textbbox((0, 0), title, font=font)
    tw = bbox[2] - bbox[0]
    draw.text(((canvas_w - tw) // 2, 4), title, font=font, fill=(40, 40, 40))

    canvas.save(out_path, quality=95)
    return base_w, base_h


def resize_watermark_for_video(
    watermark_bytes: bytes,
    selected_percent: int,
    frame_width: int,
    out_path: str,
) -> None:
    wm = Image.open(BytesIO(watermark_bytes)).convert("RGBA")
    target_w = max(64, int(min(frame_width, OUTPUT_MAX_WIDTH) * selected_percent / 100))
    target_w = min(target_w, max(64, min(frame_width, OUTPUT_MAX_WIDTH) - 40))
    resized = _resize_watermark(wm, target_w)
    resized.save(out_path, format="PNG")


# -----------------------------------------------------------------------------
# Database helpers
# -----------------------------------------------------------------------------

async def init_db(pool: asyncpg.Pool) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                watermark_png BYTEA,
                watermark_filename TEXT,
                join_links TEXT NOT NULL DEFAULT '[]',
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        await conn.execute(
            """
            INSERT INTO settings (id)
            VALUES (1)
            ON CONFLICT (id) DO NOTHING;
            """
        )
        try:
            await conn.execute("ALTER TABLE settings ADD COLUMN IF NOT EXISTS join_links TEXT NOT NULL DEFAULT '[]';")
        except Exception:
            pass
        
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS videos (
                id BIGSERIAL PRIMARY KEY,
                token TEXT UNIQUE NOT NULL,
                channel_message_id BIGINT NOT NULL,
                original_filename TEXT,
                media_type TEXT NOT NULL DEFAULT 'video',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_data (
                user_id BIGINT PRIMARY KEY,
                api_id INTEGER,
                api_hash TEXT,
                session_string TEXT,
                target_chat_id BIGINT,
                target_chat_title TEXT,
                auto_video_duration INTEGER DEFAULT 0,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )
        for col, col_type in [
            ("reply_msg_id", "INTEGER DEFAULT NULL"),
            ("reply_active", "BOOLEAN DEFAULT FALSE"),
            ("reply_chat_id", "BIGINT DEFAULT NULL"),
            ("auto_video_enabled", "BOOLEAN DEFAULT FALSE"),
            ("auto_video_path", "TEXT DEFAULT NULL"),
            ("media_type", "TEXT NOT NULL DEFAULT 'video'"),
        ]:
            try:
                await conn.execute(f"ALTER TABLE user_data ADD COLUMN IF NOT EXISTS {col} {col_type};")
            except Exception:
                pass


async def get_user_data(user_id: int) -> Optional[dict]:
    if db_pool is None:
        return None
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow('SELECT * FROM user_data WHERE user_id = $1', user_id)
        return dict(row) if row else None


async def save_user_data(user_id: int, **kwargs) -> None:
    if db_pool is None:
        return
    if not kwargs:
        return
    async with db_pool.acquire() as conn:
        existing = await conn.fetchrow("SELECT 1 FROM user_data WHERE user_id = $1", user_id)
        if existing:
            fields = []
            values = []
            for key, val in kwargs.items():
                fields.append(f"{key} = ${len(values) + 1}")
                values.append(val)
            values.append(user_id)
            query = f"UPDATE user_data SET {', '.join(fields)} WHERE user_id = ${len(values)}"
            await conn.execute(query, *values)
        else:
            columns = list(kwargs.keys()) + ['user_id']
            placeholders = ', '.join([f"${i+1}" for i in range(len(columns))])
            values = list(kwargs.values()) + [user_id]
            query = f"INSERT INTO user_data ({', '.join(columns)}) VALUES ({placeholders})"
            await conn.execute(query, *values)


async def get_settings(pool: asyncpg.Pool) -> dict[str, Any]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT watermark_png, watermark_filename, updated_at FROM settings WHERE id = 1"
        )
        return dict(row) if row else {}


async def save_watermark(pool: asyncpg.Pool, png_bytes: bytes, filename: str) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE settings
            SET watermark_png = $1,
                watermark_filename = $2,
                updated_at = NOW()
            WHERE id = 1
            """,
            png_bytes,
            filename,
        )


async def get_watermark(pool: asyncpg.Pool) -> tuple[bytes, str] | None:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT watermark_png, watermark_filename FROM settings WHERE id = 1"
        )
        if not row or not row["watermark_png"]:
            return None
        return bytes(row["watermark_png"]), row["watermark_filename"] or "watermark.png"


async def save_video_mapping(
    pool: asyncpg.Pool,
    token: str,
    channel_message_id: int,
    original_filename: str | None,
    media_type: str = "video",
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO videos (token, channel_message_id, original_filename, media_type)
            VALUES ($1, $2, $3, $4)
            """,
            token,
            channel_message_id,
            original_filename,
            media_type,
        )


async def get_video_mapping(pool: asyncpg.Pool, token: str) -> dict[str, Any] | None:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT token, channel_message_id, original_filename, media_type FROM videos WHERE token = $1",
            token,
        )
        return dict(row) if row else None


def _guess_image_suffix(filename: str | None, mime_type: str | None = None) -> str:
    if filename:
        suffix = Path(filename).suffix.lower()
        if suffix in {".jpg", ".jpeg", ".png", ".webp"}:
            return suffix
    mime_type = (mime_type or "").lower()
    if "png" in mime_type:
        return ".png"
    if "webp" in mime_type:
        return ".webp"
    if "jpeg" in mime_type or "jpg" in mime_type:
        return ".jpg"
    return ".jpg"


def _media_kind(message: Message) -> str | None:
    if message.video:
        return "video"
    if message.document and (message.document.mime_type or "").startswith("video/"):
        return "video"
    if message.photo:
        return "photo"
    if message.document and (message.document.mime_type or "").startswith("image/"):
        return "image_document"
    return None


def _media_noun(media_type: str) -> str:
    return "عکس" if media_type != "video" else "ویدیو"


def _media_caption_label(media_type: str) -> str:
    return "photo" if media_type != "video" else "video"


async def download_admin_image(message: Message) -> tuple[str, str, int]:
    if message.photo:
        photo = message.photo[-1]
        file_id = photo.file_id
        filename = "photo.jpg"
        file_size = photo.file_size or 0
        suffix = ".jpg"
    elif message.document:
        doc = message.document
        file_id = doc.file_id
        filename = doc.file_name or "image.jpg"
        file_size = doc.file_size or 0
        suffix = _guess_image_suffix(filename, doc.mime_type)
    else:
        raise ValueError("این پیام شامل عکس نیست.")

    logger.info(f"===== STARTING IMAGE DOWNLOAD =====")
    logger.info(f"File: {filename}, Size: {file_size} bytes, ID: {file_id}")

    file = await message.bot.get_file(file_id)
    file_path = file.file_path
    if not file_path:
        raise ValueError("آدرس فایل دریافت نشد.")

    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp_path = tmp.name

    try:
        await message.bot.download_file(file_path, destination=tmp_path)
        if not os.path.exists(tmp_path) or os.path.getsize(tmp_path) < 100:
            raise ValueError("دانلود عکس ناموفق بود.")
        return tmp_path, filename, file_size
    except Exception:
        safe_unlink(tmp_path)
        raise


def render_image_with_watermark(
    input_path: str,
    watermark_path: str,
    output_path: str,
) -> None:
    frame = Image.open(input_path).convert("RGB")
    watermark = Image.open(watermark_path).convert("RGBA")
    out = _overlay_watermark(frame, watermark, percent=100)
    out.save(output_path, quality=95)


# -----------------------------------------------------------------------------
# UI helpers (join links)
# -----------------------------------------------------------------------------

async def get_join_links(pool: asyncpg.Pool) -> list[dict[str, Any]]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT join_links FROM settings WHERE id = 1")
        if not row:
            return []
        raw = row["join_links"] or "[]"
        try:
            data = json.loads(raw)
            if not isinstance(data, list):
                return []
            cleaned: list[dict[str, Any]] = []
            for item in data:
                if not isinstance(item, dict):
                    continue
                if item.get("chat_id") is None or not item.get("url"):
                    continue
                cleaned.append(
                    {
                        "chat_id": int(item["chat_id"]),
                        "url": str(item["url"]),
                        "title": str(item.get("title") or ""),
                        "label": str(item.get("label") or ""),
                    }
                )
            return cleaned
        except Exception:
            return []


async def save_join_links(pool: asyncpg.Pool, entries: list[dict[str, Any]]) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            UPDATE settings
            SET join_links = $1,
                updated_at = NOW()
            WHERE id = 1
            """,
            json.dumps(entries, ensure_ascii=False),
        )


def renumber_join_links(entries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for idx, item in enumerate(entries, start=1):
        normalized.append(
            {
                "chat_id": int(item["chat_id"]),
                "url": str(item["url"]),
                "title": str(item.get("title") or f"کانال {idx}"),
                "label": f"💧عضویت {idx}",
            }
        )
    return normalized


def build_join_keyboard(entries: list[dict[str, Any]], payload: str = "") -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for item in entries:
        kb.button(text=item["label"], url=item["url"])
    kb.button(text="✅ عضو شدم", callback_data=f"join:check:{payload or '_'}")
    kb.adjust(1)
    return kb.as_markup()


def build_join_remove_keyboard(entries: list[dict[str, Any]]) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for idx, item in enumerate(entries):
        kb.button(text=f"❌ {item['label']}", callback_data=f"join:remove:{idx}")
    if entries:
        kb.button(text="🗑 حذف همه", callback_data="join:clear")
    kb.button(text="↩️ بازگشت", callback_data="join:back")
    kb.adjust(1)
    return kb.as_markup()


def format_join_list(entries: list[dict[str, Any]]) -> str:
    if not entries:
        return "فعلاً هیچ جوین اجباری‌ای تنظیم نشده است."
    lines = ["<b>فهرست جوین اجباری</b>", ""]
    for item in entries:
        title = item.get("title") or item["label"]
        lines.append(f"• {item['label']} — <a href=\"{item['url']}\">{title}</a>")
    return "\n".join(lines)


async def resolve_join_target(bot: Bot, raw_value: str) -> dict[str, Any]:
    raw = raw_value.strip()
    if not raw:
        raise ValueError("ورودی خالی است.")
    if raw.startswith("@"):
        ref: Any = raw
    elif raw.startswith("https://t.me/") or raw.startswith("http://t.me/"):
        tail = raw.split("t.me/", 1)[1].split("?", 1)[0].strip("/")
        if tail.startswith("+") or "joinchat" in tail:
            raise ValueError(
                "لینک‌های خصوصیِ دعوت برای جوین اجباریِ قابل‌بررسی مناسب نیستند. "
                "لطفاً یوزرنیم عمومی کانال را با @ یا لینک public t.me ارسال کنید."
            )
        ref = "@" + tail.lstrip("@")
    elif raw.lstrip("-").isdigit():
        ref = int(raw)
    else:
        raise ValueError("فرمت لینک نامعتبر است. از @username یا لینک public t.me استفاده کنید.")
    chat = await bot.get_chat(ref)
    username = getattr(chat, "username", None)
    title = getattr(chat, "title", None) or getattr(chat, "full_name", None) or "کانال"
    if not username:
        if isinstance(ref, int):
            raise ValueError(
                "این چت یوزرنیم عمومی ندارد. برای جوین اجباری، لطفاً کانال public با @username اضافه کنید."
            )
        raise ValueError("برای ساخت دکمه عضویت، چت باید public و دارای یوزرنیم باشد.")
    return {"chat_id": int(chat.id), "url": f"https://t.me/{username}", "title": title, "label": ""}


async def get_missing_joins(bot: Bot, user_id: int) -> list[dict[str, Any]]:
    if user_id == ADMIN_ID:
        return []
    entries = renumber_join_links(await get_join_links(db_pool))
    if not entries:
        return []
    missing: list[dict[str, Any]] = []
    for item in entries:
        try:
            member = await bot.get_chat_member(item["chat_id"], user_id)
            status = getattr(member, "status", "")
            if status not in {"creator", "administrator", "member", "restricted"}:
                missing.append(item)
        except Exception:
            missing.append(item)
    return missing


async def send_join_required_prompt(message: Message, payload: str = "") -> None:
    entries = renumber_join_links(await get_join_links(db_pool))
    if not entries:
        return

    missing = await get_missing_joins(message.bot, message.from_user.id)
    if missing:
        entries = missing

    text = (
        "⚠️ <b>برای استفاده از ربات، ابتدا در کانال‌های زیر عضو شوید:</b>\n\n"
        f"{format_join_list(entries)}\n\n"
        "بعد از عضویت روی دکمه «✅ عضو شدم» بزنید."
    )
    await message.answer(text, reply_markup=build_join_keyboard(entries, payload))


async def deliver_user_media(bot: Bot, chat_id: int, token: str) -> None:
    mapping = await get_video_mapping(db_pool, token)
    if not mapping:
        await bot.send_message(chat_id, "این لینک نامعتبر است یا فایل دیگر در دسترس نیست.")
        return

    media_type = str(mapping.get("media_type") or "video")
    noun = _media_noun(media_type)

    warning = await bot.send_message(
        chat_id,
        f"⚠️ {noun} را همین حالا ذخیره کنید.\n"
        "این پیام تا ۱۰ ثانیه دیگر به صورت خودکار حذف خواهد شد."
        "این پیام تا ۱۰ ثانیه دیگر به صورت خودکار حذف خواهد شد."
    )
    try:
        copied = await bot.copy_message(
            chat_id=chat_id,
            from_chat_id=STORAGE_CHANNEL,
            message_id=mapping["channel_message_id"],
        )
        copied_message_id = copied.message_id if hasattr(copied, "message_id") else copied
    except TelegramBadRequest:
        await warning.edit_text(f"{noun} در کانال ذخیره‌سازی در دسترس نیست.")
        return
    except TelegramForbiddenError:
        await warning.edit_text("امکان ارسال پیام به این کاربر وجود ندارد.")
        return
    asyncio.create_task(
        delete_later(bot, chat_id, [warning.message_id, copied_message_id], 10)
    )


async def deliver_user_video(bot: Bot, chat_id: int, token: str) -> None:
    await deliver_user_media(bot, chat_id, token)


async def handle_user_start_flow(message: Message, payload: str) -> None:
    missing = await get_missing_joins(message.bot, message.from_user.id)
    if missing:
        await send_join_required_prompt(message, payload)
        return
    if payload:
        await deliver_user_media(message.bot, message.chat.id, payload)
    else:
        await message.answer("لطفاً لینک ربات را از ادمین دریافت کنید.")


# -----------------------------------------------------------------------------
# Admin UI (با دکمه لاگین)
# -----------------------------------------------------------------------------

def admin_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🔐 لاگین", callback_data="login")
    kb.button(text="⚙️ Set watermark", callback_data="wm:set")
    kb.button(text="ℹ️ Current settings", callback_data="wm:info")
    kb.button(text="➕ تنظیم جوین اجباری", callback_data="join:add")
    kb.button(text="➖ حذف جوین اجباری", callback_data="join:remove")
    kb.button(text="📋 لیست جوین اجباری", callback_data="join:list")
    kb.adjust(1)
    return kb.as_markup()


def cancel_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="✖️ Cancel", callback_data="wm:cancel")
    return kb.as_markup()


def size_keyboard(job_id: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    labels = ["Small", "Medium", "Large", "XL"]
    for idx, label in enumerate(labels):
        kb.button(text=f"✅ {label}", callback_data=f"sz:{job_id}:{idx}")
    kb.adjust(2, 2)
    return kb.as_markup()


# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------

def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


def get_bot_username() -> str:
    if BOT_USERNAME:
        return BOT_USERNAME.lstrip("@")
    raise RuntimeError("BOT_USERNAME is not known yet.")


def build_start_link(token: str) -> str:
    return f"https://t.me/{get_bot_username()}?start={token}"


def make_token() -> str:
    return f"v_{secrets.token_hex(8)}"


def safe_unlink(path: str | None) -> None:
    if not path:
        return
    try:
        Path(path).unlink(missing_ok=True)
    except Exception:
        pass


async def delete_later(bot: Bot, chat_id: int, message_ids: list[int], delay: int = 10) -> None:
    await asyncio.sleep(delay)
    for mid in message_ids:
        try:
            await bot.delete_message(chat_id, mid)
        except Exception:
            pass


async def ensure_bot_username(bot: Bot) -> str:
    global BOT_USERNAME
    if BOT_USERNAME:
        return BOT_USERNAME
    me = await bot.get_me()
    BOT_USERNAME = me.username
    return BOT_USERNAME


# ========== لاگین با پشتیبانی از رمز دو مرحله‌ای و مدیریت اتصال ==========
@router.callback_query(F.data == "login")
async def cmd_login(call: CallbackQuery, state: FSMContext) -> None:
    if not call.from_user or not is_admin(call.from_user.id):
        await call.answer("Not allowed", show_alert=True)
        return
    await state.set_state(LoginState.waiting_for_api_id)
    await call.message.answer("🔑 لطفاً `api_id` خود را وارد کنید:\n(از my.telegram.org)")
    await call.answer()


@router.message(LoginState.waiting_for_api_id)
async def login_api_id(message: Message, state: FSMContext) -> None:
    try:
        api_id = int(message.text.strip())
        await state.update_data(api_id=api_id)
        await state.set_state(LoginState.waiting_for_api_hash)
        await message.answer("🔑 لطفاً `api_hash` خود را وارد کنید:")
    except ValueError:
        await message.answer("❌ api_id باید یک عدد باشد. دوباره وارد کنید:")


@router.message(LoginState.waiting_for_api_hash)
async def login_api_hash(message: Message, state: FSMContext) -> None:
    api_hash = message.text.strip()
    await state.update_data(api_hash=api_hash)
    await state.set_state(LoginState.waiting_for_phone)
    await message.answer("📞 لطفاً شماره تلفن خود را با کد کشور وارد کنید:\nمثال: `+989123456789`")


@router.message(LoginState.waiting_for_phone)
async def login_phone(message: Message, state: FSMContext) -> None:
    phone = message.text.strip()
    await state.update_data(phone=phone)
    data = await state.get_data()
    client = TelegramClient(StringSession(), data['api_id'], data['api_hash'])
    await client.connect()
    try:
        sent_code = await client.send_code_request(phone)
        await state.update_data(temp_client=client, phone_code_hash=getattr(sent_code, 'phone_code_hash', None))
        await state.set_state(LoginState.waiting_for_code)
        await message.answer("✅ کد تأیید به تلگرام شما ارسال شد.\nلطفاً کد ۵ رقمی را وارد کنید (فقط اعداد):")
    except Exception as e:
        await client.disconnect()
        await message.answer(f"❌ خطا در ارسال کد: {e}")


@router.message(LoginState.waiting_for_code)
async def login_code(message: Message, state: FSMContext) -> None:
    code = re.sub(r"\D", "", (message.text or "")).strip()
    data = await state.get_data()
    client = data.get('temp_client')
    
    if not client:
        await message.answer("❌ نشست منقضی شد. دوباره /start را بزنید.")
        await state.clear()
        return
    
    if len(code) < 5:
        await message.answer("❌ کد نامعتبر است. فقط کد ۵ رقمی را بفرستید.")
        return
    
    # اطمینان از اتصال کلاینت (اگر قطع شده، دوباره وصل کن)
    if not client.is_connected():
        try:
            await client.connect()
        except Exception as e:
            await message.answer(f"❌ خطا در اتصال مجدد: {e}")
            return
    
    try:
        await client.sign_in(
            phone=data['phone'],
            code=code,
            phone_code_hash=data.get('phone_code_hash')
        )
        session_string = client.session.save()
        await client.disconnect()
        
        user_id = message.from_user.id
        await save_user_data(
            user_id,
            api_id=data['api_id'],
            api_hash=data['api_hash'],
            session_string=session_string
        )
        await state.clear()
        await message.answer("✅ لاگین موفقیت‌آمیز بود.\nاکنون می‌توانید از قابلیت لینک استفاده کنید.")
        await cmd_start(message, state)
        
    except SessionPasswordNeededError:
        await state.set_state(LoginState.waiting_for_password)
        await message.answer("🔐 حساب شما رمز دو مرحله‌ای دارد.\nلطفاً رمز خود را وارد کنید:")
    except PhoneCodeInvalidError:
        await message.answer("❌ کد تأیید اشتباه است. دوباره امتحان کنید.")
    except PhoneCodeExpiredError:
        await message.answer("❌ کد منقضی شده است. دوباره درخواست کد بدهید.")
    except Exception as e:
        if "disconnected" in str(e).lower():
            await message.answer("❌ اتصال قطع شد. لطفاً دوباره تلاش کنید (دوباره /start و لاگین).")
        else:
            await message.answer(f"❌ خطا: {e}")


@router.message(LoginState.waiting_for_password)
async def login_password(message: Message, state: FSMContext) -> None:
    password = message.text.strip()
    data = await state.get_data()
    client = data.get('temp_client')
    
    if not client:
        await message.answer("❌ نشست منقضی شد. دوباره /start را بزنید.")
        await state.clear()
        return
    
    if not client.is_connected():
        try:
            await client.connect()
        except Exception as e:
            await message.answer(f"❌ خطا در اتصال مجدد: {e}")
            return
    
    try:
        await client.sign_in(password=password)
        session_string = client.session.save()
        await client.disconnect()
        user_id = message.from_user.id
        await save_user_data(
            user_id,
            api_id=data['api_id'],
            api_hash=data['api_hash'],
            session_string=session_string
        )
        await state.clear()
        await message.answer("✅ لاگین موفقیت‌آمیز بود.\nاکنون می‌توانید از قابلیت لینک استفاده کنید.")
        await cmd_start(message, state)
    except Exception as e:
        if "disconnected" in str(e).lower():
            await message.answer("❌ اتصال قطع شد. لطفاً دوباره تلاش کنید.")
        else:
            await message.answer(f"❌ رمز اشتباه است یا خطا: {e}")


# -----------------------------------------------------------------------------
# Telethon download helper with detailed logging
# -----------------------------------------------------------------------------

async def download_file_with_telethon_from_message(
    source_message: Message,
    save_path: str,
    storage_channel: int,
) -> tuple[bool, str]:
    if telethon_client is None:
        return False, "Telethon client not available"

    copied_message_id: int | None = None
    try:
        copied = await source_message.bot.copy_message(
            chat_id=storage_channel,
            from_chat_id=source_message.chat.id,
            message_id=source_message.message_id,
        )
        copied_message_id = copied.message_id if hasattr(copied, "message_id") else int(copied)

        tele_msg = await telethon_client.get_messages(storage_channel, ids=copied_message_id)
        if not tele_msg:
            return False, "پیام کپی‌شده در کانال ذخیره‌سازی پیدا نشد"

        await telethon_client.download_media(tele_msg, file=save_path)

        if not os.path.exists(save_path) or os.path.getsize(save_path) < 1000:
            return False, "دانلود ناموفق بود"

        return True, ""
    except RPCError as e:
        logger.error(f"Telethon RPCError: {e}")
        if "USER_NOT_PARTICIPANT" in str(e):
            return False, "اکانت یوزربات در کانال ذخیره‌سازی عضو نیست. لطفاً آن را به کانال اضافه کنید."
        return False, f"RPC error: {e}"
    except Exception as e:
        logger.error(f"Telethon unexpected error: {e}")
        return False, f"خطا: {e}"
    finally:
        if copied_message_id is not None:
            try:
                await telethon_client.delete_messages(storage_channel, copied_message_id)
            except Exception:
                pass


# -----------------------------------------------------------------------------
# Video download functions
# -----------------------------------------------------------------------------

def _is_video_message(message: Message) -> bool:
    return _media_kind(message) == "video"


def _is_image_message(message: Message) -> bool:
    return _media_kind(message) in {"photo", "image_document"}


async def download_admin_video(message: Message) -> tuple[str, str, int]:
    if message.video:
        file_id = message.video.file_id
        filename = message.video.file_name or "video.mp4"
        file_size = message.video.file_size or 0
    else:
        file_id = message.document.file_id
        filename = message.document.file_name or "video.mp4"
        file_size = message.document.file_size or 0

    logger.info(f"===== STARTING DOWNLOAD =====")
    logger.info(f"File: {filename}, Size: {file_size} bytes, ID: {file_id}")

    if file_size <= MAX_VIDEO_BYTES:
        logger.info(f"Method 1: aiohttp download (small file: {file_size} <= {MAX_VIDEO_BYTES})")
        try:
            file = await message.bot.get_file(file_id)
            file_path = file.file_path
            if not file_path:
                raise ValueError("آدرس فایل دریافت نشد.")
            
            suffix = Path(filename).suffix or ".mp4"
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                tmp_path = tmp.name
            
            url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"
            timeout = aiohttp.ClientTimeout(total=300, connect=30, sock_read=120)
            
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as response:
                    if response.status != 200:
                        error_text = await response.text()
                        raise ValueError(f"دانلود ناموفق: {error_text[:200]}")
                    with open(tmp_path, "wb") as f:
                        async for chunk in response.content.iter_chunked(8192):
                            f.write(chunk)
            
            if os.path.getsize(tmp_path) >= 1000:
                logger.info("aiohttp download successful.")
                return tmp_path, filename, file_size
            safe_unlink(tmp_path)
            logger.warning("aiohttp download failed: file too small.")
            # Fall through to Telethon route below.
        except Exception as e:
            logger.error(f"aiohttp download error: {e}")
            # Fall through to Telethon route below.

    if telethon_client is not None:
        logger.info("Method 2: Telethon download via storage channel copy")
        suffix = Path(filename).suffix or ".mp4"
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            tmp_path = tmp.name

        logger.info(f"Downloading with Telethon to {tmp_path}...")
        success, error_msg = await download_file_with_telethon_from_message(message, tmp_path, STORAGE_CHANNEL)
        
        if success:
            logger.info("Telethon download successful.")
            return tmp_path, filename, file_size
        else:
            safe_unlink(tmp_path)
            logger.error(f"Telethon download failed: {error_msg}")
            
            if "دسترسی ندارد" in error_msg or "عضو نیست" in error_msg or "دسترسی نوشتن ندارد" in error_msg:
                raise ValueError(
                    f"❌ {error_msg}\n\n"
                    "⚠️ راه‌حل: اکانت یوزربات را به کانال ذخیره‌سازی اضافه کنید و مطمئن شوید ربات هم اجازه نوشتن در آن را دارد.\n"
                    f"کانال ID: {STORAGE_CHANNEL}\n"
                    "سپس دوباره امتحان کنید."
                )
            raise ValueError(f"❌ خطا در دانلود با Telethon: {error_msg}")
    else:
        logger.error("Telethon client is not available")
        raise ValueError(
            "⚠️ Telethon در دسترس نیست.\n"
            "لطفاً API_ID, API_HASH, USER_SESSION_STRING را تنظیم کنید."
        )


# -----------------------------------------------------------------------------
# Admin handlers
# -----------------------------------------------------------------------------

@router.message(F.chat.type.in_({"group", "supergroup", "channel"}))
async def ignore_groups(message: Message) -> None:
    return


@router.message(CommandStart(), F.chat.type == "private")
async def cmd_start(message: Message, state: FSMContext) -> None:
    payload = ""
    if message.text and " " in message.text:
        payload = message.text.split(" ", 1)[1].strip()
    if is_admin(message.from_user.id):
        if payload:
            await message.answer("پنل ادمین", reply_markup=admin_keyboard())
            return
        
        data = await get_user_data(message.from_user.id)
        is_logged_in = data and data.get('session_string')
        login_status = "✅ لاگین هستید" if is_logged_in else "❌ لاگین نیستید"
        try:
            settings = await get_settings(db_pool)
            has_wm = bool(settings.get("watermark_png"))
        except Exception:
            has_wm = False
            
        text = (
            "👋 <b>پنل ادمین</b>\n\n"
            f"🟢 واترمارک: <b>{'ذخیره شده' if has_wm else 'تنظیم نشده'}</b>\n"
            f"🔐 وضعیت لاگین: <b>{login_status}</b>\n\n"
            "برای تنظیم واترمارک، جوین اجباری و پردازش ویدیو از دکمه‌های زیر استفاده کنید.\n"
            "برای استفاده از قابلیت لینک، ابتدا لاگین کنید."
        )
        await message.answer(text, reply_markup=admin_keyboard())
        return
    
    missing = await get_missing_joins(message.bot, message.from_user.id)
    if missing:
        await send_join_required_prompt(message, payload)
        return
    if not payload:
        await message.answer("لطفاً لینک ربات را از ادمین دریافت کنید.")
        return
    await deliver_user_media(message.bot, message.chat.id, payload)


# ========== هندلر لینک تلگرام ==========
@router.message(StateFilter(None), F.text & F.chat.type == "private")
async def handle_telegram_link(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        return

    current_state = await state.get_state()
    if current_state is not None:
        return

    text = message.text.strip()
    pattern = r'https?://t\.me/(?:c/)?([a-zA-Z][\w]+|\d+)/(\d+)'
    match = re.search(pattern, text)
    if not match:
        return

    chat_part = match.group(1)
    msg_id = int(match.group(2))

    await message.answer("⏳ در حال دریافت ویدیو از لینک...")

    try:
        data = await get_user_data(message.from_user.id)
        if not data or not data.get("session_string"):
            await message.answer(
                "❌ **ابتدا لاگین کنید.**\n\n"
                "برای استفاده از قابلیت لینک، باید در ربات لاگین کنید:\n"
                "1️⃣ روی /start کلیک کنید.\n"
                "2️⃣ روی دکمه «لاگین» بزنید.\n"
                "3️⃣ api_id و api_hash و شماره تلفن خود را وارد کنید.\n"
                "4️⃣ کد تأیید را وارد کنید.\n\n"
                "پس از لاگین موفق، دوباره لینک را ارسال کنید.",
                parse_mode="Markdown"
            )
            return

        client = TelegramClient(StringSession(data["session_string"]), data["api_id"], data["api_hash"])
        await client.connect()
        try:
            if chat_part.isdigit():
                chat_id = int(f"-100{chat_part}")
                entity = await client.get_entity(chat_id)
            else:
                entity = await client.get_entity(chat_part)

            tele_msg = await client.get_messages(entity, ids=msg_id)
            if not tele_msg:
                await message.answer("❌ پیام مورد نظر پیدا نشد.")
                return

            file_name = "video_from_link.mp4"
            if tele_msg.video:
                file_name = getattr(tele_msg.video, "file_name", "video_from_link.mp4")
            elif tele_msg.document and tele_msg.document.mime_type and tele_msg.document.mime_type.startswith("video/"):
                file_name = getattr(tele_msg.document, "file_name", "video_from_link.mp4")
            else:
                await message.answer("❌ پیام مورد نظر شامل ویدیو یا فایل ویدیویی نیست.")
                return

            suffix = Path(file_name).suffix or ".mp4"
            with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
                tmp_path = tmp.name

            await client.download_media(tele_msg, file=tmp_path)
            if not os.path.exists(tmp_path) or os.path.getsize(tmp_path) < 1000:
                await message.answer("❌ دانلود ویدیو از لینک ناموفق بود.")
                return

        finally:
            await client.disconnect()

        settings = await get_watermark(db_pool)
        if not settings:
            await message.answer("Please set the watermark first using the admin panel.")
            return

        await message.answer("✅ ویدیو با موفقیت دریافت شد. در حال ساخت Preview...")

        job_id = secrets.token_hex(4)
        temp_dir = tempfile.mkdtemp(prefix=f"wmjob_{job_id}_")
        frame_path = str(Path(temp_dir) / "frame.jpg")
        collage_path = str(Path(temp_dir) / "collage.jpg")

        await extract_frame(tmp_path, frame_path, at_seconds=1.0)
        with Image.open(frame_path) as im:
            frame_width, frame_height = im.size

        def _build() -> tuple[int, int]:
            return build_preview_collage(frame_path, settings[0], collage_path)

        base_w, base_h = await asyncio.to_thread(_build)

        job = Job(
            job_id=job_id,
            admin_id=message.from_user.id,
            source_path=tmp_path,
            source_filename=file_name,
            frame_path=frame_path,
            preview_path=collage_path,
            frame_width=base_w,
            frame_height=base_h,
        )
        JOBS[job_id] = job

        caption = (
            f"🖼 <b>Choose the watermark size for this {media_label}</b>\n\n"
            "The watermark position is fixed to <b>bottom-right</b>.\n"
            f"The chosen size will apply only to this {media_label}."
        )
        await message.answer_photo(
            FSInputFile(collage_path),
            caption=caption,
            reply_markup=size_keyboard(job_id),
        )

    except Exception as e:
        await message.answer(f"❌ خطا در پردازش لینک: {str(e)}")


# -----------------------------------------------------------------------------
# Callback handlers (join, watermark, etc.)
# -----------------------------------------------------------------------------

@router.callback_query(F.data.startswith("join:check:"))
async def cb_join_check(call: CallbackQuery) -> None:
    payload = ""
    try:
        payload = call.data.split(":", 2)[2]
    except Exception:
        payload = "_"
    if not call.from_user:
        await call.answer("Not allowed", show_alert=True)
        return
    missing = await get_missing_joins(call.bot, call.from_user.id)
    if missing:
        await call.answer("هنوز همه عضویت‌ها کامل نشده است.", show_alert=True)
        try:
            await call.message.edit_reply_markup(
                reply_markup=build_join_keyboard(
                    missing,
                    payload if payload != "_" else ""
                )
            )
        except Exception:
            pass
        return
    await call.answer("عضویت تأیید شد")
    try:
        await call.message.delete()
    except Exception:
        pass
    if payload and payload != "_":
        await deliver_user_media(call.bot, call.message.chat.id, payload)
    else:
        await call.message.answer("✅ عضویت شما تأیید شد. حالا می‌توانید دوباره از ربات استفاده کنید.")


@router.callback_query(F.data == "join:add")
async def cb_join_add(call: CallbackQuery, state: FSMContext) -> None:
    if not call.from_user or not is_admin(call.from_user.id):
        await call.answer("Not allowed", show_alert=True)
        return
    await state.set_state(JoinState.waiting_for_links)
    await call.message.answer(
        "لطفاً لینک‌های جوین اجباری را ارسال کنید.\n"
        "هر لینک در یک خط جداگانه باشد.\n"
        "از @username یا لینک public t.me استفاده کنید.\n\n"
        "بعد از ارسال، ربات باید پیام تایید بدهد."
    )
    await call.answer()


@router.callback_query(F.data == "join:list")
async def cb_join_list(call: CallbackQuery) -> None:
    if not call.from_user or not is_admin(call.from_user.id):
        await call.answer("Not allowed", show_alert=True)
        return
    entries = renumber_join_links(await get_join_links(db_pool))
    text = format_join_list(entries)
    if not entries:
        await call.message.answer(text)
    else:
        await call.message.answer(text, reply_markup=build_join_remove_keyboard(entries))
    await call.answer()


@router.callback_query(F.data == "join:remove")
async def cb_join_remove(call: CallbackQuery) -> None:
    if not call.from_user or not is_admin(call.from_user.id):
        await call.answer("Not allowed", show_alert=True)
        return
    entries = renumber_join_links(await get_join_links(db_pool))
    if not entries:
        await call.message.answer("فعلاً هیچ جوین اجباری‌ای ثبت نشده است.")
        await call.answer()
        return
    await call.message.answer(
        "برای حذف، روی دکمه موردنظر بزنید:",
        reply_markup=build_join_remove_keyboard(entries)
    )
    await call.answer()


@router.callback_query(F.data == "join:clear")
async def cb_join_clear(call: CallbackQuery) -> None:
    if not call.from_user or not is_admin(call.from_user.id):
        await call.answer("Not allowed", show_alert=True)
        return
    await save_join_links(db_pool, [])
    await call.message.answer("✅ همه جوین‌های اجباری حذف شدند.")
    await call.answer()


@router.callback_query(F.data.startswith("join:remove:"))
async def cb_join_remove_item(call: CallbackQuery) -> None:
    if not call.from_user or not is_admin(call.from_user.id):
        await call.answer("Not allowed", show_alert=True)
        return
    try:
        idx = int(call.data.split(":")[-1])
    except Exception:
        await call.answer("Invalid selection", show_alert=True)
        return
    entries = renumber_join_links(await get_join_links(db_pool))
    if idx < 0 or idx >= len(entries):
        await call.answer("Invalid selection", show_alert=True)
        return
    removed = entries.pop(idx)
    await save_join_links(db_pool, entries)
    await call.message.answer(
        f"✅ حذف شد: {removed['label']} — {removed.get('title') or removed['label']}"
    )
    if entries:
        await call.message.answer(
            "فهرست به‌روزشده:",
            reply_markup=build_join_remove_keyboard(entries)
        )
    await call.answer()


@router.callback_query(F.data == "join:back")
async def cb_join_back(call: CallbackQuery) -> None:
    if not call.from_user or not is_admin(call.from_user.id):
        await call.answer("Not allowed", show_alert=True)
        return
    await call.message.answer("بازگشت به پنل ادمین:", reply_markup=admin_keyboard())
    await call.answer()


@router.message(JoinState.waiting_for_links)
async def receive_join_links(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        return
    if not message.text:
        await message.answer("لطفاً لینک‌ها را به صورت متنی ارسال کنید.")
        return

    lines = [line.strip() for line in message.text.splitlines() if line.strip()]
    if not lines:
        await message.answer("هیچ لینکی دریافت نشد.")
        return
    if not db_pool:
        await message.answer("Database is not ready yet.")
        return

    current = renumber_join_links(await get_join_links(db_pool))
    seen_chat_ids = {item["chat_id"] for item in current}
    added = 0
    errors: list[str] = []

    for raw in lines:
        try:
            target = await resolve_join_target(message.bot, raw)
            if target["chat_id"] in seen_chat_ids:
                continue
            current.append(target)
            seen_chat_ids.add(target["chat_id"])
            added += 1
        except Exception as exc:
            errors.append(f"• {raw} → {exc}")

    current = renumber_join_links(current)
    await save_join_links(db_pool, current)
    await state.clear()

    if added == 0 and errors:
        await message.answer(
            "❌ هیچ لینک معتبری اضافه نشد.\n\n"
            "⚠️ موارد نامعتبر:\n" + "\n".join(errors[:10])
        )
        return
        return

    msg = f"✅ {added} لینک جدید اضافه شد."
    if errors:
        msg += "\n\n⚠️ برخی موارد نامعتبر بودند:\n" + "\n".join(errors[:5])
    await message.answer(msg)


@router.callback_query(F.data == "wm:set")
async def cb_set_watermark(call: CallbackQuery, state: FSMContext) -> None:
    if not call.from_user or not is_admin(call.from_user.id):
        await call.answer("Not allowed", show_alert=True)
        return
    await state.set_state(WatermarkState.waiting_for_png)
    await call.message.answer(
        "Send the watermark as a PNG file (max 5 MB).",
        reply_markup=cancel_keyboard(),
    )
    await call.answer()


@router.callback_query(F.data == "wm:info")
async def cb_info(call: CallbackQuery) -> None:
    if not call.from_user or not is_admin(call.from_user.id):
        await call.answer("Not allowed", show_alert=True)
        return
    try:
        settings = await get_settings(db_pool)
        has_wm = bool(settings.get("watermark_png"))
        join_count = len(renumber_join_links(await get_join_links(db_pool)))
    except Exception:
        has_wm = False
        join_count = 0
    updated_at = settings.get("updated_at")
    text = (
        "<b>Current settings</b>\n\n"
        f"Watermark saved: <b>{'yes' if has_wm else 'no'}</b>\n"
        f"Join links: <b>{join_count}</b>\n"
        f"Updated at: <code>{updated_at}</code>\n"
        f"Storage channel: <code>{STORAGE_CHANNEL}</code>\n"
        f"Admin ID: <code>{ADMIN_ID}</code>"
    )
    await call.message.answer(text)
    await call.answer()


@router.callback_query(F.data == "wm:cancel")
async def cb_cancel(call: CallbackQuery, state: FSMContext) -> None:
    if not call.from_user or not is_admin(call.from_user.id):
        await call.answer("Not allowed", show_alert=True)
        return
    await state.clear()
    await call.message.answer("Canceled.")
    await call.answer()


@router.message(WatermarkState.waiting_for_png)
async def receive_watermark(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        return
    doc = message.document
    if not doc:
        await message.answer("Please send the watermark as a PNG document.")
        return
    if doc.file_size and doc.file_size > MAX_WATERMARK_BYTES:
        await message.answer("The watermark is too large. Maximum allowed size is 5 MB.")
        return
    if doc.mime_type != "image/png" and not (doc.file_name or "").lower().endswith(".png"):
        await message.answer("The file must be a PNG.")
        return
    if not db_pool:
        await message.answer("Database is not ready yet.")
        return
    file = await message.bot.get_file(doc.file_id)
    with tempfile.NamedTemporaryFile(delete=False, suffix=".png") as tmp:
        tmp_path = tmp.name
        await message.bot.download_file(file.file_path, destination=tmp)
    try:
        with open(tmp_path, "rb") as f:
            png_bytes = f.read()
        Image.open(BytesIO(png_bytes)).verify()
        await save_watermark(db_pool, png_bytes, doc.file_name or "watermark.png")
        await message.answer("✅ Watermark saved. It will remain fixed until you change it again.")
        await state.clear()
    except Exception as exc:
        await message.answer(f"Failed to save watermark: {exc}")
    finally:
        safe_unlink(tmp_path)


# -----------------------------------------------------------------------------
# Video entry handler
# -----------------------------------------------------------------------------

@router.message(StateFilter(None), F.video | F.photo | F.document)
async def admin_media_entry(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        return

    if await state.get_state() is not None:
        return

    media_kind = _media_kind(message)
    if media_kind is None:
        return

    settings = await get_watermark(db_pool)
    if not settings:
        await message.answer("Please set the watermark first using the admin panel.")
        return
    if not db_pool:
        await message.answer("Database is not ready yet.")
        return

    media_label = _media_noun(media_kind)
    await message.answer(f"⏳ Reading your {media_label} and generating the preview collage...")

    try:
        if media_kind == "video":
            source_path, original_name, _ = await download_admin_video(message)
        else:
            source_path, original_name, _ = await download_admin_image(message)
    except Exception as exc:
        await message.answer(f"❌ دانلود {media_label} ناموفق بود:\n{exc}")
        return

    job_id = secrets.token_hex(4)
    temp_dir = tempfile.mkdtemp(prefix=f"wmjob_{job_id}_")
    frame_path = str(Path(temp_dir) / "frame.jpg")
    collage_path = str(Path(temp_dir) / "collage.jpg")

    try:
        if media_kind == "video":
            await extract_frame(source_path, frame_path, at_seconds=1.0)
            with Image.open(frame_path) as im:
                frame_width, frame_height = im.size
            preview_source = frame_path
        else:
            with Image.open(source_path) as im:
                frame_width, frame_height = im.size
            preview_source = source_path

        def _build() -> tuple[int, int]:
            return build_preview_collage(
                preview_source,
                settings[0],
                collage_path,
                title=f"Choose watermark size for this {media_label}",
            )

        base_w, base_h = await asyncio.to_thread(_build)

        job = Job(
            job_id=job_id,
            admin_id=message.from_user.id,
            source_path=source_path,
            source_filename=original_name,
            frame_path=frame_path if media_kind == "video" else source_path,
            preview_path=collage_path,
            frame_width=base_w,
            frame_height=base_h,
            media_type=media_kind,
        )
        JOBS[job_id] = job

        caption = (
            f"🖼 <b>Choose the watermark size for this {media_label}</b>\n\n"
            "The watermark position is fixed to <b>bottom-right</b>.\n"
            f"The chosen size will apply only to this {media_label}."
        )
        await message.answer_photo(
            FSInputFile(collage_path),
            caption=caption,
            reply_markup=size_keyboard(job_id),
        )
    except Exception as exc:
        await message.answer(f"Failed to create preview: {exc}")
        safe_unlink(source_path)
        shutil.rmtree(temp_dir, ignore_errors=True)
        if job_id in JOBS:
            JOBS.pop(job_id, None)


@router.callback_query(F.data.startswith("sz:"))
async def cb_choose_size(call: CallbackQuery) -> None:
    if not call.from_user or not is_admin(call.from_user.id):
        await call.answer("Not allowed", show_alert=True)
        return
    try:
        _, job_id, idx_s = call.data.split(":")
        idx = int(idx_s)
    except Exception:
        await call.answer("Invalid selection", show_alert=True)
        return
    job = JOBS.get(job_id)
    if not job:
        await call.answer("This preview expired or is already processed.", show_alert=True)
        return
    if job.status != "pending":
        await call.answer("This job is already being processed.", show_alert=True)
        return
    if idx < 0 or idx >= len(PREVIEW_SIZES):
        await call.answer("Invalid size selection", show_alert=True)
        return
    job.status = "queued"
    job.chosen_index = idx
    size_percent = PREVIEW_SIZES[idx]
    try:
        await call.message.delete()
    except Exception:
        pass
    await call.answer(f"Selected {size_percent}%")
    asyncio.create_task(process_job(call.bot, job_id))


async def process_job(bot: Bot, job_id: str) -> None:
    job = JOBS.get(job_id)
    if not job:
        return
    async with PROCESS_LOCK:
        job = JOBS.get(job_id)
        if not job:
            return
        job.status = "processing"
        settings = await get_watermark(db_pool)
        if not settings:
            await bot.send_message(job.admin_id, "Watermark is missing. Please set it again.")
            cleanup_job(job_id)
            return
        watermark_bytes, watermark_name = settings
        selected_percent = PREVIEW_SIZES[job.chosen_index or 0]
        temp_dir = Path(tempfile.mkdtemp(prefix=f"render_{job_id}_"))
        resized_wm_path = str(temp_dir / "watermark.png")
        if job.media_type == "video":
            output_path = str(temp_dir / "final.mp4")
        else:
            output_path = str(temp_dir / "final.jpg")
        try:
            await asyncio.to_thread(
                resize_watermark_for_video,
                watermark_bytes,
                selected_percent,
                job.frame_width,
                resized_wm_path,
            )
            media_label = _media_noun(job.media_type)
            await bot.send_message(
                job.admin_id,
                f"⏳ Rendering {media_label} with <b>{selected_percent}%</b> watermark..."
            )
            if job.media_type == "video":
                await render_video_with_watermark(
                    input_path=job.source_path,
                    watermark_path=resized_wm_path,
                    output_path=output_path,
                    selected_percent=selected_percent,
                    frame_width=job.frame_width,
                )
            else:
                await asyncio.to_thread(
                    render_image_with_watermark,
                    job.source_path,
                    resized_wm_path,
                    output_path,
                )
            token = make_token()
            if job.media_type == "video":
                sent = await bot.send_video(
                    chat_id=STORAGE_CHANNEL,
                    video=FSInputFile(output_path),
                    caption=f"Watermarked video ({selected_percent}%)",
                    supports_streaming=True,
                )
            elif job.media_type == "photo":
                sent = await bot.send_photo(
                    chat_id=STORAGE_CHANNEL,
                    photo=FSInputFile(output_path),
                    caption=f"Watermarked photo ({selected_percent}%)",
                )
            else:
                sent = await bot.send_document(
                    chat_id=STORAGE_CHANNEL,
                    document=FSInputFile(output_path),
                    caption=f"Watermarked photo ({selected_percent}%)",
                )
            channel_message_id = sent.message_id
            await save_video_mapping(
                db_pool,
                token=token,
                channel_message_id=channel_message_id,
                original_filename=job.source_filename,
                media_type=job.media_type,
            )
            link = build_start_link(token)
            await bot.send_message(
                job.admin_id,
                f"✅ {media_label.capitalize()} stored in the channel.\n"
                f"🔗 Link:\n<code>{link}</code>"
            )
            job.status = "done"
        except Exception as exc:
            job.status = "failed"
            await bot.send_message(job.admin_id, f"Rendering failed: {exc}")
        finally:
            cleanup_job(job_id)
            shutil.rmtree(temp_dir, ignore_errors=True)


def cleanup_job(job_id: str) -> None:
    job = JOBS.pop(job_id, None)
    if not job:
        return
    safe_unlink(job.source_path)
    safe_unlink(job.frame_path)
    safe_unlink(job.preview_path)
    try:
        temp_dir = Path(job.frame_path).parent
        shutil.rmtree(temp_dir, ignore_errors=True)
    except Exception:
        pass


@router.message(Command("help"), F.chat.type == "private")
async def help_cmd(message: Message) -> None:
    if is_admin(message.from_user.id):
        await message.answer(
            "Admin flow:\n"
            "1) Press Set watermark and upload a PNG.\n"
            "2) Send a video (any size, Telethon will handle large files if configured).\n"
            "3) Choose a size from the collage.\n"
            "4) The bot uploads the final video to the channel and gives you the link."
        )
    else:
        await message.answer("Use the link you received from the admin.")


async def health_handler(request: web.Request) -> web.Response:
    return web.Response(text="ok")


async def start_polling_if_needed(app: web.Application) -> None:
    if should_use_webhook():
        return
    bot: Bot = app["bot"]
    try:
        await bot.delete_webhook(drop_pending_updates=True)
    except Exception:
        pass
    app["polling_task"] = asyncio.create_task(
        dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    )


async def stop_polling_if_running(app: web.Application) -> None:
    task = app.get("polling_task")
    if task is None:
        return
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task


# -----------------------------------------------------------------------------
# Webhook app
# -----------------------------------------------------------------------------


def should_use_webhook() -> bool:
    flag = _cfg("USE_WEBHOOK")
    return flag.lower() in {"1", "true", "yes", "on"}


async def startup_common(bot: Bot) -> None:
    global telethon_client, db_pool

    await ensure_bot_username(bot)
    if db_pool is None:
        raise RuntimeError("Database pool not initialized")

    # Start Telethon client if credentials are provided
    if API_ID and API_HASH and USER_SESSION_STRING:
        telethon_client = TelegramClient(
            StringSession(USER_SESSION_STRING),
            API_ID,
            API_HASH,
        )
        try:
            await telethon_client.start()
            me = await telethon_client.get_me()
            logger.info(f"Telethon client started successfully as @{me.username}")
            logger.info(f"Telethon client ID: {me.id}")
        except Exception as e:
            logger.error(f"Failed to start Telethon client: {e}")
            telethon_client = None
    else:
        logger.warning("API_ID, API_HASH, or USER_SESSION_STRING not set. Large file downloads will not work.")


async def on_startup(bot: Bot) -> None:
    await startup_common(bot)

    if should_use_webhook():
        if WEBHOOK_BASE_URL:
            webhook_url = WEBHOOK_BASE_URL.rstrip("/") + WEBHOOK_PATH
            try:
                await bot.set_webhook(webhook_url, drop_pending_updates=True)
                logger.info(f"✅ Webhook successfully set to: {webhook_url}")
            except Exception as e:
                logger.error(f"❌ Failed to set webhook: {e}")
                logger.warning(
                    "⚠️ Failed to auto-set webhook. The bot will still continue, "
                    "but you may need to expose a public URL."
                )
        else:
            logger.warning(
                "⚠️ Webhook mode is enabled, but no public URL was found.\n"
                "Set WEBHOOK_BASE_URL or RENDER_EXTERNAL_URL, or disable USE_WEBHOOK."
            )
    else:
        try:
            await bot.delete_webhook(drop_pending_updates=True)
        except Exception:
            pass
        logger.info("Polling mode active: webhook has been cleared automatically.")


async def on_shutdown(bot: Bot) -> None:
    try:
        await bot.delete_webhook(drop_pending_updates=False)
    except Exception:
        pass

    global db_pool, telethon_client
    if telethon_client is not None:
        try:
            await telethon_client.disconnect()
        except Exception:
            pass
        telethon_client = None

    if db_pool is not None:
        try:
            await db_pool.close()
        except Exception:
            pass
        db_pool = None

    try:
        await bot.session.close()
    except Exception:
        pass


async def init_app() -> web.Application:
    global db_pool
    db_pool = await asyncpg.create_pool(
        DATABASE_URL,
        min_size=1,
        max_size=1,
        statement_cache_size=0,
        command_timeout=60,
    )
    await init_db(db_pool)
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    app = web.Application()
    app["bot"] = bot
    app.router.add_get("/", health_handler)
    app.router.add_get("/health", health_handler)
    app.on_startup.append(start_polling_if_needed)
    app.on_cleanup.append(stop_polling_if_running)

    if should_use_webhook():
        SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path=WEBHOOK_PATH)
        setup_application(app, dp, bot=bot)

    return app


async def run_polling() -> None:
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=1, statement_cache_size=0, command_timeout=60)
    await init_db(db_pool)
    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


def main() -> None:
    web.run_app(init_app(), host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    main()
