
import asyncio
import json
import logging
import os
import secrets
import shutil
import tempfile
from dataclasses import dataclass, field
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Any, Optional

import asyncpg
from aiohttp import web, ClientTimeout
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatAction, ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError
from aiogram.filters import CommandStart, Command
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

try:
    import imageio_ffmpeg
except Exception:
    imageio_ffmpeg = None

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

ADMIN_ID = int(os.getenv("ADMIN_ID", "8883527571"))
STORAGE_CHANNEL = int(os.getenv("STORAGE_CHANNEL", "-1003890591020"))
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
BOT_USERNAME_ENV = os.getenv("BOT_USERNAME", "").strip()
WEBHOOK_BASE_URL = (os.getenv("WEBHOOK_BASE_URL") or os.getenv("RENDER_EXTERNAL_URL") or "").strip()
WEBHOOK_PATH = os.getenv("WEBHOOK_PATH", "/webhook").strip()
PORT = int(os.getenv("PORT", "8080"))

MAX_WATERMARK_BYTES = 5 * 1024 * 1024
MAX_VIDEO_BYTES = 200 * 1024 * 1024
PREVIEW_TILE_SIZE = (640, 360)
PREVIEW_SIZES = [18, 26, 34, 42]  # percentages of the frame width
WATERMARK_MARGIN = 20
OUTPUT_MAX_WIDTH = 1280

logging.basicConfig(level=logging.INFO)
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
    status: str = "pending"  # pending | queued | processing | done | failed
    chosen_index: Optional[int] = None
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


JOBS: dict[str, Job] = {}


class WatermarkState(StatesGroup):
    waiting_for_png = State()


class JoinState(StatesGroup):
    waiting_for_links = State()


# -----------------------------------------------------------------------------
# FFmpeg helpers
# -----------------------------------------------------------------------------

def _which_ffmpeg() -> str:
    """Return an ffmpeg executable path. Prefer imageio-ffmpeg when available."""
    if imageio_ffmpeg is not None:
        try:
            return imageio_ffmpeg.get_ffmpeg_exe()
        except Exception:
            pass

    for candidate in ("ffmpeg", "/usr/bin/ffmpeg", "/usr/local/bin/ffmpeg"):
        if shutil.which(candidate) or Path(candidate).exists():
            return candidate

    raise RuntimeError(
        "ffmpeg is not available. Install ffmpeg or add imageio-ffmpeg to requirements."
    )


FFMPEG = _which_ffmpeg()


async def run_cmd(cmd: list[str]) -> tuple[int, str, str]:
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, err = await proc.communicate()
    return proc.returncode, out.decode(errors="ignore"), err.decode(errors="ignore")


async def extract_frame(video_path: str, out_path: str, at_seconds: float = 1.0) -> None:
    cmd = [
        FFMPEG,
        "-y",
        "-ss",
        str(at_seconds),
        "-i",
        video_path,
        "-frames:v",
        "1",
        "-q:v",
        "2",
        out_path,
    ]
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

    # Strip metadata and compress a bit for Telegram delivery.
    vf = (
        f"scale={target_width}:-2,"
        f"format=yuv420p"
    )

    # First scale the watermark itself to the selected size, then overlay.
    # The watermark is pre-resized in Python for determinism and smaller command lines.
    cmd = [
        FFMPEG,
        "-y",
        "-i",
        input_path,
        "-i",
        watermark_path,
        "-map_metadata",
        "-1",
        "-map_chapters",
        "-1",
        "-vf",
        vf,
        "-filter_complex",
        f"[1:v]scale={watermark_width}:-1[wm];[0:v][wm]overlay=W-w-{margin}:H-h-{margin}:format=auto",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "28",
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        "-movflags",
        "+faststart",
        "-shortest",
        output_path,
    ]

    # NOTE: We intentionally scale the base video and overlay a sized watermark.
    # Some ffmpeg builds may not like both -vf and -filter_complex together.
    # To keep compatibility, the actual command is constructed in a safer way below.
    cmd = [
        FFMPEG,
        "-y",
        "-i",
        input_path,
        "-i",
        watermark_path,
        "-filter_complex",
        f"[0:v]scale={target_width}:-2,format=yuv420p[base];[1:v]scale={watermark_width}:-1[wm];[base][wm]overlay=W-w-{margin}:H-h-{margin}:format=auto[v]",
        "-map",
        "[v]",
        "-map",
        "0:a?",
        "-map_metadata",
        "-1",
        "-map_chapters",
        "-1",
        "-c:v",
        "libx264",
        "-preset",
        "veryfast",
        "-crf",
        "28",
        "-pix_fmt",
        "yuv420p",
        "-c:a",
        "aac",
        "-b:a",
        "128k",
        "-movflags",
        "+faststart",
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
    # PIL default font is used as fallback to avoid bundling extra assets.
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


def _extract_frame_sync(video_path: str, frame_path: str, at_seconds: float = 1.0) -> None:
    cmd = [
        FFMPEG,
        "-y",
        "-ss",
        str(at_seconds),
        "-i",
        video_path,
        "-frames:v",
        "1",
        "-q:v",
        "2",
        frame_path,
    ]
    result = __import__("subprocess").run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Frame extraction failed: {result.stderr[-1000:]}")


def build_preview_collage(
    frame_path: str,
    watermark_bytes: bytes,
    out_path: str,
) -> tuple[int, int]:
    frame = Image.open(frame_path).convert("RGB")
    watermark = Image.open(BytesIO(watermark_bytes)).convert("RGBA")
    base_w, base_h = frame.size

    # Use a fixed preview tile size for a clean 2x2 collage.
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
    title = "Choose watermark size for this video"
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
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
            """
        )


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
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO videos (token, channel_message_id, original_filename)
            VALUES ($1, $2, $3)
            """,
            token,
            channel_message_id,
            original_filename,
        )


async def get_video_mapping(pool: asyncpg.Pool, token: str) -> dict[str, Any] | None:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT token, channel_message_id, original_filename FROM videos WHERE token = $1",
            token,
        )
        return dict(row) if row else None


# -----------------------------------------------------------------------------
# UI helpers

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
    text = (
        "⚠️ <b>برای استفاده از ربات، ابتدا در کانال‌های زیر عضو شوید:</b>\n\n"
        f"{format_join_list(entries)}\n\n"
        "بعد از عضویت روی دکمه «✅ عضو شدم» بزنید."
    )
    await message.answer(text, reply_markup=build_join_keyboard(entries, payload))


async def deliver_user_video(bot: Bot, chat_id: int, token: str) -> None:
    mapping = await get_video_mapping(db_pool, token)
    if not mapping:
        await bot.send_message(chat_id, "این لینک نامعتبر است یا ویدیو دیگر در دسترس نیست.")
        return

    warning = await bot.send_message(
        chat_id,
        "⚠️ ویدیو را همین حالا ذخیره کنید.\n"
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
        await warning.edit_text("ویدیو در کانال ذخیره‌سازی در دسترس نیست.")
        return
    except TelegramForbiddenError:
        await warning.edit_text("امکان ارسال پیام به این کاربر وجود ندارد.")
        return

    asyncio.create_task(
        delete_later(bot, chat_id, [warning.message_id, copied_message_id], 10)
    )


async def handle_user_start_flow(message: Message, payload: str) -> None:
    missing = await get_missing_joins(message.bot, message.from_user.id)
    if missing:
        await send_join_required_prompt(message, payload)
        return

    if payload:
        await deliver_user_video(message.bot, message.chat.id, payload)
    else:
        await message.answer("لطفاً لینک ربات را از ادمین دریافت کنید.")


# -----------------------------------------------------------------------------
# UI helpers
# -----------------------------------------------------------------------------

def admin_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
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
    raise RuntimeError("BOT_USERNAME is not known yet. It will be fetched on startup.")


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


# -----------------------------------------------------------------------------
# Admin handlers
# -----------------------------------------------------------------------------

@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext) -> None:
    payload = ""
    if message.text and " " in message.text:
        payload = message.text.split(" ", 1)[1].strip()

    if is_admin(message.from_user.id):
        if payload:
            await message.answer("پنل ادمین", reply_markup=admin_keyboard())
            return

        try:
            settings = await get_settings(db_pool)
            has_wm = bool(settings.get("watermark_png"))
        except Exception as e:
            logger.exception("Admin settings load failed: %s", e)
            has_wm = False

        text = (
            "👋 <b>پنل ادمین</b>\n\n"
            f"واترمارک: <b>{'ذخیره شده' if has_wm else 'تنظیم نشده'}</b>\n"
            "برای تنظیم واترمارک، جوین اجباری و پردازش ویدیو از دکمه‌های زیر استفاده کنید."
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

    await deliver_user_video(message.bot, message.chat.id, payload)


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
                    renumber_join_links(await get_join_links(db_pool)),
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
        await deliver_user_video(call.bot, call.message.chat.id, payload)
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
        "از @username یا لینک public t.me استفاده کنید."
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
    except Exception as e:
        logger.exception("Settings load failed: %s", e)
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
        # Validate PNG is readable.
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
# Video handling for admin
# -----------------------------------------------------------------------------

def _is_video_message(message: Message) -> bool:
    if message.video:
        return True
    if message.document and (message.document.mime_type or "").startswith("video/"):
        return True
    return False


async def download_admin_video(message: Message) -> tuple[str, str, int]:
    """Download the admin's video to a temp file. Returns (path, filename, bytes)."""
    if message.video:
        file_id = message.video.file_id
        filename = message.video.file_name or "video.mp4"
        file_size = message.video.file_size or 0
    else:
        file_id = message.document.file_id
        filename = message.document.file_name or "video.mp4"
        file_size = message.document.file_size or 0

    if file_size and file_size > MAX_VIDEO_BYTES:
        raise ValueError("Video is too large. Maximum allowed size is 200 MB.")

    try:
        file = await message.bot.get_file(file_id)
    except TelegramBadRequest as exc:
        raise ValueError(
            "تلگرام اجازه دسترسی به این فایل را از طریق Bot API نمی‌دهد. "
            "لطفاً فایل را کمی کوچک‌تر یا به صورت فشرده‌تر ارسال کنید."
        ) from exc

    suffix = Path(filename).suffix or ".mp4"
    with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
        tmp_path = tmp.name

    file_url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file.file_path}"
    timeout = ClientTimeout(total=None, sock_connect=60, sock_read=60)
    async with message.bot.session.get(file_url, timeout=timeout) as resp:
        resp.raise_for_status()
        with open(tmp_path, "wb") as f:
            async for chunk in resp.content.iter_chunked(1024 * 1024):
                f.write(chunk)

    return tmp_path, filename, file_size


@router.message(F.video | F.document)
async def admin_video_entry(message: Message, state: FSMContext) -> None:
    if not is_admin(message.from_user.id):
        return

    # If we are waiting for the watermark PNG, do not process video here.
    if await state.get_state() == WatermarkState.waiting_for_png.state:
        return

    if not _is_video_message(message):
        return

    settings = await get_watermark(db_pool)
    if not settings:
        await message.answer("Please set the watermark first using the admin panel.")
        return

    watermark_bytes, watermark_name = settings

    if not db_pool:
        await message.answer("Database is not ready yet.")
        return

    await message.answer("⏳ Reading your video and generating the preview collage...")

    try:
        video_path, original_name, _ = await download_admin_video(message)
    except Exception as exc:
        await message.answer(f"❌ دانلود ویدیو ناموفق بود:\n{exc}")
        return

    job_id = secrets.token_hex(4)
    temp_dir = tempfile.mkdtemp(prefix=f"wmjob_{job_id}_")
    frame_path = str(Path(temp_dir) / "frame.jpg")
    collage_path = str(Path(temp_dir) / "collage.jpg")

    try:
        await extract_frame(video_path, frame_path, at_seconds=1.0)
        with Image.open(frame_path) as im:
            frame_width, frame_height = im.size

        # Build the collage in a thread so Pillow does not block the event loop.
        def _build() -> tuple[int, int]:
            return build_preview_collage(frame_path, watermark_bytes, collage_path)

        base_w, base_h = await asyncio.to_thread(_build)

        job = Job(
            job_id=job_id,
            admin_id=message.from_user.id,
            source_path=video_path,
            source_filename=original_name,
            frame_path=frame_path,
            preview_path=collage_path,
            frame_width=base_w,
            frame_height=base_h,
        )
        JOBS[job_id] = job

        caption = (
            "🖼 <b>Choose the watermark size for this video</b>\n\n"
            "The watermark position is fixed to <b>bottom-right</b>.\n"
            "The chosen size will apply only to this video."
        )
        await message.answer_photo(
            FSInputFile(collage_path),
            caption=caption,
            reply_markup=size_keyboard(job_id),
        )
    except Exception as exc:
        await message.answer(f"Failed to create preview: {exc}")
        safe_unlink(video_path)
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

    # Process in the background so the callback returns immediately.
    asyncio.create_task(process_job(call.bot, job_id))


async def process_job(bot: Bot, job_id: str) -> None:
    job = JOBS.get(job_id)
    if not job:
        return

    async with PROCESS_LOCK:
        # Re-check after waiting for the lock.
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
        output_path = str(temp_dir / "final.mp4")

        try:
            # Resize the watermark PNG for this specific video.
            await asyncio.to_thread(
                resize_watermark_for_video,
                watermark_bytes,
                selected_percent,
                job.frame_width,
                resized_wm_path,
            )

            await bot.send_message(
                job.admin_id,
                f"⏳ Rendering video with <b>{selected_percent}%</b> watermark..."
            )

            await render_video_with_watermark(
                input_path=job.source_path,
                watermark_path=resized_wm_path,
                output_path=output_path,
                selected_percent=selected_percent,
                frame_width=job.frame_width,
            )

            token = make_token()
            sent = await bot.send_video(
                chat_id=STORAGE_CHANNEL,
                video=FSInputFile(output_path),
                caption=f"Watermarked video ({selected_percent}%)",
                supports_streaming=True,
            )

            channel_message_id = sent.message_id
            await save_video_mapping(
                db_pool,
                token=token,
                channel_message_id=channel_message_id,
                original_filename=job.source_filename,
            )

            link = build_start_link(token)
            await bot.send_message(
                job.admin_id,
                "✅ Video stored in the channel.\n"
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
    # Remove the job temp directory if it still exists.
    try:
        temp_dir = Path(job.frame_path).parent
        shutil.rmtree(temp_dir, ignore_errors=True)
    except Exception:
        pass


# -----------------------------------------------------------------------------
# Fallback / admin help
# -----------------------------------------------------------------------------

@router.message(Command("help"))
async def help_cmd(message: Message) -> None:
    if is_admin(message.from_user.id):
        await message.answer(
            "Admin flow:\n"
            "1) Press Set watermark and upload a PNG.\n"
            "2) Send a video.\n"
            "3) Choose a size from the collage.\n"
            "4) The bot uploads the final video to the channel and gives you the link."
        )
    else:
        await message.answer("Use the link you received from the admin.")


# -----------------------------------------------------------------------------
# Webhook app
# -----------------------------------------------------------------------------

async def on_startup(bot: Bot) -> None:
    await ensure_bot_username(bot)
    if db_pool is None:
        raise RuntimeError("Database pool not initialized")

    if WEBHOOK_BASE_URL:
        webhook_url = WEBHOOK_BASE_URL.rstrip("/") + WEBHOOK_PATH
        await bot.set_webhook(webhook_url, drop_pending_updates=True)
        logger.info("Webhook set to %s", webhook_url)
    else:
        logger.warning(
            "WEBHOOK_BASE_URL is empty; webhook will not be registered. "
            "On Render, it should fall back to RENDER_EXTERNAL_URL automatically."
        )


async def on_shutdown(bot: Bot) -> None:
    try:
        await bot.delete_webhook(drop_pending_updates=False)
    except Exception:
        pass

    global db_pool
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

    # Create the asyncpg pool on the same event loop that will serve webhook updates.
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=1, statement_cache_size=0, command_timeout=60)
    await init_db(db_pool)

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )

    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    app = web.Application()
    SimpleRequestHandler(dispatcher=dp, bot=bot).register(app, path=WEBHOOK_PATH)
    setup_application(app, dp, bot=bot)
    app["bot"] = bot
    return app


def main() -> None:
    # Let aiohttp own the event loop so asyncpg/Bot are created on the same loop.
    web.run_app(init_app(), host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    main()
