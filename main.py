import asyncio
import fcntl
import html
import logging
import os
import re
import sys
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from urllib.parse import urlparse, urlunparse

import aiosqlite

from aiogram import BaseMiddleware, Bot, Dispatcher, F
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import Command, CommandObject
from aiogram.types import (
    CallbackQuery,
    ErrorEvent,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    FSInputFile,
)
from dotenv import load_dotenv

from database import (
    DB_NAME,
    add_comments_to_task,
    add_member_note,
    add_member_warning,
    add_reddit_account,
    approve_reddit_account,
    approve_submission,
    archive_completed_records,
    archive_task,
    auto_cleanup_claims,
    ban_user,
    cancel_payment,
    claim_comment,
    close_task,
    count_archivable_records,
    create_db,
    create_task,
    clone_task,
    disable_reddit_account,
    flag_submission,
    get_active_claim,
    get_active_claims,
    get_active_reddit_accounts,
    get_active_tasks,
    get_all_member_ids,
    get_allowed_active_claim_count,
    get_comment_analytics,
    get_daily_stats,
    get_flagged_submissions,
    get_member_stats,
    get_payment_history,
    get_pending_payments,
    get_pending_payments_grouped,
    get_pending_reddit_accounts,
    get_pending_submissions,
    get_reddit_account_by_id,
    get_reddit_account_by_username,
    get_reddit_account_health,
    get_setting,
    get_submission_for_payment,
    get_submission_history,
    get_submissions_today_count,
    get_submissions_to_check,
    get_system_stats,
    get_task_stats,
    get_tasks_by_post_id,
    get_total_stats,
    get_live_check_stats,
    get_user,
    get_user_by_username,
    get_user_submissions_for_live_check,
    list_reddit_accounts,
    log_audit_action,
    mark_payment_paid,
    mark_user_payments_paid,
    normalize_reddit_username,
    reddit_comment_id_exists,
    refresh_payable_for_user,
    register_user,
    reject_reddit_account,
    reject_submission,
    remove_reddit_account,
    reopen_task_with_comments,
    reset_used_comments,
    save_qr_file_id,
    save_submission,
    save_upi_id,
    set_max_reddit_accounts,
    set_setting,
    shadowban_user,
    submission_exists_for_comment,
    submission_link_exists,
    touch_reddit_account_last_used,
    unban_user,
    update_reputation,
    update_submission_comment_status,
    update_task_status,
    warn_reddit_account,
    _sum_payment_amounts,
)


load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
dp = Dispatcher()
user_states = {}


import contextvars

# Per-update response counter — exposed as a ContextVar so it is correctly
# isolated across concurrent handler executions.
_response_counter: contextvars.ContextVar = contextvars.ContextVar("response_counter", default=None)


class _ResponseTracker:
    __slots__ = ("count", "update_id", "user_id", "handler", "trace")

    def __init__(self, update_id, user_id, handler):
        self.count = 0
        self.update_id = update_id
        self.user_id = user_id
        self.handler = handler
        self.trace = []  # list[str] short description per response

    def record(self, kind):
        self.count += 1
        self.trace.append(kind)


def install_bot_response_tracking(bot: Bot):
    """Monkey-patch a few bot methods to bump _response_counter when called.

    Tracks: send_message, send_photo, send_document, edit_message_text,
    edit_message_caption, answer_callback_query.
    Done once per Bot instance.
    """
    if getattr(bot, "_response_tracking_installed", False):
        return
    methods = [
        "send_message", "send_photo", "send_document",
        "edit_message_text", "edit_message_caption",
        "answer_callback_query",
    ]
    for name in methods:
        original = getattr(bot, name, None)
        if not original or not callable(original):
            continue
        def make(orig, m_name):
            async def wrapped(*a, **kw):
                tracker = _response_counter.get()
                if tracker is not None:
                    tracker.record(m_name)
                return await orig(*a, **kw)
            return wrapped
        # Bind to instance via setattr (overrides bound method lookup)
        setattr(bot, name, make(original, name))
    bot._response_tracking_installed = True


class AccessDebugMiddleware(BaseMiddleware):
    """Logs every incoming update and tracks per-update response count.

    A WARNING is emitted if a single update results in more than one outbound
    response — that's the signature of the duplicate-response bug.
    """
    async def __call__(self, handler, event, data):
        if isinstance(event, Message):
            user = event.from_user
            chat = event.chat
            update_id = getattr(getattr(event, "message_id", None), "real", None) or event.message_id
            logging.info(
                "[user_message] user_id=%s username=%s chat_type=%s text=%r",
                user.id if user else None,
                user.username if user else None,
                chat.type if chat else None,
                event.text,
            )
            handler_label = "msg"
        elif isinstance(event, CallbackQuery):
            user = event.from_user
            message = event.message
            chat = message.chat if message else None
            update_id = event.id
            logging.info(
                "[user_callback] user_id=%s username=%s chat_type=%s data=%r",
                user.id if user else None,
                user.username if user else None,
                chat.type if chat else None,
                event.data,
            )
            handler_label = f"cb:{event.data}"
        else:
            update_id, user, handler_label = None, None, type(event).__name__

        tracker = _ResponseTracker(update_id, user.id if user else None, handler_label)
        token = _response_counter.set(tracker)
        try:
            return await handler(event, data)
        finally:
            _response_counter.reset(token)
            if tracker.count == 0:
                logging.info(
                    "[response] update=%s user=%s handler=%s count=0",
                    tracker.update_id, tracker.user_id, tracker.handler,
                )
            elif tracker.count == 1:
                logging.info(
                    "[response] update=%s user=%s handler=%s count=1 kind=%s",
                    tracker.update_id, tracker.user_id, tracker.handler, tracker.trace[0],
                )
            else:
                logging.warning(
                    "[response] DUPLICATE_RESPONSE update=%s user=%s handler=%s count=%s trace=%s",
                    tracker.update_id, tracker.user_id, tracker.handler,
                    tracker.count, tracker.trace,
                )


dp.message.outer_middleware(AccessDebugMiddleware())
dp.callback_query.outer_middleware(AccessDebugMiddleware())
_INSTANCE_LOCK_FILE = None

# User Buttons
BTN_CLAIM = "📋 Claim Task"
BTN_SUBMIT = "📤 Submit Proof"
BTN_MY_STATS = "📊 My Stats"
BTN_PAYMENTS = "💸 Payment Info"
BTN_REDDIT_ACCOUNTS = "👤 Manage Reddit Accounts"
BTN_RULES = "📜 Rules"
BTN_HELP = "❓ Help"

# Submenu Buttons
BTN_ADMIN_TASKS = "📂 Tasks"
BTN_ADMIN_REVIEWS = "🧾 Reviews"
BTN_ADMIN_PAYMENTS = "💸 Payments"
BTN_ADMIN_MEMBERS = "👥 Members"
BTN_ADMIN_ANALYTICS = "📊 Analytics"
BTN_ADMIN_BROADCAST = "📢 Broadcast"
BTN_ADMIN_SETTINGS = "⚙ Settings"

# Task Submenu
BTN_TASKS_NEW = "➕ New Task"
BTN_TASKS_ADD_COMMENTS = "📝 Add Comments"
BTN_TASKS_ACTIVE = "📂 Active Tasks"
BTN_TASKS_STATS = "📈 Task Stats"
BTN_TASKS_MANAGE = "🔒 Manage Tasks"
BTN_TASKS_ARCHIVED = "🗃 Archived Tasks"
BTN_TASKS_REOPEN = "♻ Reopen Task"
BTN_TASKS_CLONE = "📦 Clone Task"
BTN_TASKS_RESET = "🧹 Reset Comments"

# Review Submenu
BTN_REVIEWS_PENDING = "🧾 Pending Reviews"
BTN_REVIEWS_FLAGGED = "⚠ Flagged Submissions"
BTN_REVIEWS_HISTORY = "📜 Review History"

# Payment Submenu
BTN_PAYMENTS_PENDING = "💸 Pending Payments"
BTN_PAYMENTS_PAID = "✅ Paid History"
BTN_PAYMENTS_STATS = "📊 Payment Stats"

# Member Submenu
BTN_MEMBERS_SEARCH = "👥 Search Member"
BTN_MEMBERS_WARNED = "⚠ Warnings"
BTN_MEMBERS_BANNED = "🚫 Banned Members"

# Analytics Submenu
BTN_ANALYTICS_DAILY = "📈 Daily Stats"
BTN_ANALYTICS_SYSTEM = "📊 System Stats"
BTN_ANALYTICS_EARNINGS = "💰 Earnings Stats"

# Shared/Nav
BTN_SET_UPI = "💳 Set UPI ID"
BTN_UPLOAD_QR = "🖼 Upload QR"
BTN_PAYMENT_HISTORY = "📜 Payment History"
BTN_TOTAL_EARNINGS = "💰 Total Earnings"
BTN_BACK = "⬅️ Back"
BTN_HOME = "🏠 Home"
BTN_CANCEL = "❌ Cancel"

# New admin operational buttons
BTN_PAYMENTS_PROCESS = "💸 Process Payments"
BTN_LIVE_DASHBOARD = "📊 Live Check Dashboard"
BTN_SETTINGS_CLEANUP = "🧹 Cleanup Completed Records"
BTN_SETTINGS_BACKUP = "📦 Backup Database"

CATEGORIES = ["Comment", "Upvote", "Discussion", "Review", "Meme", "Advice", "Story", "Finance", "Tech", "Relationship"]
CLAIM_COOLDOWN_SECONDS = 30
SUBMIT_COOLDOWN_SECONDS = 60

BUTTON_ALIASES = {
    BTN_CLAIM: {"Claim Task", "Claim"},
    BTN_SUBMIT: {"Submit Proof", "Submit"},
    BTN_MY_STATS: {"My Stats", "Stats", "Member Stats"},
    BTN_PAYMENTS: {"Payments", "💰 Payments", "Payment Info"},
    BTN_RULES: {"Rules"},
    BTN_HELP: {"Help"},
    BTN_BACK: {"Back"},
    BTN_HOME: {"Home"},
    BTN_CANCEL: {"Cancel"},
}


def normalize_button_text(text):
    return re.sub(r"\s+", " ", text.replace("\ufe0f", "")).strip().casefold()


BUTTON_TEXT_TO_KEY = {}
# Populate with all primary constants first
all_button_constants = [
    BTN_CLAIM, BTN_SUBMIT, BTN_MY_STATS, BTN_PAYMENTS, BTN_REDDIT_ACCOUNTS, BTN_RULES, BTN_HELP,
    BTN_BACK, BTN_HOME, BTN_CANCEL,
    BTN_ADMIN_TASKS, BTN_ADMIN_REVIEWS, BTN_ADMIN_PAYMENTS, BTN_ADMIN_MEMBERS,
    BTN_ADMIN_ANALYTICS, BTN_ADMIN_BROADCAST, BTN_ADMIN_SETTINGS,
    BTN_TASKS_NEW, BTN_TASKS_ADD_COMMENTS, BTN_TASKS_ACTIVE, BTN_TASKS_STATS, BTN_TASKS_MANAGE, BTN_TASKS_ARCHIVED,
    BTN_TASKS_REOPEN, BTN_TASKS_CLONE, BTN_TASKS_RESET,
    BTN_REVIEWS_PENDING, BTN_REVIEWS_FLAGGED, BTN_REVIEWS_HISTORY,
    BTN_PAYMENTS_PENDING, BTN_PAYMENTS_PAID, BTN_PAYMENTS_STATS,
    BTN_MEMBERS_SEARCH, BTN_MEMBERS_WARNED, BTN_MEMBERS_BANNED,
    BTN_ANALYTICS_DAILY, BTN_ANALYTICS_SYSTEM, BTN_ANALYTICS_EARNINGS,
    # Payment submenu buttons — were missing, causing UPI/QR to show "Choose an option from the menu."
    BTN_SET_UPI, BTN_UPLOAD_QR, BTN_PAYMENT_HISTORY, BTN_TOTAL_EARNINGS,
    # New admin operational buttons
    BTN_PAYMENTS_PROCESS, BTN_LIVE_DASHBOARD, BTN_SETTINGS_CLEANUP, BTN_SETTINGS_BACKUP,
]
for btn in all_button_constants:
    BUTTON_TEXT_TO_KEY[normalize_button_text(btn)] = btn

for button, aliases in BUTTON_ALIASES.items():
    for alias in aliases:
        BUTTON_TEXT_TO_KEY[normalize_button_text(alias)] = button


def button_key(text):
    if not text:
        return None
    return BUTTON_TEXT_TO_KEY.get(normalize_button_text(text))


def button_filter(button):
    return F.text.func(lambda text: button_key(text) == button)


def member_inline_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=BTN_CLAIM, callback_data="member:claim"),
            InlineKeyboardButton(text=BTN_SUBMIT, callback_data="member:submit"),
        ],
        [
            InlineKeyboardButton(text=BTN_MY_STATS, callback_data="member:stats"),
            InlineKeyboardButton(text=BTN_PAYMENTS, callback_data="member:payments"),
        ],
        [
            InlineKeyboardButton(text=BTN_REDDIT_ACCOUNTS, callback_data="member:reddit"),
            InlineKeyboardButton(text=BTN_TASKS_ACTIVE, callback_data="member:active_tasks"),
        ],
        [
            InlineKeyboardButton(text=BTN_HELP, callback_data="member:help"),
            InlineKeyboardButton(text=BTN_RULES, callback_data="member:rules"),
        ],
    ])


def payment_inline_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text=BTN_SET_UPI, callback_data="member:set_upi"),
            InlineKeyboardButton(text=BTN_UPLOAD_QR, callback_data="member:upload_qr"),
        ],
        [
            InlineKeyboardButton(text=BTN_PAYMENT_HISTORY, callback_data="member:payment_history"),
            InlineKeyboardButton(text=BTN_TOTAL_EARNINGS, callback_data="member:total_earnings"),
        ],
        [InlineKeyboardButton(text=BTN_HOME, callback_data="member:home")],
    ])


def actor_id(event):
    return event.from_user.id if event.from_user else None


def actor_username(event):
    return event.from_user.username if event.from_user else None


async def require_real_user(event):
    if event.from_user:
        return True
    target = event.message if isinstance(event, CallbackQuery) else event
    await target.answer(
        "I cannot identify anonymous/channel-sent messages. Please interact as your personal Telegram account."
    )
    return False


def describe_state(state):
    if not state:
        return "none"
    flow = state.get("flow", "unknown")
    step = state.get("step")
    return f"{flow}:{step}" if step else flow


def set_user_state(user_id, state):
    previous = user_states.get(user_id)
    user_states[user_id] = state
    logging.info(
        "State set: user=%s previous=%s active=%s",
        user_id,
        describe_state(previous),
        describe_state(state),
    )


def clear_user_state(user_id, reason):
    previous = user_states.pop(user_id, None)
    if previous:
        logging.info(
            "State cleared: user=%s previous=%s reason=%s",
            user_id,
            describe_state(previous),
            reason,
        )


def acquire_single_instance_lock():
    global _INSTANCE_LOCK_FILE
    lock_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot.polling.lock")
    _INSTANCE_LOCK_FILE = open(lock_path, "w")
    try:
        fcntl.flock(_INSTANCE_LOCK_FILE, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        logging.error(
            "Another bot polling instance is already running. Stop the old process before starting this one."
        )
        sys.exit(1)
    _INSTANCE_LOCK_FILE.seek(0)
    _INSTANCE_LOCK_FILE.truncate()
    _INSTANCE_LOCK_FILE.write(str(os.getpid()))
    _INSTANCE_LOCK_FILE.flush()
    logging.info("Single-instance polling lock acquired: pid=%s path=%s", os.getpid(), lock_path)


def log_button_click(message, button):
    if button_key(message.text) == button:
        logging.info(
            "[handler_matched] user_id=%s username=%s chat_type=%s handler=button:%s active_state=%s",
            message.from_user.id,
            message.from_user.username,
            message.chat.type,
            button,
            describe_state(user_states.get(message.from_user.id)),
        )


def log_callback_click(callback, action):
    chat = callback.message.chat if callback.message else None
    logging.info(
        "[callback_matched] user_id=%s username=%s chat_type=%s handler=%s data=%s active_state=%s",
        callback.from_user.id,
        callback.from_user.username,
        chat.type if chat else None,
        action,
        callback.data,
        describe_state(user_states.get(callback.from_user.id)),
    )


def log_handler_match(message, handler_name):
    logging.info(
        "[handler_matched] user_id=%s username=%s chat_type=%s text=%r handler=%s active_state=%s",
        message.from_user.id if message.from_user else None,
        message.from_user.username if message.from_user else None,
        message.chat.type if message.chat else None,
        message.text,
        handler_name,
        describe_state(user_states.get(message.from_user.id)) if message.from_user else "none",
    )


def load_admin_ids():
    raw_admin_ids = os.getenv("ADMIN_IDS", "")
    return {int(item.strip()) for item in raw_admin_ids.split(",") if item.strip().isdigit()}


ADMIN_IDS = load_admin_ids()


async def is_admin(user_id):
    if user_id in ADMIN_IDS:
        return True
    user = await get_user(user_id)
    return user and user.get("role") in ("admin", "moderator", "owner")


async def main_menu(user_id):
    if await is_admin(user_id):
        return await admin_menu()

    buttons = [
        [KeyboardButton(text=BTN_CLAIM), KeyboardButton(text=BTN_SUBMIT)],
        [KeyboardButton(text=BTN_MY_STATS), KeyboardButton(text=BTN_PAYMENTS)],
        [KeyboardButton(text=BTN_REDDIT_ACCOUNTS), KeyboardButton(text=BTN_TASKS_ACTIVE)],
        [KeyboardButton(text=BTN_HELP), KeyboardButton(text=BTN_RULES)],
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


async def admin_menu():
    buttons = [
        [KeyboardButton(text=BTN_ADMIN_TASKS), KeyboardButton(text=BTN_ADMIN_REVIEWS)],
        [KeyboardButton(text=BTN_ADMIN_PAYMENTS), KeyboardButton(text=BTN_ADMIN_MEMBERS)],
        [KeyboardButton(text=BTN_ADMIN_ANALYTICS), KeyboardButton(text=BTN_ADMIN_BROADCAST)],
        [KeyboardButton(text=BTN_SETTINGS_CLEANUP), KeyboardButton(text=BTN_SETTINGS_BACKUP)],
        [KeyboardButton(text=BTN_ADMIN_SETTINGS), KeyboardButton(text=BTN_HOME)],
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def admin_tasks_menu():
    buttons = [
        [KeyboardButton(text=BTN_TASKS_NEW), KeyboardButton(text=BTN_TASKS_ADD_COMMENTS)],
        [KeyboardButton(text=BTN_TASKS_ACTIVE), KeyboardButton(text=BTN_TASKS_STATS)],
        [KeyboardButton(text=BTN_TASKS_MANAGE), KeyboardButton(text=BTN_TASKS_ARCHIVED)],
        [KeyboardButton(text=BTN_BACK)],
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def admin_reviews_menu():
    buttons = [
        [KeyboardButton(text=BTN_REVIEWS_PENDING), KeyboardButton(text=BTN_REVIEWS_FLAGGED)],
        [KeyboardButton(text=BTN_REVIEWS_HISTORY), KeyboardButton(text=BTN_BACK)],
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def admin_payments_menu():
    buttons = [
        [KeyboardButton(text=BTN_PAYMENTS_PROCESS)],
        [KeyboardButton(text=BTN_LIVE_DASHBOARD)],
        [KeyboardButton(text=BTN_PAYMENTS_PENDING), KeyboardButton(text=BTN_PAYMENTS_PAID)],
        [KeyboardButton(text=BTN_PAYMENTS_STATS), KeyboardButton(text=BTN_BACK)],
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def admin_members_menu():
    buttons = [
        [KeyboardButton(text=BTN_MEMBERS_SEARCH)],
        [KeyboardButton(text=BTN_MEMBERS_WARNED), KeyboardButton(text=BTN_MEMBERS_BANNED)],
        [KeyboardButton(text=BTN_BACK)],
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def admin_analytics_menu():
    buttons = [
        [KeyboardButton(text=BTN_ANALYTICS_DAILY), KeyboardButton(text=BTN_ANALYTICS_SYSTEM)],
        [KeyboardButton(text=BTN_ANALYTICS_EARNINGS)],
        [KeyboardButton(text=BTN_BACK)],
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def payments_menu():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_SET_UPI), KeyboardButton(text=BTN_UPLOAD_QR)],
            [KeyboardButton(text=BTN_PAYMENT_HISTORY), KeyboardButton(text=BTN_TOTAL_EARNINGS)],
            [KeyboardButton(text=BTN_BACK)],
        ],
        resize_keyboard=True,
    )


def get_pagination_keyboard(current_page, total_pages, callback_prefix):
    """Generic pagination buttons."""
    buttons = []
    nav_row = []
    if current_page > 1:
        nav_row.append(InlineKeyboardButton(text="⬅️ Previous", callback_data=f"{callback_prefix}:page:{current_page-1}"))

    nav_row.append(InlineKeyboardButton(text=f"{current_page}/{total_pages}", callback_data="nav:noop"))

    if current_page < total_pages:
        nav_row.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"{callback_prefix}:page:{current_page+1}"))

    if len(nav_row) > 1:
        buttons.append(nav_row)
    return buttons


@dp.callback_query(F.data == "nav:noop")
async def nav_noop_callback(callback: CallbackQuery):
    """The 'page X/Y' indicator is non-interactive. Just dismiss the spinner."""
    await callback.answer()


def escape_markdown(text):
    """Escape special characters for legacy Telegram Markdown."""
    if not text:
        return ""
    # Only escaping _ and * for legacy Markdown
    return text.replace("_", "\\_").replace("*", "\\*")


def safe_markdown(text):
    """Escape Telegram MarkdownV2 special characters."""
    if text is None:
        return ""
    return re.sub(r"([_*\[\]()~`>#+\-=|{}.!])", r"\\\1", str(text))


def safe_html(text):
    if text is None:
        return ""
    return html.escape(str(text), quote=False)


async def answer_with_fallback(target, formatted_text, plain_text, *, parse_mode="HTML", **kwargs):
    try:
        await target.answer(formatted_text, parse_mode=parse_mode, **kwargs)
        return True
    except TelegramBadRequest:
        logging.exception(
            "Formatted Telegram message failed; retrying without parse_mode. parse_mode=%s preview=%r",
            parse_mode,
            formatted_text[:500],
        )
        fallback_kwargs = dict(kwargs)
        fallback_kwargs.pop("parse_mode", None)
        await target.answer(plain_text, **fallback_kwargs)
        return False


def parse_reddit_url(url):
    """
    Robust Reddit URL parser.
    Extracts subreddit, post_id, and optional comment_id.
    Normalizes by lowercasing subreddit, removing query params and tracking.

    Supported formats:
      /r/sub/comments/postid/
      /r/sub/comments/postid/title_slug/
      /r/sub/comments/postid/title_slug/commentid/
      /r/sub/comments/postid/_/commentid/
      /r/sub/comments/postid/comment/commentid/   ← new Reddit share format
    """
    try:
        url = url.strip()
        # Strip query parameters and fragments
        for sep in ("?", "#"):
            if sep in url:
                url = url.split(sep)[0]

        url = url.rstrip("/")
        parsed = urlparse(url)
        host = parsed.netloc.lower()

        # Handle various reddit domains
        if not any(d in host for d in {"reddit.com", "redd.it"}):
            return None

        parts = [p for p in parsed.path.split("/") if p]

        # Minimum: /r/<sub>/comments/<post_id>
        if len(parts) < 4 or parts[0].lower() != "r" or parts[2].lower() != "comments":
            return None

        subreddit = parts[1].lower()
        post_id = parts[3].lower()
        comment_id = None

        # Detect comment_id from various URL layouts:
        #   len=5: /r/sub/comments/postid/slug           → no comment
        #   len=6: /r/sub/comments/postid/slug/commentid → parts[5] is comment
        #   len=6: /r/sub/comments/postid/_/commentid    → parts[5] is comment
        #   len=6: /r/sub/comments/postid/comment/cid    → parts[5] is comment (new share URL)
        if len(parts) >= 6:
            candidate = parts[5].lower()
            # Only treat as comment_id if it looks like a Reddit base-36 ID (3-10 alphanumeric chars)
            if re.match(r'^[a-z0-9]{3,10}$', candidate):
                comment_id = candidate

        post_path = f"/r/{subreddit}/comments/{post_id}"
        normalized_url = f"https://reddit.com{post_path}"
        if comment_id:
            normalized_url += f"/_/{comment_id}"

        return {
            "subreddit": subreddit,
            "post_id": post_id,
            "comment_id": comment_id,
            "normalized_url": normalized_url,
            "post_path": post_path,
        }
    except Exception:
        logging.exception("Failed to parse Reddit URL: %s", url)
        return None


def is_reddit_short_url(url):
    try:
        parsed = urlparse(url.strip())
        host = parsed.netloc.lower()
        parts = [part.lower() for part in parsed.path.strip("/").split("/") if part]
        return (
            host in {"redd.it", "www.redd.it"}
            or host.endswith(".redd.it")
            or (
                host in {"reddit.com", "www.reddit.com", "old.reddit.com", "np.reddit.com", "m.reddit.com"}
                and (
                    parts[0:1] == ["s"]
                    or (len(parts) >= 4 and parts[0] == "r" and parts[2] == "s")
                )
            )
        )
    except Exception:
        return False


async def resolve_reddit_short_url(url):
    """Follow Reddit share-link redirects and return the final Reddit URL."""
    import aiohttp

    headers = {
        "User-Agent": "TaskWorkerBot/1.0",
    }
    timeout = aiohttp.ClientTimeout(total=10)
    async with aiohttp.ClientSession(headers=headers, timeout=timeout) as session:
        async with session.get(url, allow_redirects=True) as response:
            return str(response.url)


async def parse_reddit_post_url_for_task(raw_url):
    details = parse_reddit_url(raw_url)
    if details:
        return details, None

    if not is_reddit_short_url(raw_url):
        return None, None

    try:
        resolved_url = await resolve_reddit_short_url(raw_url)
    except Exception:
        logging.exception("Failed to resolve Reddit short URL: %s", raw_url)
        return None, "resolve_failed"

    details = parse_reddit_url(resolved_url)
    if details:
        details["source_url"] = raw_url
        details["resolved_url"] = resolved_url
        return details, None
    return None, "resolved_invalid"


def duplicate_task_summary(tasks):
    if not tasks:
        return ""
    lines = []
    for task in tasks[:5]:
        lines.append(
            f"#{task['id']} · {task['status']} · {task['payout_amount']} · {task['total_slots']} slots"
        )
    more = len(tasks) - len(lines)
    if more > 0:
        lines.append(f"...and {more} more")
    return "\n".join(lines)


# Matches Reddit post/comment URLs embedded in free text
_REDDIT_URL_RE = re.compile(
    r"https?://(?:www\.|old\.|np\.|m\.)?reddit\.com/r/[A-Za-z0-9_]+/comments/[A-Za-z0-9]+[^\s\"'<>]*",
    re.IGNORECASE,
)


def extract_reddit_url(text):
    """Return first Reddit URL found in text, or the original text if none found."""
    m = _REDDIT_URL_RE.search(text)
    return m.group(0) if m else text.strip()


def comment_matches_task(comment_details, claim):
    """
    Validation only checks subreddit and post_id.
    Title slug, comment_id, and tracking params are all ignored.
    """
    submitted_sub = comment_details["subreddit"]
    submitted_post = comment_details["post_id"]
    expected_sub = claim["subreddit"].lower()
    expected_post = claim["post_id"].lower()
    match = submitted_sub == expected_sub and submitted_post == expected_post
    if not match:
        logging.warning(
            "Proof mismatch: submitted sub=%s post=%s | expected sub=%s post=%s",
            submitted_sub, submitted_post, expected_sub, expected_post,
        )
    return match


def valid_upi_id(upi_id):
    return re.match(r"^[a-zA-Z0-9.\-_]{2,}@[a-zA-Z]{2,}$", upi_id.strip()) is not None


def total_amount(payments, paid_only=False):
    total = Decimal("0")
    currency = ""
    rows = [p for p in payments if not paid_only or p["status"] == "paid"]
    for payment in rows:
        amount = payment["amount"].strip()
        # Match both integer and decimal amounts
        match = re.search(r"([0-9]+(?:\.[0-9]+)?)", amount)
        if not match:
            continue
        
        # If currency not yet identified, try to extract it from the surrounding text
        if not currency:
            prefix = amount[:match.start()].strip()
            suffix = amount[match.end():].strip()
            currency = prefix or suffix

        try:
            total += Decimal(match.group(1))
        except InvalidOperation:
            continue
            
    # Format total: strip .00 if it's an integer
    total_text = str(int(total)) if total == total.to_integral() else f"{total:.2f}".rstrip('0').rstrip('.')
    
    if not currency:
        return total_text
    
    # Try to maintain the original position of the currency symbol if possible
    # (defaulting to prefix if we can't tell)
    return f"{currency}{total_text}"


def cooldown_left(last_claim_at):
    """Return remaining claim cooldown seconds."""
    if not last_claim_at:
        return 0

    try:
        claimed_at = datetime.fromisoformat(last_claim_at.replace(" ", "T")).replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        return 0

    elapsed = (datetime.now(timezone.utc) - claimed_at).total_seconds()
    return max(0, int(CLAIM_COOLDOWN_SECONDS - elapsed))


async def show_home(message):
    if not await require_real_user(message):
        return
    user_id = message.from_user.id
    username = message.from_user.username
    is_new = await register_user(user_id, username)
    
    if is_new and not await is_admin(user_id):
        text = (
            "👋 **Welcome to VIRON!**\n\n"
            "Earn payouts by completing Reddit engagement tasks.\n\n"
            "1️⃣ **Register:** Add Reddit accounts under 👤 **Manage Reddit Accounts**.\n"
            "2️⃣ **Claim:** Get an assigned comment via 📋 **Claim Task**.\n"
            "3️⃣ **Post & Submit:** Post on Reddit and send us the link.\n"
            "4️⃣ **Earn:** Get paid after review!\n\n"
            "💡 **Tip:** Add your UPI ID/QR in 💸 **Payment Info** early.\n\n"
            "👇 Choose an option below:"
        )
    else:
        text = (
            "🔥 **VIRON Reddit Tasks**\n\n"
            "Complete tasks, earn rewards. Ready for the next one?\n\n"
            "👇 Choose an option below:"
        )

    await message.answer(
        text,
        reply_markup=member_inline_menu(),
        parse_mode=None
    )


@dp.message(Command("start"))
async def start(message: Message):
    if not await require_real_user(message):
        return
    log_handler_match(message, "start_handler")
    clear_user_state(message.from_user.id, "start")
    await show_home(message)


@dp.message(Command("newtask"))
@dp.message(button_filter(BTN_TASKS_NEW))
async def start_new_task(message: Message):
    log_button_click(message, BTN_TASKS_NEW)
    if not await is_admin(message.from_user.id):
        await message.answer("Only admins can create tasks.")
        return
    set_user_state(message.from_user.id, {"flow": "new_task", "step": "post_url"})
    await message.answer("Send the Reddit POST URL.", reply_markup=back_cancel_keyboard())


def category_keyboard():
    rows = []
    for i in range(0, len(CATEGORIES), 2):
        pair = CATEGORIES[i:i+2]
        rows.append([InlineKeyboardButton(text=c, callback_data=f"task:category:{c}") for c in pair])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def priority_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔴 Urgent", callback_data="task:priority:urgent"),
         InlineKeyboardButton(text="🟠 High", callback_data="task:priority:high")],
        [InlineKeyboardButton(text="🟡 Normal", callback_data="task:priority:normal"),
         InlineKeyboardButton(text="🔵 Low", callback_data="task:priority:low")],
    ])


@dp.callback_query(F.data.startswith("task:category:"))
async def choose_category(callback: CallbackQuery):
    log_callback_click(callback, "choose_category")
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True)
        return
    state = user_states.get(callback.from_user.id)
    if not state or state.get("flow") != "new_task" or state.get("step") != "category":
        await callback.answer("No task setup in progress.", show_alert=True)
        return
    category = callback.data.split(":")[2]
    state["category"] = category
    state["min_level"] = "Beginner"  # member levels are not surfaced to users; default open access
    state["previous_step"] = "category"
    state["step"] = "priority"
    logging.info(
        "Task creation advanced: admin=%s category=%s step=priority",
        callback.from_user.id,
        category,
    )
    await callback.message.answer(
        f"Category: {category}\n\nChoose task priority:",
        reply_markup=priority_keyboard(),
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("task:priority:"))
async def handle_priority(callback: CallbackQuery):
    log_callback_click(callback, "handle_priority")
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True)
        return
    state = user_states.get(callback.from_user.id)
    if not state or state.get("step") != "priority":
        await callback.answer("Wrong step.", show_alert=True)
        return
    priority = callback.data.split(":")[2]
    state["priority"] = priority
    state["previous_step"] = "priority"
    state["step"] = "instructions"
    logging.info(
        "Task creation advanced: admin=%s priority=%s step=instructions",
        callback.from_user.id,
        priority,
    )
    await callback.message.answer(
        f"Priority: {priority.title()}\n\n"
        "Send task instructions for members (or /skip to leave blank).",
        reply_markup=back_cancel_keyboard(),
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("task:duplicate:"))
async def handle_duplicate_task_confirm(callback: CallbackQuery):
    log_callback_click(callback, "handle_duplicate_task_confirm")
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True)
        return

    state = user_states.get(callback.from_user.id)
    if not state or state.get("flow") != "new_task" or state.get("step") != "duplicate_confirm":
        await callback.answer("No duplicate task confirmation is active.", show_alert=True)
        return

    action = callback.data.split(":")[2]
    if action == "confirm":
        state["allow_duplicate"] = True
        state["previous_step"] = "post_url"
        state["step"] = "payout"
        await callback.message.edit_text("✅ Confirmed. Creating another task for the same Reddit post.")
        await callback.message.answer("Send payout amount. Example: ₹10", reply_markup=back_cancel_keyboard())
        await callback.answer()
        return

    if action == "manage":
        clear_user_state(callback.from_user.id, "duplicate_manage_existing")
        await callback.answer()
        await manage_tasks_paged(callback, page=1)
        return

    if action == "cancel":
        clear_user_state(callback.from_user.id, "duplicate_cancel")
        await callback.message.edit_text("❌ Task creation cancelled.")
        await callback.message.answer(
            "🔥 **VIRON Reddit Tasks**\n\n"
            "Complete tasks, earn rewards. Ready for the next one?\n\n"
            "👇 Choose an option below:",
            reply_markup=await main_menu(callback.from_user.id),
            parse_mode=None,
        )
        await callback.answer()
        return

    await callback.answer("Unknown action.", show_alert=True)


@dp.message(Command("addcomment"))
async def add_comment_command(message: Message, command: CommandObject):
    if command.args:
        await save_comments_from_text(message, command.args)
        return
    await start_add_comments(message)


@dp.message(button_filter(BTN_TASKS_ADD_COMMENTS))
async def start_add_comments(message: Message):
    log_button_click(message, BTN_TASKS_ADD_COMMENTS)
    if not await is_admin(message.from_user.id):
        await message.answer("Only admins can add comments.")
        return
    set_user_state(message.from_user.id, {
        "flow": "add_comments", "step": "task_id",
        "session_id": _addc_new_session_id(),
        "buffer": [],
    })
    await message.answer("Send the task ID.")


@dp.message(Command("done"))
async def done_command(message: Message):
    """Finalize the comment upload buffer. Reliable entry point that does NOT
    depend on the catch-all message router."""
    if not await is_admin(message.from_user.id):
        await message.answer("Only admins can use /done.")
        return
    log_handler_match(message, "done_command")
    state = user_states.get(message.from_user.id)
    _addc_log("cmd_done_entry", state, admin=message.from_user.id)
    if not state or state.get("flow") != "add_comments":
        await message.answer(
            "⚠ No active comment upload session.\n\n"
            "Tap 📝 Add Comments to start a new one.",
        )
        return
    if state.get("step") != "comments":
        await message.answer(
            "⚠ Not in upload step. Use the buttons above to ✅ Confirm or ❌ Cancel.",
        )
        return
    if not state.get("buffer"):
        await message.answer("Buffer is empty — send your comments first, then /done.")
        return
    await _addc_show_preview(message, state)


@dp.message(Command("process"))
async def process_command(message: Message):
    """Alias for /done. Same finalize path."""
    await done_command(message)


@dp.message(Command("closetask"))
async def close_task_command(message: Message, command: CommandObject):
    if not await is_admin(message.from_user.id):
        await message.answer("Only admins can close tasks.")
        return
    if command.args:
        match = re.search(r'\d+', command.args)
        if match:
            await close_task_by_id(message, int(match.group()))
            return
    set_user_state(message.from_user.id, {"flow": "task_status", "status": "closed"})
    await message.answer("Send task ID to close.")


@dp.message(button_filter(BTN_TASKS_MANAGE))
async def manage_tasks_button(message: Message):
    log_button_click(message, BTN_TASKS_MANAGE)
    if not await is_admin(message.from_user.id):
        await message.answer("Only admins can manage tasks.")
        return
    await manage_tasks_paged(message, page=1)


async def manage_tasks_paged(message_or_callback, page=1):
    """Show one task at a time with manage actions — paginated to avoid chat flooding."""
    rows = await get_active_tasks()
    if not rows:
        text = "No tasks available to manage."
        if isinstance(message_or_callback, CallbackQuery):
            await message_or_callback.message.edit_text(text)
        else:
            await message_or_callback.answer(text)
        return

    total = len(rows)
    if page > total: page = total
    if page < 1: page = 1
    task = rows[page - 1]
    
    # Detailed analytics for this task
    analytics = await get_comment_analytics(task["id"])
    avail = analytics.get("available", 0)
    reusable = analytics.get("reusable", 0)
    claimed = analytics.get("claimed", 0)
    submitted = analytics.get("submitted", 0)
    approved = analytics.get("approved", 0)

    claims = task["claims"] or 0
    slots_left = max(0, task["total_slots"] - claims)
    status_icons = {
        "active": "🟢", "paused": "⏸", "under_review": "🟡",
        "full": "🔴", "closed": "🔒", "archived": "🗃",
    }
    icon = status_icons.get(task["status"], "❓")

    text = (
        f"🔒 **Manage Task {page}/{total}**\n\n"
        f"Task #{task['id']} {icon} {task['status'].title()}\n"
        f"r/{escape_markdown(task['subreddit'])} | {task['category']}\n"
        f"💸 {task['payout_amount']} | Slots: {claims}/{task['total_slots']}\n\n"
        f"📊 **Comment Pool:**\n"
        f"✅ Available: {avail}\n"
        f"♻ Reusable: {reusable}\n"
        f"🕒 Claimed: {claimed}\n"
        f"🧾 Submitted: {submitted}\n"
        f"💎 Approved: {approved}\n\n"
        f"💡 {slots_left} slot(s) remaining."
    )
    keyboard = [
        [InlineKeyboardButton(text="📝 Add Comments", callback_data=f"task:addc:{task['id']}")],
        [InlineKeyboardButton(text="❌ Close", callback_data=f"task:status:closed:{task['id']}"),
         InlineKeyboardButton(text="⏸ Pause", callback_data=f"task:status:paused:{task['id']}"),
         InlineKeyboardButton(text="🔓 Reopen", callback_data=f"task:op:reopen_prompt:{task['id']}")],
        [InlineKeyboardButton(text="🧹 Reset Pool", callback_data=f"task:op:reset:{task['id']}"),
         InlineKeyboardButton(text="📦 Clone Task", callback_data=f"task:op:clone_prompt:{task['id']}")],
        [InlineKeyboardButton(text="🟡 Review Mode", callback_data=f"task:status:under_review:{task['id']}"),
         InlineKeyboardButton(text="🗑 Archive", callback_data=f"task:status:archived:{task['id']}")],
    ]
    keyboard.extend(get_pagination_keyboard(page, total, "manage"))
    markup = InlineKeyboardMarkup(inline_keyboard=keyboard)

    if isinstance(message_or_callback, CallbackQuery):
        # Use edit_text if it's a callback, but handle potential "message is not modified" error
        try:
            await message_or_callback.message.edit_text(text, reply_markup=markup, parse_mode=None)
        except Exception as e:
            if "message is not modified" not in str(e).lower():
                logging.error("Failed to edit task management message: %s", e)
    else:
        await message_or_callback.answer(text, reply_markup=markup, parse_mode=None)


@dp.message(button_filter(BTN_ANALYTICS_SYSTEM))
async def system_stats_handler(message: Message):
    if not await is_admin(message.from_user.id):
        return
    stats = await get_system_stats()
    text = (
        "📊 **SYSTEM STATS**\n\n"
        f"👥 **Total Members:** {stats['total_members']}\n"
        f"📌 **Active Tasks:** {stats['active_tasks']}\n"
        f"📤 **Pending Reviews:** {stats['pending_reviews']}\n"
        f"💸 **Pending Payments:** ₹{stats['pending_payouts_sum']:.2f}\n"
        f"🟢 **Verified Live:** {stats.get('live_count', 0)}\n"
        f"🔴 **Removed/Dead:** {stats.get('removed_count', 0)}\n\n"
        f"📂 **Total Tasks:** {stats['total_tasks']}\n"
        f"📤 **Total Submissions:** {stats['total_submissions']}\n"
        f"💰 **Total Paid:** ₹{stats['total_payouts']:.2f}"
    )
    await message.answer(text, parse_mode=None)


def admin_settings_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text=BTN_SETTINGS_CLEANUP), KeyboardButton(text=BTN_SETTINGS_BACKUP)],
            [KeyboardButton(text=BTN_BACK), KeyboardButton(text=BTN_HOME)],
        ],
        resize_keyboard=True,
    )


@dp.message(button_filter(BTN_ADMIN_SETTINGS))
async def admin_settings_handler(message: Message):
    if not await is_admin(message.from_user.id):
        return
    maintenance = await get_setting("maintenance_mode", "0")
    force_reuse = await get_setting("force_reuse_comments", "0")
    maint_label = "🔴 Disable Maintenance Mode" if maintenance == "1" else "🟢 Enable Maintenance Mode"
    reuse_label = "🔴 Disable Force Reuse" if force_reuse == "1" else "🟢 Enable Force Reuse"
    status_text = "🔴 MAINTENANCE MODE ON" if maintenance == "1" else "🟢 System Online"
    claim_timeout = await get_setting("claim_timeout_minutes", "30")
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=maint_label, callback_data="admin:config:maintenance")],
        [InlineKeyboardButton(text=reuse_label, callback_data="admin:config:force_reuse")],
        [InlineKeyboardButton(text=f"⏰ Claim Timeout: {claim_timeout} min", callback_data="admin:config:timeout")],
    ])
    await message.answer(
        f"⚙ ADMIN SETTINGS\n\n"
        f"Status: {status_text}\n"
        f"Claim Timeout: {claim_timeout} min\n"
        f"Duplicate Protection: ✅ Always on\n\n"
        "Tap a setting to change it:",
        reply_markup=keyboard,
    )
    await message.answer(
        "Operational tools:",
        reply_markup=admin_settings_keyboard(),
    )


@dp.callback_query(F.data == "admin:config:timeout")
async def config_timeout(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True)
        return
    set_user_state(callback.from_user.id, {"flow": "set_claim_timeout"})
    await callback.message.answer("Send new claim timeout in minutes (e.g. 30):")
    await callback.answer()


@dp.callback_query(F.data == "admin:config:force_reuse")
async def toggle_force_reuse(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True)
        return
    current = await get_setting("force_reuse_comments", "0")
    new_val = "0" if current == "1" else "1"
    await set_setting("force_reuse_comments", new_val)
    state_text = "ENABLED" if new_val == "1" else "DISABLED"
    logging.info("Force reuse comments %s by admin=%s", state_text, callback.from_user.id)
    await log_audit_action(callback.from_user.id, "force_reuse_comments", f"Set to {state_text}")
    await callback.answer(f"Force reuse {state_text}.", show_alert=True)
    await admin_settings_handler(callback.message) # Refresh


@dp.callback_query(F.data == "admin:config:maintenance")
async def toggle_maintenance(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True)
        return
    current = await get_setting("maintenance_mode", "0")
    new_val = "0" if current == "1" else "1"
    await set_setting("maintenance_mode", new_val)
    state_text = "ENABLED" if new_val == "1" else "DISABLED"
    logging.info("Maintenance mode %s by admin=%s", state_text, callback.from_user.id)
    await log_audit_action(callback.from_user.id, "maintenance_mode", f"Set to {state_text}")
    await callback.answer(f"Maintenance mode {state_text}.", show_alert=True)
    await callback.message.edit_text(
        f"⚙ Maintenance mode is now {'🔴 ON' if new_val == '1' else '🟢 OFF'}."
    )


# ─── Cleanup / archive ──────────────────────────────────────────────────────

@dp.message(button_filter(BTN_SETTINGS_CLEANUP))
async def cleanup_button(message: Message):
    log_button_click(message, BTN_SETTINGS_CLEANUP)
    if not await is_admin(message.from_user.id):
        return
    counts = await count_archivable_records(min_age_days=7)
    total = counts["tasks"] + counts["payments"] + counts["submissions"]
    if total == 0:
        await message.answer(
            "🧹 **Nothing to archive right now.**\n\n"
            "Cleanup archives completed tasks and reviewed records older than 7 days.",
            parse_mode=None
        )
        return
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🗑 Confirm Cleanup", callback_data="cleanup:confirm"),
            InlineKeyboardButton(text="❌ Cancel", callback_data="cleanup:cancel"),
        ],
    ])
    await message.answer(
        "🧹 **DATABASE CLEANUP**\n\n"
        "This will archive the following records older than 7 days:\n\n"
        f"• **{counts['tasks']}** Completed task(s)\n"
        f"• **{counts['payments']}** Paid/Failed payment(s)\n"
        f"• **{counts['submissions']}** Reviewed submission(s)\n\n"
        "💡 **Note:** Records are not deleted; they are moved to the internal archive flag (archived=1) to keep the active system fast.",
        reply_markup=keyboard,
        parse_mode=None
    )


@dp.callback_query(F.data == "cleanup:confirm")
async def cleanup_confirm_callback(callback: CallbackQuery):
    log_callback_click(callback, "cleanup_confirm")
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True)
        return
    archived = await archive_completed_records(min_age_days=7)
    await log_audit_action(
        callback.from_user.id,
        "cleanup_archive",
        f"tasks={archived['tasks']} payments={archived['payments']} submissions={archived['submissions']}",
    )
    logging.info(
        "Cleanup archive: admin=%s tasks=%s payments=%s submissions=%s",
        callback.from_user.id,
        archived["tasks"], archived["payments"], archived["submissions"],
    )
    await callback.message.edit_text(
        "✅ **Cleanup Complete!**\n\n"
        "The following records have been archived:\n\n"
        f"• **{archived['tasks']}** Tasks\n"
        f"• **{archived['payments']}** Payments\n"
        f"• **{archived['submissions']}** Submissions\n\n"
        "All active queues are now optimized for speed.",
        parse_mode=None
    )
    await callback.answer("Cleanup done.")


@dp.callback_query(F.data == "cleanup:cancel")
async def cleanup_cancel_callback(callback: CallbackQuery):
    log_callback_click(callback, "cleanup_cancel")
    await callback.message.edit_text("❌ Cleanup cancelled.")
    await callback.answer("Cancelled.")


# ─── Database backup ────────────────────────────────────────────────────────

@dp.message(button_filter(BTN_SETTINGS_BACKUP))
async def backup_button(message: Message, bot: Bot):
    log_button_click(message, BTN_SETTINGS_BACKUP)
    if not await is_admin(message.from_user.id):
        return
    
    import shutil
    ts_file = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    ts_label = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    backup_filename = f"viron_backup_{ts_file}.db"
    
    try:
        # Create a copy to avoid locking issues during transit
        shutil.copy2(DB_NAME, backup_filename)
        
        size_kb = os.path.getsize(backup_filename) / 1024
        db_file = FSInputFile(backup_filename, filename=backup_filename)
        
        await message.answer_document(
            db_file, 
            caption=(
                "📦 **DATABASE BACKUP**\n\n"
                f"**Generated:** {ts_label}\n"
                f"**Size:** {size_kb:.1f} KB\n\n"
                "This file contains the complete system state including users, tasks, and analytics."
            ),
            parse_mode=None
        )
        
        await log_audit_action(message.from_user.id, "backup_download", f"size_kb={size_kb:.1f}")
        logging.info("Backup generated: admin=%s size_kb=%.1f", message.from_user.id, size_kb)
        
        # Clean up the temporary backup file
        if os.path.exists(backup_filename):
            os.remove(backup_filename)
            
    except Exception as e:
        logging.exception("Backup failed: admin=%s", message.from_user.id)
        await message.answer(f"❌ **Backup Failed:** {str(e)}", parse_mode=None)


@dp.callback_query(F.data.startswith("task:op:"))
async def handle_task_op(callback: CallbackQuery):
    log_callback_click(callback, "handle_task_op")
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True)
        return
        
    parts = callback.data.split(":")
    op = parts[2]
    task_id = int(parts[3])
    
    if op == "reopen_prompt":
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ YES (Reuse comments)", callback_data=f"task:op:reopen_yes:{task_id}")],
            [InlineKeyboardButton(text="❌ NO (New comments only)", callback_data=f"task:op:reopen_no:{task_id}")],
            [InlineKeyboardButton(text="⬅ Back", callback_data=f"manage:page:1")] # Simplified back
        ])
        await callback.message.edit_text(
            f"♻ **Reopen Task #{task_id}?**\n\n"
            "Do you want to reuse existing comments that weren't already approved?",
            reply_markup=keyboard,
            parse_mode=None
        )
    
    elif op == "reopen_yes":
        await reopen_task_with_comments(task_id, reuse_comments=True)
        await callback.answer("Task reopened with reusable comments.")
        await manage_tasks_paged(callback, page=1) # Refresh view
        
    elif op == "reopen_no":
        await reopen_task_with_comments(task_id, reuse_comments=False)
        await callback.answer("Task reopened. You must add new comments.")
        await manage_tasks_paged(callback, page=1)
        
    elif op == "reset":
        await reset_used_comments(task_id)
        await callback.answer("Comment pool reset to available.")
        await manage_tasks_paged(callback, page=1)
        
    elif op == "clone_prompt":
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ YES (Clone with comments)", callback_data=f"task:op:clone_yes:{task_id}")],
            [InlineKeyboardButton(text="❌ NO (Settings only)", callback_data=f"task:op:clone_no:{task_id}")],
            [InlineKeyboardButton(text="⬅ Back", callback_data=f"manage:page:1")]
        ])
        await callback.message.edit_text(
            f"📦 **Clone Task #{task_id}?**\n\n"
            "This will create a new task with identical settings.\n"
            "Do you want to copy the comment pool as well?",
            reply_markup=keyboard,
            parse_mode=None
        )
        
    elif op == "clone_yes":
        new_id = await clone_task(task_id, reuse_comments=True)
        await callback.answer(f"Task cloned! New Task ID: #{new_id}")
        await manage_tasks_paged(callback, page=1)
        
    elif op == "clone_no":
        new_id = await clone_task(task_id, reuse_comments=False)
        await callback.answer(f"Task cloned! New Task ID: #{new_id}")
        await manage_tasks_paged(callback, page=1)
    
    await callback.answer()


@dp.callback_query(F.data.startswith("task:status:"))
async def handle_setstatus(callback: CallbackQuery):
    log_callback_click(callback, "handle_setstatus")
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True)
        return
    parts = callback.data.split(":")
    status = parts[2]
    task_id = int(parts[3])
    
    if status == "archived":
        await archive_task(task_id)
        logging.info("Task archived: task=%s admin=%s", task_id, callback.from_user.id)
        await callback.message.edit_text(f"🗑 Task #{task_id} archived and moved to history.")
        await callback.answer("Archived.")
        return

    ok = await update_task_status(task_id, status)
    if ok:
        logging.info("Task status changed: task=%s status=%s admin=%s", task_id, status, callback.from_user.id)
        await callback.message.edit_text(f"Task #{task_id} status set to {status}.")
    else:
        await callback.message.edit_text("Task not found.")
    await callback.answer(f"Status set to {status}.")


@dp.message(Command("taskstats"))
@dp.message(button_filter(BTN_TASKS_STATS))
async def task_stats(message: Message, command: CommandObject = None):
    log_button_click(message, BTN_TASKS_STATS)
    clear_user_state(message.from_user.id, "task_stats")
    if not await is_admin(message.from_user.id):
        await message.answer("Only admins can view task stats.")
        return
    task_id = None
    if command and command.args:
        if not command.args.strip().isdigit():
            await message.answer("Use /taskstats or /taskstats task_id")
            return
        task_id = int(command.args.strip())
    await send_task_stats(message, task_id)


async def cleanup_and_notify(bot: Bot):
    """Release expired claims and notify users. Reads timeout from DB settings."""
    expired, timeout_minutes = await auto_cleanup_claims()
    for claim in expired:
        try:
            await bot.send_message(
                claim["assigned_to"],
                f"⚠ **Claim Expired**\n\nYour claim for Task #{claim['task_id']} has expired because no proof was submitted within {timeout_minutes} minutes. The comment has been released back to the pool."
            )
        except Exception:
            logging.warning("Failed to notify user %s about expired claim", claim["assigned_to"])


@dp.message(button_filter(BTN_TASKS_ACTIVE))
async def active_tasks_handler(message: Message):
    log_handler_match(message, "active_tasks_handler")
    await active_tasks(message, page=1)


async def active_tasks(message_or_callback, page=1):
    bot = (
        message_or_callback.bot
        if isinstance(message_or_callback, Message)
        else message_or_callback.message.bot
    )
    user_id = message_or_callback.from_user.id
    await cleanup_and_notify(bot)
    is_user_admin = await is_admin(user_id)

    rows = await get_active_tasks()
    if not rows:
        text = "No active tasks right now. Check back soon."
        if isinstance(message_or_callback, CallbackQuery):
            await message_or_callback.message.edit_text(text)
        else:
            await message_or_callback.answer(text)
        return

    total = len(rows)
    if page > total: page = total
    if page < 1: page = 1
    task = rows[page - 1]

    claims = task["claims"] or 0
    slots_left = max(0, task["total_slots"] - claims)
    total_comments = task["total_comments"] or 0
    status = task["status"]

    status_icons = {
        "active": "🟢 Active",
        "paused": "⏸ Paused",
        "under_review": "🟡 Under Review",
        "full": "🔴 Full",
    }
    status_label = status_icons.get(status, status)

    if is_user_admin:
        comments_warning = ""
        if total_comments < task["total_slots"] and status != "full":
            comments_warning = f"\n⚠ Only {total_comments} comment(s) added — add more to fill all slots."

        text = (
            f"📂 **Active Tasks ({page}/{total})**\n\n"
            f"Task #{task['id']} r/{escape_markdown(task['subreddit'])}\n"
            f"Status: {status_label}\n\n"
            f"📝 Category: {task['category']}\n"
            f"💸 Payout: {task['payout_amount']}\n"
            f"📦 Slots: {claims}/{task['total_slots']} used ({slots_left} left)\n"
            f"💬 Comments Added: {total_comments}\n"
            f"📤 Submissions: {task['submissions'] or 0}"
            f"{comments_warning}"
        )
        keyboard = [
            [InlineKeyboardButton(text=f"🔒 Manage #{task['id']}", callback_data=f"task:manage_single:{task['id']}")]
        ]
    else:
        # Check eligibility for member
        eligible, reason = await get_task_eligibility(user_id, task["id"])
        
        status_text = ""
        if not eligible:
            status_text = f"\n\n⚠️ **Ineligible:** {reason}"
        elif slots_left <= 0:
            status_text = f"\n\n🔴 **Task is Full**"
        else:
            status_text = f"\n\n✅ **Available to claim!**"

        text = (
            f"📂 **Active Tasks ({page}/{total})**\n\n"
            f"Task #{task['id']} - r/{escape_markdown(task['subreddit'])}\n"
            f"📝 Category: {escape_markdown(task['category'])}\n"
            f"💸 Payout: {escape_markdown(task['payout_amount'])}\n"
            f"📦 Slots left: {slots_left}"
            f"{status_text}\n\n"
            "Tap 📋 Claim Task from the menu to receive an assignment."
        )
        keyboard = []

    keyboard.extend(get_pagination_keyboard(page, total, "task"))
    markup = InlineKeyboardMarkup(inline_keyboard=keyboard) if keyboard else None

    if isinstance(message_or_callback, CallbackQuery):
        await message_or_callback.message.edit_text(text, reply_markup=markup, parse_mode=None)
    else:
        await message_or_callback.answer(text, reply_markup=markup, parse_mode=None)


@dp.callback_query(F.data.startswith("task:page:"))
async def task_page_callback(callback: CallbackQuery):
    log_callback_click(callback, "task_page")
    page = int(callback.data.split(":")[2])
    await active_tasks(callback, page)
    await callback.answer()


@dp.callback_query(F.data.startswith("task:manage_single:"))
async def manage_single_task(callback: CallbackQuery):
    log_callback_click(callback, "manage_single_task")
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True)
        return
    task_id = int(callback.data.split(":")[2])
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📝 Add Comments", callback_data=f"task:addc:{task_id}")],
        [InlineKeyboardButton(text="❌ Close", callback_data=f"task:status:closed:{task_id}"),
         InlineKeyboardButton(text="⏸ Pause", callback_data=f"task:status:paused:{task_id}"),
         InlineKeyboardButton(text="🔓 Reopen", callback_data=f"task:status:active:{task_id}")],
        [InlineKeyboardButton(text="🟡 Review Mode", callback_data=f"task:status:under_review:{task_id}"),
         InlineKeyboardButton(text="🗑 Archive", callback_data=f"task:status:archived:{task_id}")],
        [InlineKeyboardButton(text="⬅️ Back to List", callback_data="manage:page:1")],
    ])
    await callback.message.edit_text(f"🔒 Managing Task #{task_id}", reply_markup=keyboard)
    await callback.answer()


@dp.callback_query(F.data.startswith("task:addc:"))
async def task_addc_callback(callback: CallbackQuery):
    """Quick-add comments to a specific task without re-asking for task ID."""
    log_callback_click(callback, "task_addc")
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True)
        return
    try:
        task_id = int(callback.data.split(":")[2])
    except (IndexError, ValueError):
        await callback.answer("Bad task id.", show_alert=True); return
    set_user_state(callback.from_user.id, {
        "flow": "add_comments",
        "step": "comments",
        "task_id": task_id,
        "buffer": [],
        "session_id": _addc_new_session_id(),
    })
    await callback.message.answer(
        f"📝 Adding comments to Task #{task_id}.\n\n" + _ADD_COMMENTS_INSTRUCTIONS,
        reply_markup=back_cancel_keyboard(),
    )
    await callback.answer("Ready — paste your comments.")


@dp.callback_query(F.data.startswith("manage:page:"))
async def manage_page_callback(callback: CallbackQuery):
    log_callback_click(callback, "manage_page")
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True)
        return
    page = int(callback.data.split(":")[2])
    await manage_tasks_paged(callback, page)
    await callback.answer()


async def send_task_stats(message, task_id=None):
    rows = await get_task_stats(task_id)
    if not rows:
        await message.answer("No tasks found.")
        return
    stats = await get_total_stats()
    lines = [
        "📈 TASK STATS\n",
        f"Active tasks: {stats['active_tasks']}",
        f"Claims: {stats['claims']}",
        f"Submissions: {stats['submissions']}",
        f"Pending reviews: {stats['pending_reviews']}",
        f"Pending payments: {stats['pending_payments']}",
        f"Completed payments: {stats['completed_payments']}",
    ]
    for task in rows[:8]:
        claims = task["claims"] or 0
        slots_left = max(0, task["total_slots"] - claims)
        lines.append(
            f"\nTask #{task['id']} [{task['status']}]\n"
            f"r/{task['subreddit']} | {task['category']}\n"
            f"Slots: {claims}/{task['total_slots']} used ({slots_left} left)\n"
            f"Comments: {task['total_comments'] or 0} added\n"
            f"Submissions: {task['submissions'] or 0}\n"
            f"Pending reviews: {task['pending_reviews'] or 0}\n"
            f"Pending payments: {task['pending_payments'] or 0}"
        )
    await message.answer("\n".join(lines))


async def claim_for_user(target, bot, user_id, username):
    clear_user_state(user_id, "claim")
    if await get_setting("maintenance_mode", "0") == "1":
        await target.answer(
            "⚠ System temporarily under maintenance.\n\nPlease try again later."
        )
        return
    await register_user(user_id, username)
    await cleanup_and_notify(bot)

    user = await get_user(user_id)
    if user and user["is_banned"]:
        await target.answer("🚫 Your account has been restricted.\n\nContact admin if this is a mistake.")
        return

    is_user_admin = await is_admin(user_id)
    force_reuse_setting = await get_setting("force_reuse_comments", "0")
    force_reuse = (force_reuse_setting == "1" and is_user_admin)

    remaining = cooldown_left(user["last_claim_at"] if user else None)
    if remaining > 0:
        await target.answer(f"⏳ Please wait {remaining} seconds before claiming another task.")
        return

    claim_data = await claim_comment(user_id, force_reuse=force_reuse)
    if claim_data == "banned":
        await target.answer("🚫 Your account has been restricted.\n\nContact admin if this is a mistake.")
        return
    if not claim_data:
        await target.answer("No active tasks are available right now.")
        return

    # At-limit: user already holds the maximum simultaneous claims they are
    # allowed (based on their approved Reddit accounts + admin cap). Show the
    # outstanding claims so they know what to finish first.
    if claim_data.get("at_limit"):
        active = claim_data.get("active_claims") or []
        lines = [
            f"⚠ You're holding {claim_data['current']}/{claim_data['max']} active claim(s).",
            "Submit proof for an existing claim before taking on another.\n",
            "Your active claims:",
        ]
        for c in active:
            lines.append(
                f"• Task #{c['task_id']} · r/{c['subreddit']} · "
                f"{c['payout_amount']}"
            )
        await target.answer("\n".join(lines), parse_mode=None)
        return

    title = "📌 YOUR ACTIVE TASK" if claim_data["already_claimed"] else "🔥 TASK ASSIGNED"
    if not claim_data["already_claimed"]:
        logging.info(
            "Claim created: user=%s task=%s comment=%s",
            user_id, claim_data["task_id"], claim_data["comment_id"],
    )
    instructions = claim_data["instructions"] or "Post the comment exactly as written. Do not edit or paraphrase."
    timeout_min = int(await get_setting("claim_timeout_minutes", "30"))
    formatted = (
        f"{safe_html(title)}\n\n"
        f"📌 <b>Task ID:</b> #{claim_data['task_id']}\n"
        f"📍 <b>Subreddit:</b> r/{safe_html(claim_data['subreddit'])}\n\n"
        f"🔗 <b>Post Link:</b>\n{safe_html(claim_data['post_url'])}\n\n"
        f"💬 <b>Your Comment:</b>\n<code>{safe_html(claim_data['comment_text'])}</code>\n\n"
        f"💸 <b>Payout:</b> {safe_html(claim_data['payout_amount'])}\n"
        f"⏰ <b>Expires in:</b> {timeout_min} minutes\n\n"
        f"📋 <b>Instructions:</b>\n{safe_html(instructions)}\n\n"
        "⚠️ <b>Important:</b>\n"
        "• Post the comment exactly as shown.\n"
        "• After posting, tap 📤 Submit Proof or send /submit &lt;comment link&gt;.\n"
        "• Do not delete your comment."
    )
    plain = (
        f"{title}\n\n"
        f"Task ID: #{claim_data['task_id']}\n"
        f"Subreddit: r/{claim_data['subreddit']}\n\n"
        f"Post Link:\n{claim_data['post_url']}\n\n"
        f"Your Comment:\n{claim_data['comment_text']}\n\n"
        f"Payout: {claim_data['payout_amount']}\n"
        f"Expires in: {timeout_min} minutes\n\n"
        f"Instructions:\n{instructions}\n\n"
        "Important:\n"
        "• Post the comment exactly as shown.\n"
        "• After posting, tap Submit Proof or send /submit <comment link>.\n"
        "• Do not delete your comment."
    )
    formatted_success = await answer_with_fallback(
        target,
        formatted,
        plain,
        parse_mode="HTML",
    )
    logging.info(
        "[claim_task] user_id=%s task_id=%s comment_id=%s parse_mode=HTML formatted_success=%s preview=%r",
        user_id,
        claim_data["task_id"],
        claim_data["comment_id"],
        formatted_success,
        plain[:300],
    )


@dp.message(Command("claim"))
@dp.message(button_filter(BTN_CLAIM))
async def claim(message: Message):
    if not await require_real_user(message):
        return
    log_handler_match(message, "claim_handler")
    log_button_click(message, BTN_CLAIM)
    await claim_for_user(message, message.bot, message.from_user.id, message.from_user.username)


@dp.message(Command("submit"))
async def submit_command(message: Message, command: CommandObject):
    if not await require_real_user(message):
        return
    log_handler_match(message, "submit_command")
    if command.args:
        await handle_submit_link(message, command.args.strip())
        return
    await start_submit_flow(message)


@dp.message(button_filter(BTN_SUBMIT))
async def start_submit_flow(message: Message):
    if not await require_real_user(message):
        return
    log_handler_match(message, "start_submit_flow")
    log_button_click(message, BTN_SUBMIT)
    if await get_setting("maintenance_mode", "0") == "1":
        await message.answer(
            "⚠ System temporarily under maintenance.\n\nPlease try again later."
        )
        return
    await register_user(message.from_user.id, message.from_user.username)
    claim_data = await get_active_claim(message.from_user.id)
    if not claim_data:
        await message.answer("You do not have an active task. Use 📋 Claim Task first.")
        return
    set_user_state(message.from_user.id, {"flow": "submit_proof"})
    await message.answer(
        "📤 **Submit Your Proof**\n\n"
        "Please send the link to your Reddit **COMMENT** below.\n\n"
        "📍 **Example format:**\n"
        "`https://reddit.com/r/sub/comments/postid/comment/commentid/`\n\n"
        "💡 **Tip:** Tap 'Share' on your comment and 'Copy Link'.\n"
        "❌ **Note:** Do not send the post link itself.",
        parse_mode=None
    )


_SUCCESS_MSG = (
    "✅ **Submission Received!**\n\n"
    "Your proof has been successfully submitted and is now waiting for admin review. You'll be notified once it's approved!"
)

# Per-failure user-friendly rejection messages
_ERR_NOT_REDDIT = (
    "❌ **Invalid Link**\n\n"
    "That doesn't look like a valid Reddit comment link. Please make sure you've copied the link to your specific comment.\n\n"
    "💡 **How to get it:**\n"
    "1. Find your comment on Reddit\n"
    "2. Tap **Share** → **Copy Link**\n"
    "3. Paste it here"
)
_ERR_POST_LINK = (
    "❌ You sent the post link, not your comment link.\n\n"
    "We need the link to your specific comment, not the Reddit post itself.\n\n"
    "How to get it:\n"
    "1. Find your comment on the Reddit post\n"
    "2. Tap the three-dot menu (⋯) on your comment\n"
    "3. Tap Share → Copy Link\n"
    "4. Paste that link here"
)
_ERR_WRONG_POST = (
    "❌ Wrong post.\n\n"
    "Your comment link belongs to a different Reddit post than the one assigned to you.\n\n"
    "Make sure you are posting in the correct Reddit thread shown in your task."
)
_ERR_DUPLICATE_LINK = (
    "❌ This comment link was already submitted by someone else.\n\n"
    "Each comment link can only be used once across the platform."
)
_ERR_DUPLICATE_COMMENT_ID = (
    "❌ This Reddit comment was already submitted as proof.\n\n"
    "Each Reddit comment can only be used once. Please post a new comment and submit its link."
)
_ERR_ALREADY_SUBMITTED = (
    "❌ You already submitted proof for this task.\n\n"
    "You cannot submit proof for the same task twice."
)
_ERR_REDDIT_LOOKUP_FAILED = (
    "⚠ Could not verify the comment's author with Reddit.\n\n"
    "Possible reasons:\n"
    "• The comment was deleted or removed.\n"
    "• Reddit is rate-limiting us right now.\n\n"
    "Please make sure your comment is still public and try again in a minute."
)
_ERR_UNREGISTERED_ACCOUNT = (
    "⚠ This Reddit account is not registered to your profile.\n\n"
    "Add it via 👤 Manage Reddit Accounts → ➕ Add Reddit Account, then wait for admin approval.\n\n"
    "Only approved accounts can be used as proof."
)
_ERR_ACCOUNT_PENDING = (
    "⏳ That Reddit account is still pending admin approval.\n\n"
    "We'll notify you as soon as it's approved — you can submit again after that."
)
_ERR_ACCOUNT_DISABLED = (
    "🚫 This Reddit account has been disabled by an admin.\n\n"
    "Contact support if you believe this is a mistake."
)


def _pick_claim_for_url(active_claims, comment_details):
    """Choose the active claim that matches the submitted URL's subreddit+post_id."""
    sub = comment_details["subreddit"]
    post = comment_details["post_id"]
    for claim in active_claims:
        if claim["subreddit"].lower() == sub and claim["post_id"].lower() == post:
            return claim
    return None


async def handle_submit_link(message, raw_text):
    await register_user(message.from_user.id, message.from_user.username)
    bot = message.bot
    user_id = message.from_user.id
    username = message.from_user.username

    # Extract Reddit URL from message — user may paste extra context text around the link
    reddit_link = extract_reddit_url(raw_text)

    # Cooldown check
    user = await get_user(user_id)
    if user and user.get("last_submit_at"):
        try:
            last_submit = datetime.fromisoformat(
                user["last_submit_at"].replace(" ", "T")
            ).replace(tzinfo=timezone.utc)
            elapsed = (datetime.now(timezone.utc) - last_submit).total_seconds()
            if elapsed < SUBMIT_COOLDOWN_SECONDS:
                await message.answer(
                    f"⏳ Please wait {int(SUBMIT_COOLDOWN_SECONDS - elapsed)} seconds before submitting again."
                )
                return
        except ValueError:
            pass

    active_claims = await get_active_claims(user_id)
    if not active_claims:
        await message.answer("You do not have an active task. Use 📋 Claim Task first.")
        return

    # Parse the submitted URL
    comment_details = parse_reddit_url(reddit_link)
    if not comment_details:
        logging.warning(
            "Proof rejected - unparseable URL: user=%s raw=%r extracted=%r",
            user_id, raw_text, reddit_link,
        )
        await message.answer(_ERR_NOT_REDDIT)
        return

    # Require a comment-level URL (not just the post URL)
    if not comment_details.get("comment_id"):
        logging.warning(
            "Proof rejected - post-level URL (no comment_id): user=%s url=%r",
            user_id, reddit_link,
        )
        await message.answer(_ERR_POST_LINK)
        return

    # Find which of the user's active claims this URL belongs to
    claim_data = _pick_claim_for_url(active_claims, comment_details)
    if not claim_data:
        logging.warning(
            "Proof rejected - URL doesn't match any active claim: user=%s "
            "submitted(sub=%s post=%s) active=%s",
            user_id, comment_details["subreddit"], comment_details["post_id"],
            [(c["subreddit"], c["post_id"]) for c in active_claims],
        )
        await message.answer(_ERR_WRONG_POST)
        return

    reddit_cid = comment_details["comment_id"]

    # Duplicate submission guards — check all three angles
    if await submission_link_exists(comment_details["normalized_url"]):
        logging.warning(
            "Proof rejected - duplicate normalized link: user=%s task=%s url=%r",
            user_id, claim_data["task_id"], comment_details["normalized_url"],
        )
        await message.answer(_ERR_DUPLICATE_LINK)
        return
    if await reddit_comment_id_exists(reddit_cid):
        logging.warning(
            "Proof rejected - duplicate comment_id: user=%s task=%s cid=%s",
            user_id, claim_data["task_id"], reddit_cid,
        )
        await message.answer(_ERR_DUPLICATE_COMMENT_ID)
        return
    if await submission_exists_for_comment(claim_data["comment_id"]):
        logging.warning(
            "Proof rejected - slot already submitted: user=%s task=%s slot_comment_id=%s",
            user_id, claim_data["task_id"], claim_data["comment_id"],
        )
        await message.answer(_ERR_ALREADY_SUBMITTED)
        return

    # Resolve the Reddit comment author. This is what binds the submission to a
    # specific registered Reddit account.
    meta = await _fetch_reddit_comment_meta(reddit_cid)
    author = meta.get("author")
    if not author or meta.get("status") == "error":
        logging.warning(
            "Proof rejected - author lookup failed: user=%s cid=%s meta=%s",
            user_id, reddit_cid, meta,
        )
        await message.answer(_ERR_REDDIT_LOOKUP_FAILED)
        return

    # Look up the Reddit account row to verify ownership + status
    acct = await get_reddit_account_by_username(author)
    if not acct:
        logging.warning(
            "Proof rejected - unregistered Reddit author: user=%s author=%s",
            user_id, author,
        )
        await message.answer(_ERR_UNREGISTERED_ACCOUNT)
        return

    if acct["telegram_user_id"] != user_id:
        # Someone is trying to submit proof from a Reddit account that belongs to
        # a different Telegram user. Hard block + admin alert.
        logging.warning(
            "Proof rejected - cross-user Reddit author: user=%s author=%s owner=%s",
            user_id, author, acct["telegram_user_id"],
        )
        await message.answer(_ERR_UNREGISTERED_ACCOUNT)
        await _notify_admins(
            bot,
            "🚨 Cross-user submission attempt\n\n"
            f"Telegram user: {user_id} (@{username or 'no_handle'})\n"
            f"Submitted Reddit author: u/{author}\n"
            f"Registered owner: telegram_user_id={acct['telegram_user_id']}\n"
            f"Task: #{claim_data['task_id']}\n\n"
            "Please investigate for account-sharing or impersonation.",
        )
        return

    if acct["status"] == "pending":
        await message.answer(_ERR_ACCOUNT_PENDING)
        return
    if acct["status"] in ("disabled", "rejected"):
        await message.answer(_ERR_ACCOUNT_DISABLED)
        return
    if acct["status"] != "active":
        await message.answer(_ERR_UNREGISTERED_ACCOUNT)
        return

    submission_id = await save_submission(
        user_id,
        username,
        claim_data["task_id"],
        claim_data["comment_id"],
        claim_data["comment_text"],
        reddit_link,
        comment_details["normalized_url"],
        reddit_cid,
    )
    if submission_id is None:
        await message.answer(_ERR_ALREADY_SUBMITTED)
        return

    # Attach the resolved Reddit account to the submission for stats & dashboards
    try:
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute(
                "UPDATE submissions SET reddit_account_id = ?, reddit_author = ? WHERE id = ?",
                (acct["id"], author, submission_id),
            )
            await db.commit()
        await touch_reddit_account_last_used(acct["id"])
    except Exception:
        logging.exception(
            "Could not annotate submission with reddit_account: submission=%s acct=%s",
            submission_id, acct["id"],
        )

    clear_user_state(user_id, "submission_saved")
    logging.info(
        "Submission saved: id=%s user=%s task=%s comment_id=%s reddit_cid=%s "
        "reddit_author=%s account_id=%s",
        submission_id, user_id, claim_data["task_id"],
        claim_data["comment_id"], reddit_cid, author, acct["id"],
    )
    await message.answer(_SUCCESS_MSG, reply_markup=await main_menu(user_id))


@dp.message(button_filter(BTN_PAYMENTS_PENDING))
async def pending_payments_handler(message: Message, bot: Bot):
    log_handler_match(message, "pending_payments_handler")
    if not await is_admin(message.from_user.id):
        await message.answer("Only admins can view pending payments.")
        return
    await pending_payments(message, bot, page=1)


async def pending_payments(message_or_callback, bot: Bot, page=1):
    rows = await get_pending_payments()
    if not rows:
        text = "No pending payments."
        if isinstance(message_or_callback, CallbackQuery):
            await message_or_callback.message.edit_text(text)
        else:
            await message_or_callback.answer(text)
        return

    total = len(rows)
    if page > total: page = total
    if page < 1: page = 1
    payment = rows[page-1]

    keyboard = [[
        InlineKeyboardButton(text="✅ Mark Paid", callback_data=f"payment:confirm:{payment['id']}"),
    ]]
    keyboard.extend(get_pagination_keyboard(page, total, "payment"))
    markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
    caption = payment_caption(payment)
    qr_file_id = payment["qr_file_id"]

    if isinstance(message_or_callback, CallbackQuery):
        if qr_file_id:
            # Updating a message with photo usually requires edit_message_media
            # For simplicity, if it was a photo, we'll send a new one or just update caption if same photo
            # but usually it's different photos.
            # To keep it robust without complex media editing, we'll just send new message if media changes.
            try:
                # Try editing caption first
                await message_or_callback.message.edit_caption(caption=caption, reply_markup=markup)
                return
            except Exception:
                # If fail (e.g. no photo in message), send new
                pass
        else:
            await message_or_callback.message.edit_text(caption, reply_markup=markup)
            return

    # For initial message or if edit failed
    if qr_file_id:
        await bot.send_photo(
            chat_id=message_or_callback.from_user.id,
            photo=qr_file_id,
            caption=caption,
            reply_markup=markup,
        )
    else:
        await bot.send_message(
            chat_id=message_or_callback.from_user.id,
            text=caption,
            reply_markup=markup,
        )


@dp.callback_query(F.data.startswith("payment:page:"))
async def payment_page_callback(callback: CallbackQuery, bot: Bot):
    log_callback_click(callback, "payment_page")
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True)
        return
    page = int(callback.data.split(":")[2])
    await pending_payments(callback, bot, page)
    await callback.answer()


def payment_caption(payment):
    lines = [
        f"👤 @{payment['username'] or 'no_username'}",
        f"💸 {payment['amount']}",
        f"📌 Task #{payment['task_id']}",
        "",
        "💳 UPI:",
        payment["upi_id"] or "UPI not set",
    ]

    if not payment["qr_file_id"]:
        lines.append("")
        lines.append("🖼 No QR uploaded.")

    if not payment["upi_id"] and not payment["qr_file_id"]:
        lines.append("⚠ No payment method uploaded.")

    return "\n".join(lines)


# ─── Process Payments ────────────────────────────────────────────────────────
#
# A "process payments" session is stored in user_states under flow="process_payments":
#   queue          : list[dict] of remaining grouped-by-user payment bundles
#   paid_users     : int
#   paid_amounts   : list of paid payment rows (for total)
#   paid_total_text: cached display total
#   skipped        : int
#   live_total     : int  — sum of live comments across paid members
#   removed_total  : int  — sum of dead comments across paid members
#   send_msg_for   : int or None — when admin is composing a member message

_PROCESS_FLOW = "process_payments"
_SEND_MSG_FLOW = "process_send_message"


def _bundle_handle(bundle):
    return f"@{bundle['username']}" if bundle.get("username") else f"ID {bundle['user_id']}"


def _process_payments_card(bundle):
    """Per-member card per spec: member info, comment status, payment summary."""
    handle = _bundle_handle(bundle)
    age_h = bundle.get("oldest_pending_age_h")
    age_text = f"{age_h}h" if age_h is not None else "—"

    live = bundle.get("live_count", 0)
    removed = bundle.get("removed_count", 0)
    deleted = bundle.get("deleted_count", 0)
    shadow = bundle.get("shadow_removed_count", 0)
    unchecked = bundle.get("unchecked_count", 0)
    awaiting = bundle.get("awaiting_24h_count", 0)
    payable = bundle.get("payable_count", 0)
    approved = bundle.get("approved_count", 0)
    dead_total = removed + deleted + shadow

    lines = [
        f"👤 {handle}",
        f"🆔 {bundle['user_id']}",
        f"💰 Pending total: {bundle.get('total_pending_amount', '0')}",
        f"📝 Approved: {approved}",
        f"✅ Live: {live}    ❌ Dead: {dead_total}    ⏳ <24h: {awaiting}",
        f"⏳ Oldest pending: {age_text}",
        "",
        "💳 Payment method:",
    ]
    if bundle.get("upi_id"):
        lines.append(f"UPI: {bundle['upi_id']}")
    else:
        lines.append("UPI: (not set)")
    if bundle.get("qr_file_id"):
        lines.append("QR: available — tap below to view")
    if not bundle.get("upi_id") and not bundle.get("qr_file_id"):
        lines.append("⚠ No payment method set")

    lines.append("")
    lines.append("📋 Comment status:")

    # Group payments into sections
    def fmt(p):
        link = p.get("reddit_link") or "(no link)"
        return f"• #{p['task_id']} {p.get('amount', '')} — {link}"

    live_items = [p for p in bundle["payments"] if p["live_status"] == "live"]
    dead_items = [p for p in bundle["payments"] if p["live_status"] in ("removed", "deleted", "shadow_removed")]
    pending_items = [p for p in bundle["payments"] if p["live_status"] in ("unchecked", "error")]

    if live_items:
        lines.append("✅ Live:")
        for p in live_items:
            tag = " (✅ payable)" if p["is_payable"] == 1 else " (⏳ awaiting 24h)"
            lines.append(fmt(p) + tag)
    if dead_items:
        lines.append("❌ Dead:")
        for p in dead_items:
            lines.append(fmt(p) + f" ({p['live_status']})")
    if pending_items:
        lines.append("⏳ Recently submitted (no live check yet):")
        for p in pending_items:
            lines.append(fmt(p))

    lines.extend([
        "",
        "💰 PAYMENT SUMMARY",
        f"Total approved: {approved}",
        f"Live eligible (payable): {payable}",
        f"Awaiting 24h: {awaiting}",
        f"Dead: {dead_total}",
        f"Unchecked: {unchecked}",
        "",
        f"Final payable: {bundle.get('final_payable_amount', '0')}",
    ])
    return "\n".join(lines)


def _process_payments_keyboard(bundle, has_next):
    uid = bundle["user_id"]
    rows = [
        [
            InlineKeyboardButton(text="✅ Mark Paid", callback_data=f"process:paid:{uid}"),
            InlineKeyboardButton(text="⏭ Skip", callback_data="process:skip"),
        ],
        [
            InlineKeyboardButton(text="📨 Send Message", callback_data=f"process:msg:{uid}"),
            InlineKeyboardButton(text="🔄 Refresh Live", callback_data=f"process:refresh:{uid}"),
        ],
        [
            InlineKeyboardButton(text="📋 View All Links", callback_data=f"process:links:{uid}"),
        ],
    ]
    if bundle.get("qr_file_id"):
        rows[-1].append(InlineKeyboardButton(text="🖼 Show QR", callback_data=f"process:qr:{uid}"))
    nav = []
    if has_next:
        nav.append(InlineKeyboardButton(text="➡️ Next Member", callback_data="process:next"))
    nav.append(InlineKeyboardButton(text="🏁 End", callback_data="process:end"))
    rows.append(nav)
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _show_process_payments_card(message_or_callback, state, bot):
    queue = state["queue"]
    if not queue:
        await _finish_process_payments(message_or_callback, state)
        return
    bundle = queue[0]
    has_next = len(queue) > 1
    text = _process_payments_card(bundle)
    keyboard = _process_payments_keyboard(bundle, has_next)
    target_chat = message_or_callback.from_user.id
    await bot.send_message(target_chat, text, reply_markup=keyboard, parse_mode=None)


async def _finish_process_payments(message_or_callback, state):
    paid_users = state.get("paid_users", 0)
    paid_total = state.get("paid_total_text", "0")
    skipped = state.get("skipped", 0)
    live_total = state.get("live_total", 0)
    removed_total = state.get("removed_total", 0)

    text = (
        "💸 Process Payments — done\n\n"
        f"Members paid: {paid_users}\n"
        f"Live comments paid: {live_total}\n"
        f"Dead comments skipped: {removed_total}\n"
        f"Total paid: {paid_total}\n"
        f"Skipped members: {skipped}\n"
    )
    user_id = message_or_callback.from_user.id
    clear_user_state(user_id, "process_payments_done")
    if isinstance(message_or_callback, CallbackQuery):
        await message_or_callback.message.answer(text, reply_markup=admin_payments_menu(), parse_mode=None)
    else:
        await message_or_callback.answer(text, reply_markup=admin_payments_menu(), parse_mode=None)


async def _reload_current_bundle(state):
    """Re-fetch the bundle for the user at head of queue so we always show fresh data."""
    if not state["queue"]:
        return
    head_uid = state["queue"][0]["user_id"]
    fresh = await get_pending_payments_grouped()
    fresh_by_uid = {b["user_id"]: b for b in fresh}
    if head_uid in fresh_by_uid:
        state["queue"][0] = fresh_by_uid[head_uid]
    else:
        # No pending payments left for this user — drop them
        state["queue"].pop(0)


@dp.message(button_filter(BTN_PAYMENTS_PROCESS))
async def start_process_payments(message: Message, bot: Bot):
    log_button_click(message, BTN_PAYMENTS_PROCESS)
    if not await is_admin(message.from_user.id):
        await message.answer("Only admins can process payments.")
        return
    bundles = await get_pending_payments_grouped()
    if not bundles:
        await message.answer("No pending payments to process.")
        return
    total_users = len(bundles)
    total_payouts = sum(len(b["payments"]) for b in bundles)
    set_user_state(message.from_user.id, {
        "flow": _PROCESS_FLOW,
        "queue": bundles,
        "paid_users": 0,
        "skipped": 0,
        "paid_amounts": [],
        "paid_total_text": "0",
        "live_total": 0,
        "removed_total": 0,
        "send_msg_for": None,
        "started_at": datetime.now(timezone.utc).isoformat(),
    })
    logging.info(
        "Process Payments started: admin=%s users=%s payouts=%s",
        message.from_user.id, total_users, total_payouts,
    )
    await message.answer(
        f"💸 Process Payments\n\n{total_users} member(s) waiting, {total_payouts} payout(s) total.\nShowing one member at a time.",
        parse_mode=None,
    )
    state = user_states[message.from_user.id]
    await _show_process_payments_card(message, state, bot)


def _process_state(callback):
    """Return state if admin is in an active process_payments flow, else None."""
    state = user_states.get(callback.from_user.id)
    if not state or state.get("flow") != _PROCESS_FLOW:
        return None
    return state


@dp.callback_query(F.data.startswith("process:links:"))
async def process_links_callback(callback: CallbackQuery):
    log_callback_click(callback, "process_links")
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True); return
    uid = int(callback.data.split(":")[2])
    state = _process_state(callback)
    if not state or not state["queue"] or state["queue"][0]["user_id"] != uid:
        await callback.answer("Out of sync.", show_alert=True); return
    bundle = state["queue"][0]
    lines = ["🔗 All Reddit links"]
    for p in bundle["payments"]:
        icon = {"live": "🟢", "removed": "🔴", "deleted": "🔴",
                "shadow_removed": "🔴", "unchecked": "⚪", "error": "⚠️"}.get(p["live_status"], "⚪")
        pay_tag = " (payable)" if p["is_payable"] == 1 else ""
        lines.append(f"{icon} #{p['task_id']} {p.get('amount', '')}{pay_tag}: {p.get('reddit_link') or '(no link)'}")
    await callback.message.answer("\n".join(lines), disable_web_page_preview=True)
    await callback.answer()


@dp.callback_query(F.data.startswith("process:qr:"))
async def process_qr_callback(callback: CallbackQuery, bot: Bot):
    log_callback_click(callback, "process_qr")
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True); return
    uid = int(callback.data.split(":")[2])
    user = await get_user(uid)
    qr = user.get("qr_file_id") if user else None
    if not qr:
        await callback.answer("No QR uploaded.", show_alert=True); return
    try:
        await bot.send_photo(callback.from_user.id, qr, caption=f"QR for @{user.get('username') or uid}")
        await callback.answer()
    except Exception:
        logging.exception("Failed to send QR in process payments")
        await callback.answer("Failed to send QR.", show_alert=True)


@dp.callback_query(F.data == "process:next")
async def process_next_callback(callback: CallbackQuery, bot: Bot):
    log_callback_click(callback, "process_next")
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True); return
    state = _process_state(callback)
    if not state:
        await callback.answer("No active session.", show_alert=True); return
    if state["queue"]:
        state["queue"] = state["queue"][1:] + [state["queue"][0]]
    await callback.answer()
    await _show_process_payments_card(callback, state, bot)


@dp.callback_query(F.data == "process:skip")
async def process_skip_callback(callback: CallbackQuery, bot: Bot):
    log_callback_click(callback, "process_skip")
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True); return
    state = _process_state(callback)
    if not state:
        await callback.answer("No active session.", show_alert=True); return
    if state["queue"]:
        state["queue"].pop(0)
        state["skipped"] = state.get("skipped", 0) + 1
    await callback.answer("Skipped.")
    await _show_process_payments_card(callback, state, bot)


@dp.callback_query(F.data.startswith("process:paid:"))
async def process_paid_callback(callback: CallbackQuery, bot: Bot):
    log_callback_click(callback, "process_paid")
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True); return
    state = _process_state(callback)
    if not state:
        await callback.answer("No active session.", show_alert=True); return
    uid = int(callback.data.split(":")[2])
    if not state["queue"] or state["queue"][0]["user_id"] != uid:
        await callback.answer("Out of sync — refreshing.", show_alert=True)
        await _show_process_payments_card(callback, state, bot)
        return

    bundle = state["queue"][0]
    paid_count, paid_rows, blocked_count, waiting_count = await mark_user_payments_paid(uid)
    if paid_count == 0:
        if blocked_count > 0 and waiting_count == 0:
            await callback.answer(
                f"❌ {blocked_count} payment(s) blocked (dead comments). Skipping.",
                show_alert=True,
            )
            state["queue"].pop(0)
        elif waiting_count > 0:
            await callback.answer(
                f"⏳ {waiting_count} payment(s) still under 24h. Try Refresh Live or Skip.",
                show_alert=True,
            )
        else:
            await callback.answer("Nothing to pay (already cleared).", show_alert=True)
            state["queue"].pop(0)
        await _show_process_payments_card(callback, state, bot)
        return

    paid_total_text = _sum_payment_amounts(paid_rows)
    state["paid_users"] = state.get("paid_users", 0) + 1
    state["paid_amounts"].extend(paid_rows)
    state["paid_total_text"] = _sum_payment_amounts(state["paid_amounts"])
    state["live_total"] = state.get("live_total", 0) + paid_count
    state["removed_total"] = state.get("removed_total", 0) + blocked_count

    await log_audit_action(
        callback.from_user.id, "process_payment",
        f"user={uid} paid={paid_count} blocked={blocked_count} waiting={waiting_count} total={paid_total_text}",
    )
    logging.info(
        "Process Payments: paid user=%s count=%s total=%s blocked=%s waiting=%s admin=%s",
        uid, paid_count, paid_total_text, blocked_count, waiting_count, callback.from_user.id,
    )

    try:
        notify = [
            "✅ Payment Processed",
            "",
            f"💸 Amount: {paid_total_text}",
            f"📝 Eligible Comments: {paid_count}",
        ]
        if blocked_count > 0:
            notify.append(f"❌ Deleted: {blocked_count}")
        if waiting_count > 0:
            notify.append(f"⏳ Awaiting 24h: {waiting_count} (will pay next round)")
        notify.append("")
        notify.append("Thank you for participating.")
        await bot.send_message(uid, "\n".join(notify), parse_mode=None)
    except Exception:
        logging.warning("Could not notify user=%s about processed payment", uid)

    state["queue"].pop(0)
    await callback.answer(f"Paid {paid_total_text}.")
    await _show_process_payments_card(callback, state, bot)


@dp.callback_query(F.data.startswith("process:msg:"))
async def process_msg_callback(callback: CallbackQuery):
    log_callback_click(callback, "process_msg")
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True); return
    state = _process_state(callback)
    if not state:
        await callback.answer("No active session.", show_alert=True); return
    uid = int(callback.data.split(":")[2])
    if not state["queue"] or state["queue"][0]["user_id"] != uid:
        await callback.answer("Out of sync.", show_alert=True); return
    state["send_msg_for"] = uid
    state["msg_resume_flow"] = _PROCESS_FLOW
    # Switch user_state flow to send_message so catch-all routes free text to us
    user_states[callback.from_user.id] = {**state, "flow": _SEND_MSG_FLOW}
    await callback.message.answer(
        f"📨 Type the message to send to member {uid}.\nSend /cancel to abort.",
        parse_mode=None,
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("process:refresh:"))
async def process_refresh_callback(callback: CallbackQuery, bot: Bot):
    log_callback_click(callback, "process_refresh")
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True); return
    state = _process_state(callback)
    if not state:
        await callback.answer("No active session.", show_alert=True); return
    uid = int(callback.data.split(":")[2])
    if not state["queue"] or state["queue"][0]["user_id"] != uid:
        await callback.answer("Out of sync.", show_alert=True); return

    await callback.answer("Refreshing… checking Reddit.", show_alert=False)
    subs = await get_user_submissions_for_live_check(uid)
    if not subs:
        await callback.message.answer("No submissions to check for this member.")
        return

    import aiohttp
    counts = {"live": 0, "removed": 0, "deleted": 0, "shadow_removed": 0, "error": 0}
    timeout = aiohttp.ClientTimeout(total=20, connect=10)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for s in subs:
            try:
                status = await _check_reddit_comment_alive(session, s["reddit_comment_id"])
                await update_submission_comment_status(s["id"], status)
                counts[status] = counts.get(status, 0) + 1
            except Exception:
                logging.exception("Refresh-live per-sub failed: submission=%s", s["id"])
                counts["error"] = counts.get("error", 0) + 1
            await asyncio.sleep(0.6)
    await refresh_payable_for_user(uid)

    summary = f"🔄 Refreshed {len(subs)} submission(s) — " + ", ".join(f"{k}:{v}" for k, v in counts.items() if v)
    await callback.message.answer(summary, parse_mode=None)

    # Re-fetch the bundle to reflect new state, then redraw
    await _reload_current_bundle(state)
    await _show_process_payments_card(callback, state, bot)


@dp.callback_query(F.data == "process:end")
async def process_end_callback(callback: CallbackQuery):
    log_callback_click(callback, "process_end")
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True); return
    state = _process_state(callback)
    if not state:
        await callback.answer("No active session.", show_alert=True); return
    await callback.answer("Ending.")
    await _finish_process_payments(callback, state)


async def _handle_process_send_message(message: Message, state, bot: Bot):
    """Called from handle_text when admin is composing a member message."""
    text = (message.text or "").strip()
    target_uid = state.get("send_msg_for")
    if not target_uid:
        await message.answer("No target — try the 📨 Send Message button again.")
        return
    if not text:
        await message.answer("Empty message ignored.")
        return
    if text.lower() in ("/cancel", "cancel"):
        # Restore the process flow, redraw card
        state["flow"] = _PROCESS_FLOW
        state["send_msg_for"] = None
        user_states[message.from_user.id] = state
        await message.answer("Cancelled.")
        await _show_process_payments_card(message, state, bot)
        return
    try:
        await bot.send_message(target_uid, f"[Admin Notice]\n\n{text}", parse_mode=None)
        await log_audit_action(message.from_user.id, "admin_message",
                               f"to={target_uid} chars={len(text)}")
        await message.answer(f"✅ Sent to {target_uid}.")
    except Exception:
        logging.exception("Failed to send admin message to %s", target_uid)
        await message.answer("❌ Could not deliver message (member may have blocked the bot).")
    state["flow"] = _PROCESS_FLOW
    state["send_msg_for"] = None
    user_states[message.from_user.id] = state
    await _show_process_payments_card(message, state, bot)


# ─── Live Check Dashboard ──────────────────────────────────────────────────

@dp.message(button_filter(BTN_LIVE_DASHBOARD))
async def live_dashboard_handler(message: Message):
    log_handler_match(message, "live_dashboard_handler")
    log_button_click(message, BTN_LIVE_DASHBOARD)
    await live_check_dashboard(message)


async def live_check_dashboard(message: Message):
    if not await is_admin(message.from_user.id):
        await message.answer("Admins only.")
        return
    stats = await get_live_check_stats()
    text = (
        "📊 Live Check Dashboard\n\n"
        f"Total live: {stats['total_live']}\n"
        f"Total dead: {stats['total_dead']}\n"
        f"  • Removed (by mod): {stats['removed_by_mod']}\n"
        f"  • Deleted (by user): {stats['deleted_by_user']}\n"
        f"  • Shadow removed: {stats['shadow_removed']}\n\n"
        f"⏳ Awaiting 24h: {stats['awaiting_24h']}\n"
        f"💰 Payable now: {stats['payable']}\n\n"
        f"Unchecked: {stats['unchecked']}\n"
        f"Errors on last check: {stats['errors']}\n"
        f"Stale (no check in >2h): {stats['stale']}"
    )
    await message.answer(text, parse_mode=None)


@dp.message(Command("checkqr"))
async def check_qr_command(message: Message, command: CommandObject, bot: Bot):
    if not await is_admin(message.from_user.id):
        await message.answer("Only admins can check QR uploads.")
        return
    if not command.args or not command.args.strip().isdigit():
        await message.answer("Use: /checkqr user_id")
        return

    member_id = int(command.args.strip())
    user = await get_user(member_id)
    qr_file_id = user["qr_file_id"] if user else None
    logging.info(
        "QR fetch success: admin=%s member=%s has_qr=%s",
        message.from_user.id,
        member_id,
        bool(qr_file_id),
    )

    if not qr_file_id:
        await message.answer("No QR uploaded.")
        return

    try:
        await bot.send_photo(
            chat_id=message.from_user.id,
            photo=qr_file_id,
            caption=f"QR for member {member_id}",
        )
        logging.info("send_photo success: admin=%s member=%s", message.from_user.id, member_id)
    except Exception:
        logging.exception("send_photo failure: admin=%s member=%s", message.from_user.id, member_id)
        await message.answer("QR was found in DB, but Telegram could not send it.")


@dp.callback_query(F.data.startswith("payment:confirm:"))
async def confirm_payment(callback: CallbackQuery):
    log_callback_click(callback, "pay_confirm")
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True)
        return
    payment_id = int(callback.data.split(":")[2])
    # Block payment if the underlying Reddit comment is dead
    sub = await get_submission_for_payment(payment_id)
    if sub and sub.get("comment_alive") == 0:
        await callback.answer(
            "❌ Comment was removed on Reddit. Payment blocked.",
            show_alert=True,
        )
        return
    payments = await get_pending_payments()
    payment = next((p for p in payments if p["id"] == payment_id), None)
    if not payment:
        await callback.answer("Payment not found or already paid.", show_alert=True)
        return
    confirm_keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Confirm", callback_data=f"payment:paid:{payment_id}"),
            InlineKeyboardButton(text="❌ Cancel", callback_data=f"payment:cancel:{payment_id}"),
        ]
    ])
    confirm_text = (
        f"⚠ Confirm Payment\n\n"
        f"👤 @{payment['username'] or 'no_username'}\n"
        f"💸 Amount: {payment['amount']}\n"
        f"📌 Task: #{payment['task_id']}"
    )
    await callback.message.answer(confirm_text, reply_markup=confirm_keyboard)
    await callback.answer()


@dp.callback_query(F.data.startswith("payment:paid:"))
async def execute_payment(callback: CallbackQuery, bot: Bot):
    log_callback_click(callback, "pay_execute")
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True)
        return
    payment_id = int(callback.data.split(":")[2])
    # Double-check liveness gate at execute time too
    sub = await get_submission_for_payment(payment_id)
    if sub and sub.get("comment_alive") == 0:
        await callback.answer(
            "❌ Comment was removed on Reddit. Payment blocked.",
            show_alert=True,
        )
        return
    payment = await mark_payment_paid(payment_id)
    if not payment:
        await callback.answer("Payment not found or already paid.", show_alert=True)
        return
    logging.info("Payment paid: id=%s admin=%s", payment_id, callback.from_user.id)
    await log_audit_action(callback.from_user.id, "payment_paid", f"Payment {payment_id} marked paid: {payment['amount']}")
    paid_text = f"✅ Payment marked paid\n\nAmount: {payment['amount']}"
    if callback.message.photo:
        await callback.message.edit_caption(caption=paid_text)
    else:
        await callback.message.edit_text(paid_text)
    try:
        await bot.send_message(
            payment["user_id"],
            f"✅ Payment Sent\n\n💸 Amount:\n{payment['amount']}\n\n📌 Task:\n#{payment['task_id']}\n\nThank you for participating in VIRON."
        )
    except Exception:
        logging.exception("Could not notify member about payment")
    await callback.answer("Paid.")


@dp.callback_query(F.data.startswith("payment:cancel:"))
async def cancel_payment_callback(callback: CallbackQuery):
    log_callback_click(callback, "cancel_payment")
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True)
        return
    payment_id = int(callback.data.split(":")[2])
    payment = await cancel_payment(payment_id)
    if not payment:
        await callback.answer("Payment not found.", show_alert=True)
        return
    logging.info("Payment cancelled: id=%s admin=%s", payment_id, callback.from_user.id)
    cancel_text = f"❌ Payment cancelled\n\nAmount: {payment['amount']}"
    if callback.message.photo:
        await callback.message.edit_caption(caption=cancel_text)
    else:
        await callback.message.edit_text(cancel_text)
    await callback.answer("Cancelled.")


@dp.message(button_filter(BTN_REVIEWS_FLAGGED))
async def flagged_submissions(message: Message):
    log_button_click(message, BTN_REVIEWS_FLAGGED)
    clear_user_state(message.from_user.id, "flagged_submissions")
    if not await is_admin(message.from_user.id):
        await message.answer("Only admins can view flagged submissions.")
        return
    rows = await get_flagged_submissions()
    if not rows:
        await message.answer("No flagged submissions.")
        return
    for row in rows:
        await message.answer(
            f"⚠ Flagged Submission\n\n"
            f"Reason: {row['reason']}\n"
            f"Member: @{row['username'] or 'no_username'}\n"
            f"Task: #{row['task_id']}\n"
            f"Proof: {row['reddit_link']}"
        )


@dp.message(button_filter(BTN_REVIEWS_PENDING))
async def review_button(message: Message):
    log_button_click(message, BTN_REVIEWS_PENDING)
    clear_user_state(message.from_user.id, "review_queue")
    if not await is_admin(message.from_user.id):
        await message.answer("Only admins can review submissions.")
        return
    await send_review_queue(message)


@dp.message(Command("review"))
async def review_command(message: Message):
    if not await is_admin(message.from_user.id):
        await message.answer("Only admins can review submissions.")
        return
    await send_review_queue(message)


@dp.message(button_filter(BTN_MEMBERS_SEARCH))
async def member_stats_admin(message: Message):
    log_button_click(message, BTN_MEMBERS_SEARCH)
    if not await is_admin(message.from_user.id):
        await message.answer("Only admins can view member stats.")
        return
    set_user_state(message.from_user.id, {"flow": "member_stats"})
    await message.answer("🔍 Send Member Telegram ID or @username.")


@dp.message(Command("shadowban"))
async def shadowban_command(message: Message, command: CommandObject):
    if not await is_admin(message.from_user.id): return
    if not command.args:
        await message.answer("Use: /shadowban user_id")
        return
    try:
        user_id = int(command.args.strip())
        await shadowban_user(user_id, 1)
        await log_audit_action(message.from_user.id, "shadowban", f"Shadowbanned {user_id}")
        await message.answer(f"👻 User {user_id} shadowbanned.")
    except Exception:
        await message.answer("Invalid user ID.")


@dp.message(Command("unshadowban"))
async def unshadowban_command(message: Message, command: CommandObject):
    if not await is_admin(message.from_user.id): return
    if not command.args:
        await message.answer("Use: /unshadowban user_id")
        return
    try:
        user_id = int(command.args.strip())
        await shadowban_user(user_id, 0)
        await log_audit_action(message.from_user.id, "unshadowban", f"Unshadowbanned {user_id}")
        await message.answer(f"✅ User {user_id} unshadowbanned.")
    except Exception:
        await message.answer("Invalid user ID.")


@dp.message(Command("reputation"))
async def reputation_command(message: Message, command: CommandObject):
    if not await is_admin(message.from_user.id): return
    if not command.args:
        await message.answer("Use: /reputation user_id score_change")
        return
    try:
        parts = command.args.split(maxsplit=1)
        user_id = int(parts[0])
        score_change = int(parts[1])
        await update_reputation(user_id, score_change)
        await message.answer(f"⭐ Reputation of {user_id} updated by {score_change}.")
    except Exception:
        await message.answer("Use: /reputation user_id score_change")


# ------------------------------------------------------------
# Reddit-account admin tools
# ------------------------------------------------------------

async def _resolve_telegram_user(token):
    """Accept @username, raw username, or numeric ID; return telegram_id or None."""
    token = token.strip()
    if token.startswith("@"):
        return await get_user_by_username(token)
    if token.isdigit():
        return int(token)
    if re.match(r"^[A-Za-z0-9_]{3,}$", token):
        return await get_user_by_username(token)
    return None


@dp.message(Command("reddit_pending"))
async def reddit_pending_command(message: Message):
    if not await is_admin(message.from_user.id):
        return
    pending = await get_pending_reddit_accounts()
    if not pending:
        await message.answer("✅ No pending Reddit account requests.")
        return
    for row in pending:
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="✅ Approve", callback_data=f"radm:approve:{row['id']}"),
            InlineKeyboardButton(text="❌ Reject", callback_data=f"radm:reject:{row['id']}"),
        ]])
        await message.answer(
            f"⏳ Pending request\n\n"
            f"Telegram user: {row['telegram_user_id']} (@{row['tg_username'] or 'no_handle'})\n"
            f"Reddit username: u/{row['reddit_username']}\n"
            f"Account ID: {row['id']}\n"
            f"Submitted: {row['added_at']}",
            reply_markup=kb,
            parse_mode=None,
        )


@dp.callback_query(F.data.startswith("radm:approve:"))
async def reddit_admin_approve_callback(callback: CallbackQuery, bot: Bot):
    log_callback_click(callback, "radm:approve")
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True)
        return
    account_id = int(callback.data.split(":")[2])
    row = await approve_reddit_account(account_id, callback.from_user.id)
    if not row:
        await callback.answer("Account not found.", show_alert=True)
        return
    await log_audit_action(
        callback.from_user.id, "reddit_approve",
        f"Approved Reddit account {account_id} (u/{row['reddit_username']}) "
        f"for telegram_user={row['telegram_user_id']}",
    )
    try:
        await callback.message.edit_text(
            f"✅ Approved u/{row['reddit_username']} for telegram_user={row['telegram_user_id']}.",
            parse_mode=None,
        )
    except Exception:
        pass
    try:
        await bot.send_message(
            row["telegram_user_id"],
            f"✅ Your Reddit account u/{row['reddit_username']} has been approved.\n\n"
            "You can now submit proofs from this account.",
            parse_mode=None,
        )
    except Exception:
        logging.warning("Could not notify user about reddit approval: user=%s", row["telegram_user_id"])
    await callback.answer("Approved.")


@dp.callback_query(F.data.startswith("radm:reject:"))
async def reddit_admin_reject_callback(callback: CallbackQuery):
    log_callback_click(callback, "radm:reject")
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True)
        return
    account_id = int(callback.data.split(":")[2])
    set_user_state(
        callback.from_user.id,
        {"flow": "reddit_reject_reason", "account_id": account_id,
         "card_chat_id": callback.message.chat.id,
         "card_message_id": callback.message.message_id},
    )
    await callback.message.answer(
        "Send a short rejection reason (or type `-` for none).",
        parse_mode=None,
    )
    await callback.answer()


@dp.message(Command("reddit_approve"))
async def reddit_approve_command(message: Message, bot: Bot):
    if not await is_admin(message.from_user.id):
        return
    args = (message.text or "").split(maxsplit=1)
    if len(args) < 2 or not args[1].strip().isdigit():
        await message.answer("Use: /reddit_approve <account_id>")
        return
    account_id = int(args[1].strip())
    row = await approve_reddit_account(account_id, message.from_user.id)
    if not row:
        await message.answer("Account not found.")
        return
    await log_audit_action(
        message.from_user.id, "reddit_approve",
        f"Approved Reddit account {account_id} (u/{row['reddit_username']}) "
        f"for telegram_user={row['telegram_user_id']}",
    )
    await message.answer(
        f"✅ Approved u/{row['reddit_username']} for {row['telegram_user_id']}."
    )
    try:
        await bot.send_message(
            row["telegram_user_id"],
            f"✅ Your Reddit account u/{row['reddit_username']} has been approved.",
        )
    except Exception:
        pass


@dp.message(Command("reddit_reject"))
async def reddit_reject_command(message: Message, bot: Bot, command: CommandObject):
    if not await is_admin(message.from_user.id):
        return
    if not command.args:
        await message.answer("Use: /reddit_reject <account_id> [reason]")
        return
    parts = command.args.split(maxsplit=1)
    if not parts[0].isdigit():
        await message.answer("Account ID must be numeric.")
        return
    account_id = int(parts[0])
    reason = parts[1] if len(parts) > 1 else ""
    row = await reject_reddit_account(account_id, message.from_user.id, reason)
    if not row:
        await message.answer("Account not found.")
        return
    await log_audit_action(
        message.from_user.id, "reddit_reject",
        f"Rejected Reddit account {account_id} (u/{row['reddit_username']}) "
        f"for telegram_user={row['telegram_user_id']}: {reason or '(no reason)'}",
    )
    await message.answer(f"❌ Rejected u/{row['reddit_username']}.")
    try:
        await bot.send_message(
            row["telegram_user_id"],
            f"❌ Your Reddit account u/{row['reddit_username']} was not approved.\n\n"
            f"Reason: {reason or 'not provided'}\n\n"
            "You may add a different account or contact an admin.",
        )
    except Exception:
        pass


@dp.message(Command("reddit_disable"))
async def reddit_disable_command(message: Message, command: CommandObject, bot: Bot):
    if not await is_admin(message.from_user.id):
        return
    if not command.args:
        await message.answer("Use: /reddit_disable <account_id> [reason]")
        return
    parts = command.args.split(maxsplit=1)
    if not parts[0].isdigit():
        await message.answer("Account ID must be numeric.")
        return
    account_id = int(parts[0])
    reason = parts[1] if len(parts) > 1 else ""
    row = await disable_reddit_account(account_id, message.from_user.id, reason)
    if not row:
        await message.answer("Account not found.")
        return
    await log_audit_action(
        message.from_user.id, "reddit_disable",
        f"Disabled Reddit account {account_id} (u/{row['reddit_username']}): {reason or '(no reason)'}",
    )
    await message.answer(f"🚫 Disabled u/{row['reddit_username']}.")
    try:
        await bot.send_message(
            row["telegram_user_id"],
            f"🚫 Your Reddit account u/{row['reddit_username']} has been disabled by an admin.\n\n"
            f"Reason: {reason or 'not provided'}",
        )
    except Exception:
        pass


@dp.message(Command("set_reddit_limit"))
async def set_reddit_limit_command(message: Message, command: CommandObject):
    if not await is_admin(message.from_user.id):
        return
    if not command.args:
        await message.answer("Use: /set_reddit_limit <@user|id> <max_accounts>")
        return
    parts = command.args.split()
    if len(parts) < 2 or not parts[-1].isdigit():
        await message.answer("Use: /set_reddit_limit <@user|id> <max_accounts>")
        return
    target = " ".join(parts[:-1])
    value = int(parts[-1])
    if value < 1 or value > 20:
        await message.answer("max_accounts must be between 1 and 20.")
        return
    telegram_id = await _resolve_telegram_user(target)
    if not telegram_id:
        await message.answer("Could not resolve that user.")
        return
    await set_max_reddit_accounts(telegram_id, value)
    await log_audit_action(
        message.from_user.id, "reddit_set_limit",
        f"Set max_reddit_accounts={value} for telegram_user={telegram_id}",
    )
    await message.answer(f"⚙ Limit set: telegram_user={telegram_id} → {value} Reddit account(s).")


@dp.message(Command("reddit_info"))
async def reddit_info_command(message: Message, command: CommandObject):
    """Admin view of one member's registered Reddit accounts + per-account health."""
    if not await is_admin(message.from_user.id):
        return
    if not command.args:
        await message.answer("Use: /reddit_info <@user|id>")
        return
    telegram_id = await _resolve_telegram_user(command.args.strip())
    if not telegram_id:
        await message.answer("Could not resolve that user.")
        return
    accounts = await list_reddit_accounts(telegram_id)
    user_row = await get_user(telegram_id)
    cap = (user_row or {}).get("max_reddit_accounts") or 1
    if not accounts:
        await message.answer(
            f"telegram_user={telegram_id} has no registered Reddit accounts.\n"
            f"Cap: {cap}",
        )
        return
    lines = [f"👤 Reddit accounts for telegram_user={telegram_id}", f"Cap: {cap}\n"]
    for acc in accounts:
        health = await get_reddit_account_health(acc["id"])
        if health:
            lines.append(
                f"• u/{acc['reddit_username']} [{acc['status']}] · id={acc['id']}\n"
                f"   submissions={health['total_submissions']} approved={health['approved']} "
                f"rejected={health['rejected']} pending={health['pending']}\n"
                f"   live={health['live']} dead={health['dead']} "
                f"approval={health['approval_rate']}% live={health['live_rate']}% "
                f"warnings={health['warnings']}"
            )
        else:
            lines.append(f"• u/{acc['reddit_username']} [{acc['status']}] · id={acc['id']}")
    await message.answer("\n".join(lines), parse_mode=None)


async def _continue_reddit_reject_reason(message: Message, state, bot: Bot):
    account_id = state.get("account_id")
    reason = (message.text or "").strip()
    if reason == "-":
        reason = ""
    row = await reject_reddit_account(account_id, message.from_user.id, reason)
    clear_user_state(message.from_user.id, "reddit_reject_done")
    if not row:
        await message.answer("Account not found (already rejected?)")
        return
    await log_audit_action(
        message.from_user.id, "reddit_reject",
        f"Rejected Reddit account {account_id} (u/{row['reddit_username']}) "
        f"for telegram_user={row['telegram_user_id']}: {reason or '(no reason)'}",
    )
    await message.answer(f"❌ Rejected u/{row['reddit_username']}.")
    card_chat = state.get("card_chat_id")
    card_msg = state.get("card_message_id")
    if card_chat and card_msg:
        try:
            await bot.edit_message_text(
                f"❌ Rejected u/{row['reddit_username']} (reason: {reason or 'not provided'})",
                chat_id=card_chat,
                message_id=card_msg,
            )
        except Exception:
            pass
    try:
        await bot.send_message(
            row["telegram_user_id"],
            f"❌ Your Reddit account u/{row['reddit_username']} was not approved.\n\n"
            f"Reason: {reason or 'not provided'}\n\n"
            "You may add a different account or contact an admin.",
        )
    except Exception:
        pass


@dp.message(button_filter(BTN_ADMIN_BROADCAST))
async def broadcast_start(message: Message):
    log_button_click(message, BTN_ADMIN_BROADCAST)
    if not await is_admin(message.from_user.id):
        await message.answer("Only admins can broadcast.")
        return
    clear_user_state(message.from_user.id, "broadcast_start")
    keyboard = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="All Members", callback_data="broadcast:aud:all"),
        InlineKeyboardButton(text="Active Members", callback_data="broadcast:aud:active"),
    ]])
    await message.answer("Choose broadcast audience.", reply_markup=keyboard)


@dp.callback_query(F.data.startswith("broadcast:aud:"))
async def broadcast_audience(callback: CallbackQuery):
    log_callback_click(callback, "broadcast_audience")
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True)
        return
    audience = callback.data.split(":")[2]
    set_user_state(callback.from_user.id, {"flow": "broadcast_compose", "audience": audience})
    await callback.message.answer(
        f"📢 Broadcast to: {'All Members' if audience == 'all' else 'Active Members'}\n\n"
        "Send the message to broadcast. It will be previewed before sending."
    )
    await callback.answer()


@dp.callback_query(F.data == "broadcast:send")
async def broadcast_send_confirm(callback: CallbackQuery, bot: Bot):
    log_callback_click(callback, "broadcast_send")
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True)
        return
    state = user_states.get(callback.from_user.id)
    if not state or state.get("flow") != "broadcast_confirm":
        await callback.answer("No broadcast pending.", show_alert=True)
        return
    audience = state["audience"]
    text = state["text"]
    member_ids = await get_all_member_ids(active_only=audience == "active")
    sent = 0
    for member_id in member_ids:
        try:
            await bot.send_message(member_id, f"📢 Announcement\n\n{text}")
            sent += 1
        except Exception:
            logging.exception("Broadcast failed for user=%s", member_id)
    clear_user_state(callback.from_user.id, "broadcast_sent")
    logging.info("Broadcast sent: admin=%s count=%s", callback.from_user.id, sent)
    await log_audit_action(callback.from_user.id, "broadcast", f"Broadcast to {audience}: {sent} sent")
    await callback.message.edit_text(f"✅ Broadcast sent to {sent} member(s).")
    await callback.answer("Sent.")


@dp.callback_query(F.data == "broadcast:cancel")
async def broadcast_cancel(callback: CallbackQuery):
    log_callback_click(callback, "broadcast_cancel")
    clear_user_state(callback.from_user.id, "broadcast_cancelled")
    await callback.message.edit_text("❌ Broadcast cancelled.")
    await callback.answer("Cancelled.")


async def send_my_stats(target, user_id, username):
    clear_user_state(user_id, "my_stats")
    await register_user(user_id, username)
    stats = await get_member_stats(user_id)
    if not stats:
        await target.answer("Could not load stats. Please try again.")
        return
    payments = await get_payment_history(user_id)
    earned = total_amount(payments, paid_only=True) or "₹0"
    today = await get_submissions_today_count(user_id)

    await target.answer(
        "👤 **My Stats**\n\n"
        f"✅ **Completed:** {stats['approved']}\n"
        f"⏳ **Pending:** {stats['pending']}\n"
        f"❌ **Rejected:** {stats['rejected']}\n\n"
        f"💰 **Total Earned:** {earned}\n"
        f"📤 **Submitted Today:** {today}",
        parse_mode=None
    )


@dp.message(button_filter(BTN_MY_STATS))
@dp.message(Command("stats"))
async def my_stats(message: Message):
    if not await require_real_user(message):
        return
    log_handler_match(message, "my_stats")
    log_button_click(message, BTN_MY_STATS)
    await send_my_stats(message, message.from_user.id, message.from_user.username)


_REDDIT_STATUS_ICONS = {
    "pending": "⏳",
    "active": "✅",
    "disabled": "🚫",
    "rejected": "❌",
}


def _reddit_status_label(status):
    icon = _REDDIT_STATUS_ICONS.get(status, "•")
    return f"{icon} {status.title()}"


def _reddit_accounts_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="➕ Add Reddit Account", callback_data="reddit:add"),
            InlineKeyboardButton(text="📋 View Registered", callback_data="reddit:list"),
        ],
        [
            InlineKeyboardButton(text="❌ Remove Reddit Account", callback_data="reddit:remove_pick"),
            InlineKeyboardButton(text="🏠 Home", callback_data="member:home"),
        ],
    ])


async def _send_reddit_accounts_home(target, user_id):
    accounts = await list_reddit_accounts(user_id)
    if not accounts:
        body = (
            "👤 Manage Reddit Accounts\n\n"
            "No Reddit accounts registered yet.\n\n"
            "Tap ➕ Add Reddit Account to submit one for admin approval.\n\n"
            "Note: each Reddit account is reviewed by an admin before it can be used as proof."
        )
    else:
        lines = ["👤 Manage Reddit Accounts\n"]
        for acc in accounts:
            lines.append(f"• u/{acc['reddit_username']} — {_reddit_status_label(acc['status'])}")
        lines.append("\nOnly ✅ Active accounts are accepted as proof. Use ❌ to remove.")
        body = "\n".join(lines)
    await target.answer(body, reply_markup=_reddit_accounts_keyboard(), parse_mode=None)


async def _open_manage_reddit_accounts(message_or_callback, user_id, username):
    await register_user(user_id, username)
    target = (
        message_or_callback.message
        if isinstance(message_or_callback, CallbackQuery)
        else message_or_callback
    )
    await _send_reddit_accounts_home(target, user_id)


@dp.message(button_filter(BTN_REDDIT_ACCOUNTS))
async def open_reddit_accounts(message: Message):
    if not await require_real_user(message):
        return
    log_handler_match(message, "open_reddit_accounts")
    log_button_click(message, BTN_REDDIT_ACCOUNTS)
    clear_user_state(message.from_user.id, "manage_reddit_open")
    await _open_manage_reddit_accounts(message, message.from_user.id, message.from_user.username)


async def _notify_admins(bot: Bot, text: str):
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, text, parse_mode=None)
        except Exception:
            logging.warning("Could not deliver admin alert to %s", admin_id)


async def _notify_admins_with_buttons(bot: Bot, text: str, keyboard: InlineKeyboardMarkup):
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, text, reply_markup=keyboard, parse_mode=None)
        except Exception:
            logging.warning("Could not deliver admin alert to %s", admin_id)


@dp.callback_query(F.data == "reddit:add")
async def reddit_add_callback(callback: CallbackQuery):
    log_callback_click(callback, "reddit:add")
    user_id = callback.from_user.id
    set_user_state(user_id, {"flow": "add_reddit_account"})
    await callback.message.answer(
        "➕ Add Reddit Account\n\n"
        "Send your Reddit username (just the name, no `u/` prefix needed).\n\n"
        "Example: SilverFox42\n\n"
        "Rules:\n"
        "• 3–20 chars, letters / digits / `_` / `-`.\n"
        "• Must belong to YOU.\n"
        "• Goes into the admin approval queue first.\n\n"
        "Tap ❌ Cancel to abort.",
        reply_markup=back_cancel_keyboard(),
        parse_mode=None,
    )
    await callback.answer()


@dp.callback_query(F.data == "reddit:list")
async def reddit_list_callback(callback: CallbackQuery):
    log_callback_click(callback, "reddit:list")
    await _send_reddit_accounts_home(callback.message, callback.from_user.id)
    await callback.answer()


@dp.callback_query(F.data == "reddit:remove_pick")
async def reddit_remove_pick_callback(callback: CallbackQuery):
    log_callback_click(callback, "reddit:remove_pick")
    accounts = await list_reddit_accounts(callback.from_user.id)
    accounts = [a for a in accounts if a["status"] != "disabled"]
    if not accounts:
        await callback.message.answer(
            "You don't have any removable accounts.\n\n"
            "Disabled accounts can only be cleared by an admin.",
            parse_mode=None,
        )
        await callback.answer()
        return
    rows = [
        [InlineKeyboardButton(
            text=f"❌ u/{acc['reddit_username']} ({acc['status']})",
            callback_data=f"reddit:remove:{acc['reddit_username']}",
        )]
        for acc in accounts
    ]
    rows.append([InlineKeyboardButton(text="⬅️ Back", callback_data="member:reddit")])
    await callback.message.answer(
        "Tap the account you want to remove from your profile:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=rows),
        parse_mode=None,
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("reddit:remove:"))
async def reddit_remove_callback(callback: CallbackQuery):
    log_callback_click(callback, "reddit:remove")
    username = callback.data.split(":", 2)[2]
    ok = await remove_reddit_account(callback.from_user.id, username)
    if ok:
        await callback.message.answer(
            f"✅ Removed u/{username} from your profile.\n\n"
            "You can re-add it later — it will go through admin approval again.",
            parse_mode=None,
        )
        logging.info(
            "Reddit account removed: telegram_user=%s reddit_username=%s",
            callback.from_user.id, username,
        )
    else:
        await callback.message.answer(
            "Could not remove that account. It may already be gone or disabled.",
            parse_mode=None,
        )
    await _send_reddit_accounts_home(callback.message, callback.from_user.id)
    await callback.answer()


async def continue_add_reddit_account(message: Message, bot: Bot):
    raw = message.text.strip()
    user_id = message.from_user.id
    username = message.from_user.username
    name = normalize_reddit_username(raw)
    if not name:
        await message.answer(
            "❌ That doesn't look like a valid Reddit username.\n\n"
            "Use 3–20 characters: letters, digits, `_` or `-`.",
            parse_mode=None,
        )
        return

    code, payload = await add_reddit_account(user_id, name)

    if code == "invalid_format":
        await message.answer(
            "❌ Invalid format. Use 3–20 characters: letters, digits, `_` or `-`.",
            parse_mode=None,
        )
        return

    if code == "limit_reached":
        clear_user_state(user_id, "reddit_limit_reached")
        await message.answer(
            f"⚠ You've reached your registered-account limit ({payload['current']}/{payload['max']}).\n\n"
            "Remove an unused account first, or contact an admin to lift the cap.",
            reply_markup=await main_menu(user_id),
            parse_mode=None,
        )
        return

    if code == "duplicate_self":
        clear_user_state(user_id, "reddit_dup_self")
        await message.answer(
            f"ℹ️ u/{payload['reddit_username']} is already on your profile (status: {payload['status']}).",
            reply_markup=await main_menu(user_id),
            parse_mode=None,
        )
        return

    if code == "taken_by_other":
        clear_user_state(user_id, "reddit_taken")
        await message.answer(
            "⚠ This Reddit account is registered to another member.\n\n"
            "If you believe this is a mistake, contact an admin.",
            reply_markup=await main_menu(user_id),
            parse_mode=None,
        )
        # Hard block + admin alert
        await _notify_admins(
            bot,
            "🚨 Cross-user Reddit account attempt\n\n"
            f"Telegram user: {user_id} (@{username or 'no_handle'})\n"
            f"Attempted to register: u/{payload['reddit_username']}\n"
            f"Already owned by Telegram user: {payload['other_user_id']} "
            f"(status: {payload['status']})\n\n"
            "Investigate possible account-sharing or impersonation.",
        )
        logging.warning(
            "Reddit cross-user attempt: telegram_user=%s reddit_username=%s owner=%s",
            user_id, payload["reddit_username"], payload["other_user_id"],
        )
        return

    if code == "pending":
        clear_user_state(user_id, "reddit_pending")
        await message.answer(
            f"✅ Request submitted for u/{payload['reddit_username']}.\n\n"
            "⏳ An admin will review and approve it shortly. You'll be notified.\n\n"
            "Until then, proofs from this account will be rejected.",
            reply_markup=await main_menu(user_id),
            parse_mode=None,
        )
        approval_kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="✅ Approve",
                callback_data=f"radm:approve:{payload['account_id']}",
            ),
            InlineKeyboardButton(
                text="❌ Reject",
                callback_data=f"radm:reject:{payload['account_id']}",
            ),
        ]])
        await _notify_admins_with_buttons(
            bot,
            "👤 New Reddit account pending approval\n\n"
            f"Telegram user: {user_id} (@{username or 'no_handle'})\n"
            f"Reddit username: u/{payload['reddit_username']}\n"
            f"Account ID: {payload['account_id']}\n\n"
            "Approve or reject below.",
            approval_kb,
        )
        logging.info(
            "Reddit account requested: telegram_user=%s reddit_username=%s id=%s",
            user_id, payload["reddit_username"], payload["account_id"],
        )
        return

    # Defensive fallback
    await message.answer("Something went wrong. Try again.", parse_mode=None)


@dp.message(Command("history"))
async def command_history(message: Message):
    rows = await get_submission_history(message.from_user.id)
    if not rows:
        await message.answer("No submission history found.")
        return
    lines = ["📜 SUBMISSION HISTORY\n"]
    icons = {"approved": "✅", "pending_review": "⏳", "rejected": "❌", "flagged": "⚠"}
    for row in rows:
        icon = icons.get(row['status'], "❓")
        lines.append(f"{icon} Task #{row['task_id']} | {row['payout_amount']} | {row['status']}")
    await message.answer("\n".join(lines))

@dp.message(button_filter(BTN_PAYMENTS))
async def payments_menu_open(message: Message):
    if not await require_real_user(message):
        return
    log_handler_match(message, "payments_menu_open")
    log_button_click(message, BTN_PAYMENTS)
    clear_user_state(message.from_user.id, "payments_menu")
    await register_user(message.from_user.id, message.from_user.username)
    await message.answer(
        "💸 **Payment Information**\n\n"
        "Approved submissions are processed manually by our team.\n\n"
        "🕒 **Timing:**\n"
        "• Payments are sent multiple times a day.\n"
        "• Maximum processing time is 3 days.\n\n"
        "💡 **Tip:** Ensure your UPI ID or QR is correct to avoid delays.",
        parse_mode=None
    )
    await message.answer(
        "Manage your payment details below:",
        reply_markup=payments_menu(),
    )
    await message.answer("Group payment buttons:", reply_markup=payment_inline_menu())


@dp.message(button_filter(BTN_SET_UPI))
async def set_upi(message: Message):
    if not await require_real_user(message):
        return
    log_handler_match(message, "set_upi")
    log_button_click(message, BTN_SET_UPI)
    set_user_state(message.from_user.id, {"flow": "set_upi"})
    await message.answer("Send your UPI ID. Example: name@okaxis")


@dp.message(button_filter(BTN_UPLOAD_QR))
async def upload_qr(message: Message):
    if not await require_real_user(message):
        return
    log_handler_match(message, "upload_qr")
    log_button_click(message, BTN_UPLOAD_QR)
    set_user_state(message.from_user.id, {"flow": "upload_qr"})
    await message.answer("Upload your payment QR image.")


@dp.message(button_filter(BTN_PAYMENT_HISTORY))
async def payment_history(message: Message):
    if not await require_real_user(message):
        return
    log_handler_match(message, "payment_history")
    log_button_click(message, BTN_PAYMENT_HISTORY)
    clear_user_state(message.from_user.id, "payment_history")
    rows = await get_payment_history(message.from_user.id)
    if not rows:
        await message.answer("No payments yet.")
        return
    lines = ["💰 PAYMENT HISTORY\n"]
    icons = {"paid": "✅", "pending": "⏳", "processing": "🔄", "failed": "❌", "rejected": "❌"}
    for payment in rows[:20]:
        icon = icons.get(payment["status"], "⏳")
        lines.append(f"{icon} {payment['amount']} {payment['status'].title()} - Task #{payment['task_id']}")
    lines.append(f"\n💵 Total Earned:\n{total_amount(rows, paid_only=True)}")
    await message.answer("\n".join(lines))


@dp.message(button_filter(BTN_TOTAL_EARNINGS))
async def total_earnings(message: Message):
    if not await require_real_user(message):
        return
    log_handler_match(message, "total_earnings")
    log_button_click(message, BTN_TOTAL_EARNINGS)
    clear_user_state(message.from_user.id, "total_earnings")
    rows = await get_payment_history(message.from_user.id)
    await message.answer(f"💵 Total Earned:\n{total_amount(rows, paid_only=True)}")


@dp.message(button_filter(BTN_RULES))
async def rules(message: Message):
    log_handler_match(message, "rules")
    log_button_click(message, BTN_RULES)
    clear_user_state(message.from_user.id, "rules")
    await message.answer(
        "📜 **System Rules**\n\n"
        "• **No Spam:** Do not attempt to game the system.\n"
        "• **No Fake Proofs:** We verify every link.\n"
        "• **Don't Delete:** Keep your comments live to get paid.\n"
        "• **Respect:** Be polite with admins and members.\n\n"
        "⚠️ **Violations** may result in rejected tasks, payment holds, or permanent bans.",
        parse_mode=None
    )


@dp.message(button_filter(BTN_HELP))
async def help_button(message: Message):
    log_handler_match(message, "help_button")
    log_button_click(message, BTN_HELP)
    clear_user_state(message.from_user.id, "help")
    await message.answer(
        "❓ **Help Guide**\n\n"
        "📋 **Claim Task:** Receive a unique comment to post.\n"
        "📤 **Submit Proof:** Send your Reddit comment link.\n"
        "📊 **My Stats:** Track your progress and earnings.\n"
        "💸 **Payments:** Approved tasks go to the payout queue.\n\n"
        "💡 **Pro Tips:**\n"
        "• Submit only **COMMENT** links (not post links).\n"
        "• Set your UPI ID in **Payment Info** early.\n"
        "• Do not edit your comments heavily.",
        parse_mode=None
    )


@dp.callback_query(F.data.startswith("member:"))
async def member_menu_callback(callback: CallbackQuery, bot: Bot):
    log_callback_click(callback, "member_menu")
    if not callback.from_user:
        await callback.answer("Open the bot as your personal account.", show_alert=True)
        return

    action = callback.data.split(":", 1)[1]
    user_id = callback.from_user.id
    username = callback.from_user.username

    if action == "home":
        clear_user_state(user_id, "inline_home")
        try:
            await callback.message.edit_text(
                "🔥 **VIRON Reddit Tasks**\n\nChoose an option:",
                reply_markup=member_inline_menu(),
                parse_mode=None,
            )
        except Exception:
            await callback.message.answer(
                "🔥 **VIRON Reddit Tasks**\n\nChoose an option:",
                reply_markup=member_inline_menu(),
                parse_mode=None,
            )
    elif action == "claim":
        clear_user_state(user_id, "inline_claim")
        await claim_for_user(callback.message, bot, user_id, username)
    elif action == "submit":
        if await get_setting("maintenance_mode", "0") == "1":
            await callback.message.answer("⚠ System temporarily under maintenance.\n\nPlease try again later.")
        else:
            await register_user(user_id, username)
            claim_data = await get_active_claim(user_id)
            if not claim_data:
                await callback.message.answer("You do not have an active task. Tap 📋 Claim Task first.")
            else:
                set_user_state(user_id, {"flow": "submit_proof"})
                await callback.message.answer(
                    "📤 **Submit Your Proof**\n\n"
                    "In groups, send your proof as a command so Telegram delivers it:\n"
                    "`/submit https://reddit.com/r/sub/comments/postid/comment/commentid/`\n\n"
                    "You can also open the bot in private chat and send the link there.",
                    parse_mode=None,
                )
    elif action == "stats":
        clear_user_state(user_id, "inline_stats")
        await send_my_stats(callback.message, user_id, username)
    elif action == "payments":
        clear_user_state(user_id, "inline_payments")
        await register_user(user_id, username)
        await callback.message.answer(
            "💸 **Payment Information**\n\nApproved submissions are processed manually by our team.",
            reply_markup=payment_inline_menu(),
            parse_mode=None,
        )
    elif action == "active_tasks":
        clear_user_state(user_id, "inline_active_tasks")
        await active_tasks(callback, page=1)
    elif action == "reddit":
        clear_user_state(user_id, "inline_reddit")
        await _open_manage_reddit_accounts(callback, user_id, username)
    elif action == "help":
        clear_user_state(user_id, "inline_help")
        await callback.message.answer(
            "❓ **Help Guide**\n\n"
            "📋 **Claim Task:** Receive a unique comment to post.\n"
            "📤 **Submit Proof:** Send `/submit <comment link>` in groups.\n"
            "📊 **My Stats:** Track your progress and earnings.",
            parse_mode=None,
        )
    elif action == "rules":
        clear_user_state(user_id, "inline_rules")
        await callback.message.answer(
            "📜 **System Rules**\n\n"
            "• **No Spam:** Do not attempt to game the system.\n"
            "• **No Fake Proofs:** We verify every link.\n"
            "• **Don't Delete:** Keep your comments live to get paid.",
            parse_mode=None,
        )
    elif action == "set_upi":
        set_user_state(user_id, {"flow": "set_upi"})
        await callback.message.answer(
            "Send your UPI using a command in groups:\n"
            "`/upi name@bank`\n\n"
            "Or open the bot in private chat and send the UPI ID.",
            parse_mode=None,
        )
    elif action == "upload_qr":
        set_user_state(user_id, {"flow": "upload_qr"})
        await callback.message.answer("Upload your payment QR image in private chat, or send it here as a reply/visible message.")
    elif action == "payment_history":
        rows = await get_payment_history(user_id)
        if not rows:
            await callback.message.answer("No payments yet.")
        else:
            lines = ["💰 PAYMENT HISTORY\n"]
            icons = {"paid": "✅", "pending": "⏳", "processing": "🔄", "failed": "❌", "rejected": "❌"}
            for payment in rows[:20]:
                icon = icons.get(payment["status"], "⏳")
                lines.append(f"{icon} {payment['amount']} {payment['status'].title()} - Task #{payment['task_id']}")
            lines.append(f"\n💵 Total Earned:\n{total_amount(rows, paid_only=True)}")
            await callback.message.answer("\n".join(lines))
    elif action == "total_earnings":
        rows = await get_payment_history(user_id)
        await callback.message.answer(f"💵 Total Earned:\n{total_amount(rows, paid_only=True)}")

    await callback.answer()


# Legacy help:topic:* callbacks — kept so any old inline buttons still in chat
# don't error out for a member who taps them. Just re-show the new help page.
@dp.callback_query(F.data.startswith("help:topic:"))
async def help_callback(callback: CallbackQuery):
    try:
        await callback.message.edit_text(
            "❓ HELP GUIDE\n\n"
            "Open ❓ Help from the menu for the latest help text."
        )
    except Exception:
        pass
    await callback.answer()


@dp.message(button_filter(BTN_BACK))
async def back(message: Message):
    log_handler_match(message, "back")
    log_button_click(message, BTN_BACK)
    state = user_states.get(message.from_user.id)
    if state and "previous_step" in state:
        state["step"] = state.pop("previous_step")
        await prompt_for_step(message, state)
        return
    clear_user_state(message.from_user.id, "back")
    await show_home(message)


@dp.message(F.photo)
async def handle_photo(message: Message):
    if not await require_real_user(message):
        return
    log_handler_match(message, "handle_photo")
    if not await is_admin(message.from_user.id):
        if await get_setting("maintenance_mode", "0") == "1":
            await message.answer("⚠️ **System Maintenance**", parse_mode=None)
            return
    state = user_states.get(message.from_user.id)
    if not state or state.get("flow") != "upload_qr":
        await message.answer("Use 💰 Payments → 🖼 Upload QR before sending a QR image.")
        return
    await register_user(message.from_user.id, message.from_user.username)
    file_id = message.photo[-1].file_id
    saved_qr = await save_qr_file_id(message.from_user.id, file_id)
    clear_user_state(message.from_user.id, "qr_saved")
    await message.answer(
        "✅ **QR Code Saved!**\n\nAdmins will see your QR when processing your payment.",
        reply_markup=payments_menu(),
        parse_mode=None
    )


async def send_review_queue(message_or_callback, page=1):
    rows = await get_pending_submissions()
    if not rows:
        text = "No submissions pending review."
        if isinstance(message_or_callback, CallbackQuery):
            await message_or_callback.message.edit_text(text)
        else:
            await message_or_callback.answer(text)
        return
    
    total = len(rows)
    if page > total: page = total
    if page < 1: page = 1
    row = rows[page-1]
    
    stats = await get_member_stats(row['user_id'])
    approval_rate = f"{stats['approval_rate']}%" if stats else "N/A"
    warnings = stats['warnings'] if stats else 0
    
    text = (
        f"🧾 **Review Queue ({page}/{total})**\n\n"
        f"👤 Member: @{row['username'] or 'no_username'}\n"
        f"📌 Task #{row['task_id']}\n"
        f"🔗 Reddit Proof:\n{row['reddit_link']}\n\n"
        f"⭐ Approval Rate: {approval_rate}\n"
        f"⚠ Warnings: {warnings}\n"
        f"💸 Amount: {row['payout_amount']}"
    )
    
    keyboard = [
        [
            InlineKeyboardButton(text="✅ Approve", callback_data=f"review:approve:{row['id']}"),
            InlineKeyboardButton(text="❌ Reject", callback_data=f"review:reject:{row['id']}"),
        ],
        [InlineKeyboardButton(text="⚠ Flag", callback_data=f"review:flag:{row['id']}")],
    ]
    keyboard.extend(get_pagination_keyboard(page, total, "review"))
    
    markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    if isinstance(message_or_callback, CallbackQuery):
        await message_or_callback.message.edit_text(text, reply_markup=markup, parse_mode=None, disable_web_page_preview=True)
    else:
        await message_or_callback.answer(text, reply_markup=markup, parse_mode=None, disable_web_page_preview=True)


@dp.callback_query(F.data.startswith("review:page:"))
async def review_page_callback(callback: CallbackQuery):
    log_callback_click(callback, "review_page")
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True)
        return
    page = int(callback.data.split(":")[2])
    await send_review_queue(callback, page)
    await callback.answer()


@dp.callback_query(F.data.startswith("review:approve:"))
async def approve_callback(callback: CallbackQuery, bot: Bot):
    log_callback_click(callback, "review_approve")
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True)
        return
    submission_id = int(callback.data.split(":")[2])
    submission = await approve_submission(submission_id, callback.from_user.id)
    if not submission:
        await callback.answer("Already reviewed or not found.", show_alert=True)
        return
    logging.info("Submission approved: id=%s admin=%s", submission_id, callback.from_user.id)
    await callback.message.edit_text(f"✅ Submission approved\n\nTask #{submission['task_id']}")
    try:
        await bot.send_message(
            submission["user_id"],
            "✅ Submission Approved\n\n💸 Payment added to queue."
        )
    except Exception:
        logging.exception("Could not notify member about approval")
    await callback.answer("Approved.")


@dp.callback_query(F.data.startswith("review:reject:"))
async def reject_callback(callback: CallbackQuery):
    log_callback_click(callback, "review_reject")
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True)
        return
    submission_id = int(callback.data.split(":")[2])
    set_user_state(callback.from_user.id, {"flow": "reject_reason", "submission_id": submission_id})
    await callback.message.answer("Send rejection reason.")
    await callback.answer()


@dp.callback_query(F.data.startswith("review:flag:"))
async def flag_callback(callback: CallbackQuery):
    log_callback_click(callback, "review_flag")
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True)
        return
    submission_id = int(callback.data.split(":")[2])
    await flag_submission(submission_id, None, "Admin flagged for review")
    logging.warning("Submission flagged: id=%s admin=%s", submission_id, callback.from_user.id)
    await callback.message.edit_text(f"⚠ Submission #{submission_id} flagged.")
    await callback.answer("Flagged.")

@dp.message(Command("ban"))
async def ban_command(message: Message, command: CommandObject):
    if not await is_admin(message.from_user.id): return
    if not command.args:
        await message.answer("Use: /ban user_id reason")
        return
    try:
        parts = command.args.split(maxsplit=1)
        user_id = int(parts[0])
        reason = parts[1] if len(parts) > 1 else "No reason"
    except (ValueError, IndexError):
        await message.answer("Invalid format. Use: /ban user_id reason")
        return
    await ban_user(user_id, reason)
    await log_audit_action(message.from_user.id, "ban", f"Banned {user_id}: {reason}")
    await message.answer(f"User {user_id} banned.")

@dp.message(Command("unban"))
async def unban_command(message: Message, command: CommandObject):
    if not await is_admin(message.from_user.id): return
    if not command.args:
        await message.answer("Use: /unban user_id")
        return
    try:
        user_id = int(command.args.strip())
    except ValueError:
        await message.answer("Invalid user ID. Use: /unban user_id")
        return
    await unban_user(user_id)
    await log_audit_action(message.from_user.id, "unban", f"Unbanned {user_id}")
    await message.answer(f"User {user_id} unbanned.")

@dp.message(Command("addnote"))
async def note_command(message: Message, command: CommandObject):
    if not await is_admin(message.from_user.id): return
    if not command.args:
        await message.answer("Use: /addnote user_id note text")
        return
    try:
        parts = command.args.split(maxsplit=1)
        if len(parts) < 2:
            await message.answer("Use: /addnote user_id note text")
            return
        user_id = int(parts[0])
        note = parts[1]
    except ValueError:
        await message.answer("Invalid user ID. Use: /addnote user_id note text")
        return
    await add_member_note(user_id, note)
    await message.answer(f"Note added to {user_id}.")

@dp.message(Command("warn"))
async def warn_command(message: Message, command: CommandObject):
    if not await is_admin(message.from_user.id): return
    if not command.args:
        await message.answer("Use: /warn user_id")
        return
    try:
        user_id = int(command.args.strip())
    except ValueError:
        await message.answer("Invalid user ID. Use: /warn user_id")
        return
    count = await add_member_warning(user_id)
    await log_audit_action(message.from_user.id, "warn", f"Warned {user_id}")
    await message.answer(f"User {user_id} warned. Total warnings: {count}")
    if count >= 3:
        await ban_user(user_id, "Auto-banned due to excessive warnings.")
        await log_audit_action(message.from_user.id, "ban", f"Auto-banned {user_id}: reached {count} warnings")
        await message.answer(f"User {user_id} auto-banned for reaching {count} warnings.")

@dp.message(Command("dailystats"))
@dp.message(button_filter(BTN_ANALYTICS_DAILY))
async def command_dailystats(message: Message):
    if not await is_admin(message.from_user.id): return
    stats = await get_daily_stats()
    await message.answer(
        "📅 **DAILY STATS**\n\n"
        f"✅ Tasks Completed: {stats['tasks_completed']}\n"
        f"❌ Tasks Rejected: {stats['tasks_rejected']}\n"
        f"📌 Claims Today: {stats['claims_today']}\n"
        f"👥 Active Members: {stats['active_members']}\n"
        f"💰 Payments Sent: ₹{stats['payments_sent_today']:.2f}",
        parse_mode=None
    )

async def list_members(message_or_callback, page=1):
    member_ids = await get_all_member_ids()
    if not member_ids:
        if isinstance(message_or_callback, CallbackQuery):
            await message_or_callback.message.answer("No members found.")
        else:
            await message_or_callback.answer("No members found.")
        return

    total = len(member_ids)
    per_page = 10
    total_pages = (total + per_page - 1) // per_page
    if page > total_pages: page = total_pages
    if page < 1: page = 1
    
    start_idx = (page - 1) * per_page
    end_idx = start_idx + per_page
    current_ids = member_ids[start_idx:end_idx]
    
    lines = [f"👥 **Members List ({page}/{total_pages})**\n"]
    for mid in current_ids:
        stats = await get_member_stats(mid)
        username = f"@{stats.get('username')}" if stats and stats.get('username') else f"ID {mid}"
        lines.append(f"• {username} — `{mid}`")
        
    text = "\n".join(lines)
    keyboard = get_pagination_keyboard(page, total_pages, "members")
    markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    if isinstance(message_or_callback, CallbackQuery):
        await message_or_callback.message.edit_text(text, reply_markup=markup, parse_mode=None)
    else:
        await message_or_callback.answer(text, reply_markup=markup, parse_mode=None)


@dp.callback_query(F.data.startswith("members:page:"))
async def members_page_callback(callback: CallbackQuery):
    log_callback_click(callback, "members_page")
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True)
        return
    page = int(callback.data.split(":")[2])
    await list_members(callback, page)
    await callback.answer()

@dp.message(Command("leaderboard"))
async def command_leaderboard(message: Message):
    # Leaderboard removed to avoid member ranking
    await message.answer("💡 Use 📊 **My Stats** to view your personal progress and earnings.", parse_mode=None)

async def archived_tasks(message: Message):
    if not await is_admin(message.from_user.id):
        return
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
        SELECT tasks.id, tasks.subreddit, tasks.payout_amount, tasks.total_slots,
               tasks.closed_at, COUNT(DISTINCT submissions.id) AS submissions
        FROM tasks
        LEFT JOIN submissions ON submissions.task_id = tasks.id
        WHERE tasks.status IN ('archived', 'closed')
        GROUP BY tasks.id
        ORDER BY tasks.closed_at DESC
        LIMIT 10
        """)
        rows = [dict(r) for r in await cursor.fetchall()]
    if not rows:
        await message.answer("No archived or closed tasks.")
        return
    lines = ["🗃 **Archived Tasks** (last 10)\n"]
    for t in rows:
        closed = (t['closed_at'] or '')[:10]
        lines.append(f"• Task #{t['id']} r/{t['subreddit']} | {t['payout_amount']} | {t['submissions']} subs | {closed}")
    await message.answer("\n".join(lines), parse_mode=None)


async def review_history(message: Message):
    if not await is_admin(message.from_user.id):
        return
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
        SELECT submissions.id, submissions.username, submissions.status,
               submissions.reviewed_at, tasks.subreddit, tasks.payout_amount
        FROM submissions
        JOIN tasks ON tasks.id = submissions.task_id
        WHERE submissions.status IN ('approved', 'rejected')
        ORDER BY submissions.reviewed_at DESC
        LIMIT 15
        """)
        rows = [dict(r) for r in await cursor.fetchall()]
    if not rows:
        await message.answer("No reviewed submissions yet.")
        return
    icons = {"approved": "✅", "rejected": "❌"}
    lines = ["📜 **Recent Reviews** (last 15)\n"]
    for s in rows:
        icon = icons.get(s['status'], "❓")
        date = (s['reviewed_at'] or '')[:10]
        lines.append(f"{icon} #{s['id']} @{s['username'] or '?'} r/{s['subreddit']} {s['payout_amount']} {date}")
    await message.answer("\n".join(lines), parse_mode=None)


async def paid_payments_history(message: Message):
    if not await is_admin(message.from_user.id):
        return
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
        SELECT payments.id, payments.username, payments.amount,
               payments.paid_at, payments.task_id
        FROM payments
        WHERE payments.status = 'paid'
        ORDER BY payments.paid_at DESC
        LIMIT 15
        """)
        rows = [dict(r) for r in await cursor.fetchall()]
    if not rows:
        await message.answer("No paid payments yet.")
        return
    lines = ["✅ **Paid History** (last 15)\n"]
    for p in rows:
        date = (p['paid_at'] or '')[:10]
        lines.append(f"✅ #{p['id']} @{p['username'] or '?'} {p['amount']} Task #{p['task_id']} {date}")
    await message.answer("\n".join(lines), parse_mode=None)


async def payment_stats_handler(message: Message):
    if not await is_admin(message.from_user.id):
        return
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("""
        SELECT
            COUNT(*) FILTER (WHERE status = 'pending') AS pending,
            COUNT(*) FILTER (WHERE status = 'paid') AS paid,
            COUNT(*) FILTER (WHERE status = 'failed') AS failed
        FROM payments
        """)
        row = await cursor.fetchone()
        cursor2 = await db.execute("SELECT amount FROM payments WHERE status = 'paid'")
        paid_rows = await cursor2.fetchall()
        
    def extract_float(s):
        match = re.search(r'[\d.]+', s)
        return float(match.group()) if match else 0.0

    total = sum(extract_float(r[0]) for r in paid_rows)
    await message.answer(
        f"📊 **Payment Stats**\n\n"
        f"⏳ Pending: {row[0]}\n"
        f"✅ Paid: {row[1]}\n"
        f"❌ Failed/Cancelled: {row[2]}\n"
        f"💰 Total Paid Out: ₹{total:.2f}",
        parse_mode=None,
    )


async def warned_members(message: Message):
    if not await is_admin(message.from_user.id):
        return
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
        SELECT telegram_id, username, warnings
        FROM users
        WHERE warnings > 0
        ORDER BY warnings DESC
        LIMIT 20
        """)
        rows = [dict(r) for r in await cursor.fetchall()]
    if not rows:
        await message.answer("No warned members.")
        return
    lines = ["⚠ **Warned Members**\n"]
    for u in rows:
        lines.append(f"⚠ {u['warnings']}x @{u['username'] or '?'} (`{u['telegram_id']}`)")
    await message.answer("\n".join(lines), parse_mode=None)


async def banned_members(message: Message):
    if not await is_admin(message.from_user.id):
        return
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
        SELECT telegram_id, username
        FROM users
        WHERE is_banned = 1
        ORDER BY telegram_id DESC
        LIMIT 20
        """)
        rows = [dict(r) for r in await cursor.fetchall()]
    if not rows:
        await message.answer("No restricted accounts.")
        return
    lines = ["🚫 **Restricted Accounts**\n"]
    for u in rows:
        lines.append(f"🚫 @{u['username'] or '?'} (`{u['telegram_id']}`)")
    await message.answer("\n".join(lines), parse_mode=None)


async def earnings_stats_handler(message: Message):
    if not await is_admin(message.from_user.id):
        return
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
        SELECT payments.username, payments.user_id, payments.amount
        FROM payments
        WHERE status = 'paid'
        """)
        rows = [dict(r) for r in await cursor.fetchall()]
        
    def extract_float(s):
        match = re.search(r'[\d.]+', str(s))
        return float(match.group()) if match else 0.0

    # Group by user in Python
    grouped = {}
    for r in rows:
        uid = r['user_id']
        if uid not in grouped:
            grouped[uid] = {'username': r['username'], 'total': 0.0, 'count': 0}
        grouped[uid]['total'] += extract_float(r['amount'])
        grouped[uid]['count'] += 1
    
    sorted_earners = sorted(grouped.items(), key=lambda x: x[1]['total'], reverse=True)[:10]
    
    if not sorted_earners:
        await message.answer("No earnings data yet.")
        return
        
    lines = ["💰 **Top Earners**\n"]
    for i, (uid, data) in enumerate(sorted_earners, 1):
        name = f"@{data['username']}" if data['username'] else f"ID:{uid}"
        lines.append(f"{i}. {name} — ₹{data['total']:.2f} ({data['count']} payments)")
    await message.answer("\n".join(lines), parse_mode=None)


@dp.error()
async def handle_error(event: ErrorEvent):
    logging.error(
        "Update failed",
        exc_info=(type(event.exception), event.exception, event.exception.__traceback__),
    )
    return True


# ─── Comment Live Checker ───────────────────────────────────────────────────

_REDDIT_UA = "VIRON-TaskBot/1.0 (comment liveness checker)"
_REDDIT_API = "https://www.reddit.com/api/info.json"
_LIVE_CHECK_INTERVAL_SECONDS = 30 * 60  # 30 minutes between sweeps


async def _fetch_reddit_comment_meta(comment_id):
    """One-shot lookup that returns the comment author + liveness for proof submit.

    Returns a dict ``{"author": str|None, "status": "live"|"removed"|"deleted"|
    "shadow_removed"|"error"}``. Never raises; transport errors map to 'error'.
    """
    import aiohttp
    timeout = aiohttp.ClientTimeout(total=15, connect=8)
    try:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            url = f"{_REDDIT_API}?id=t1_{comment_id}"
            async with session.get(url, headers={"User-Agent": _REDDIT_UA}) as resp:
                if resp.status == 404:
                    return {"author": None, "status": "shadow_removed"}
                if resp.status != 200:
                    return {"author": None, "status": "error"}
                data = await resp.json(content_type=None)
        children = (data.get("data") or {}).get("children") or []
        if not children:
            return {"author": None, "status": "shadow_removed"}
        comment = (children[0].get("data") or {})
        body = (comment.get("body") or "").strip().lower()
        author_raw = (comment.get("author") or "").strip()
        author = author_raw.lower() if author_raw else None
        if comment.get("removed_by_category") or body == "[removed]":
            return {"author": author, "status": "removed"}
        if body == "[deleted]" or (author and author == "[deleted]"):
            return {"author": None, "status": "deleted"}
        return {"author": author, "status": "live"}
    except Exception:
        logging.exception("Reddit author lookup failed: cid=%s", comment_id)
        return {"author": None, "status": "error"}


async def _check_reddit_comment_alive(session, comment_id):
    """Return granular liveness: 'live'|'removed'|'deleted'|'shadow_removed'|'error'.

    - 'live'           : comment exists with content + author
    - 'removed'        : moderator/admin removed (body=[removed] or removed_by_category set)
    - 'deleted'        : the author deleted their own comment (body=[deleted] or author=[deleted])
    - 'shadow_removed' : not found at all (404 / empty children) — likely shadowban or hard delete
    - 'error'          : network/parse failure; safe to retry next sweep
    """
    try:
        url = f"{_REDDIT_API}?id=t1_{comment_id}"
        async with session.get(url, headers={"User-Agent": _REDDIT_UA}, timeout=15) as resp:
            if resp.status == 404:
                return "shadow_removed"
            if resp.status != 200:
                logging.warning("Reddit check non-200: cid=%s status=%s", comment_id, resp.status)
                return "error"
            data = await resp.json(content_type=None)

        children = (data.get("data") or {}).get("children") or []
        if not children:
            return "shadow_removed"

        comment = (children[0].get("data") or {})
        body = (comment.get("body") or "").strip().lower()
        author = (comment.get("author") or "").strip().lower()

        if comment.get("removed_by_category"):
            return "removed"
        if body == "[removed]":
            return "removed"
        if body == "[deleted]" or author == "[deleted]":
            return "deleted"

        return "live"
    except asyncio.TimeoutError:
        logging.warning("Reddit check timeout: cid=%s", comment_id)
        return "error"
    except Exception as e:
        logging.exception("Reddit check error: cid=%s error=%s", comment_id, str(e))
        return "error"


async def comment_live_checker_task(bot: Bot):
    """Background coroutine: periodically verifies Reddit comments are still live.

    Per-submission errors never kill the loop; they are caught, logged, and the
    submission is left for the next sweep. Iteration-level errors are also caught.
    """
    import aiohttp
    await asyncio.sleep(60)  # let the bot warm up first
    logging.info("Comment live checker started (interval=%ss)", _LIVE_CHECK_INTERVAL_SECONDS)
    while True:
        sweep_started = datetime.now(timezone.utc).isoformat()
        try:
            subs = await get_submissions_to_check(max_age_days=7, recheck_after_hours=12, limit=50)
            counts = {"live": 0, "removed": 0, "deleted": 0, "shadow_removed": 0, "error": 0}
            if subs:
                timeout = aiohttp.ClientTimeout(total=20, connect=10)
                async with aiohttp.ClientSession(timeout=timeout) as session:
                    for s in subs:
                        try:
                            status = await _check_reddit_comment_alive(session, s["reddit_comment_id"])
                            await update_submission_comment_status(s["id"], status)
                            counts[status] = counts.get(status, 0) + 1
                            logging.info(
                                "Comment check: submission=%s cid=%s status=%s",
                                s["id"], s["reddit_comment_id"], status,
                            )
                            # Notify worker on transition to dead (first time we see it dead)
                            if status in ("removed", "deleted", "shadow_removed") and s.get("live_status") != status:
                                try:
                                    reason_text = {
                                        "removed": "removed by Reddit moderators or admin",
                                        "deleted": "deleted (the comment or its author was removed)",
                                        "shadow_removed": "no longer visible on Reddit (possible shadowban)",
                                    }[status]
                                    await bot.send_message(
                                        s["user_id"],
                                        f"⚠️ Comment {status.replace('_', ' ').title()}\n\n"
                                        f"Your Reddit comment for Task #{s['task_id']} was {reason_text}.\n\n"
                                        "Submission flagged. It will not count toward your next payout unless restored.",
                                        parse_mode=None,
                                    )
                                except Exception:
                                    logging.warning("Could not notify user=%s about %s comment", s["user_id"], status)
                        except Exception:
                            logging.exception(
                                "Per-submission check failed: submission=%s cid=%s",
                                s.get("id"), s.get("reddit_comment_id"),
                            )
                            try:
                                await update_submission_comment_status(s["id"], "error")
                            except Exception:
                                logging.exception("Could not record 'error' status for submission=%s", s.get("id"))
                        # Reddit anon rate limit ~60/min
                        await asyncio.sleep(1.2)
            logging.info(
                "Live check sweep done: started=%s checked=%s counts=%s",
                sweep_started, len(subs) if subs else 0, counts,
            )
        except Exception:
            logging.exception("Comment live checker iteration failed")
        await asyncio.sleep(_LIVE_CHECK_INTERVAL_SECONDS)


async def claim_expiry_task(bot: Bot):
    """Background coroutine: periodically releases expired claims and notifies users."""
    await asyncio.sleep(120)  # let the bot warm up
    logging.info("Claim expiry task started")
    while True:
        try:
            expired_claims, timeout_min = await auto_cleanup_claims()
            if expired_claims:
                logging.info("Cleaned up %s expired claims (timeout=%s min)", len(expired_claims), timeout_min)
                for claim in expired_claims:
                    if claim.get("assigned_to"):
                        try:
                            await bot.send_message(
                                claim["assigned_to"],
                                "⌛ **Claim Expired**\n\n"
                                f"Your claim for **Task #{claim['task_id']}** has expired due to inactivity.\n\n"
                                "The comment and slot have been released. You can claim a new task when you're ready!",
                                parse_mode=None
                            )
                        except Exception:
                            logging.warning("Could not notify user=%s about expired claim", claim["assigned_to"])
        except Exception:
            logging.exception("Claim expiry task iteration failed")
        await asyncio.sleep(5 * 60)  # Check every 5 minutes


@dp.message()
async def handle_text(message: Message, bot: Bot):
    # This catch-all handler should be at the end of the module.
    # It handles state-based input and buttons that don't have specific decorators.
    log_handler_match(message, "handle_text")
    if not message.text:
        return
    if not await require_real_user(message):
        return
    
    text_lower = message.text.strip().lower()
    key = button_key(message.text)
    
    # Global Admin bypass for maintenance check
    is_user_admin = await is_admin(message.from_user.id)
    if not is_user_admin and await get_setting("maintenance_mode", "0") == "1":
        await message.answer("⚠️ **System Maintenance**\n\nThe system is temporarily offline for maintenance. Please check back later.", parse_mode=None)
        return

    # Global navigation handling
    if text_lower in {"home", "🏠 home", "/start"} or key == BTN_HOME:
        clear_user_state(message.from_user.id, "global_home")
        await show_home(message)
        return

    if text_lower in {"cancel", "❌ cancel"} or key == BTN_CANCEL:
        clear_user_state(message.from_user.id, "global_cancel")
        await message.answer("❌ **Action Cancelled**", parse_mode=None)
        await show_home(message)
        return
        
    if text_lower in {"back", "⬅️ back"} or key == BTN_BACK:
        state = user_states.get(message.from_user.id)
        if state and "previous_step" in state:
            state["step"] = state.pop("previous_step")
            await prompt_for_step(message, state)
            return
        else:
            clear_user_state(message.from_user.id, "global_back")
            await show_home(message)
            return

    state = user_states.get(message.from_user.id)
    button_match = button_key(message.text)

    # Admin only button check
    admin_only_buttons = {
        BTN_ADMIN_TASKS, BTN_ADMIN_REVIEWS, BTN_ADMIN_PAYMENTS, BTN_ADMIN_MEMBERS,
        BTN_ADMIN_ANALYTICS, BTN_ADMIN_BROADCAST, BTN_ADMIN_SETTINGS,
        BTN_TASKS_NEW, BTN_TASKS_ADD_COMMENTS, BTN_TASKS_STATS, BTN_TASKS_MANAGE,
        BTN_TASKS_ARCHIVED, BTN_TASKS_REOPEN, BTN_TASKS_CLONE, BTN_TASKS_RESET,
        BTN_REVIEWS_PENDING, BTN_REVIEWS_FLAGGED, BTN_REVIEWS_HISTORY,
        BTN_PAYMENTS_PROCESS, BTN_LIVE_DASHBOARD, BTN_PAYMENTS_PENDING, BTN_PAYMENTS_PAID, BTN_PAYMENTS_STATS,
        BTN_MEMBERS_SEARCH, BTN_MEMBERS_WARNED, BTN_MEMBERS_BANNED,
        BTN_ANALYTICS_DAILY, BTN_ANALYTICS_SYSTEM, BTN_ANALYTICS_EARNINGS,
        BTN_SETTINGS_CLEANUP, BTN_SETTINGS_BACKUP,
    }
    if button_match in admin_only_buttons and not is_user_admin:
        logging.warning("Blocked admin button for non-admin: user=%s button=%s", message.from_user.id, button_match)
        await message.answer("That option is only available to admins.", reply_markup=await main_menu(message.from_user.id))
        return
    
    if state:
        # Button taps interrupt non-free-text flows
        free_text_flows = {"broadcast_compose", "reject_reason", _SEND_MSG_FLOW, "add_comments"}
        if button_match and button_match not in {BTN_BACK, BTN_HOME, BTN_CANCEL} \
                and state.get("flow") not in free_text_flows:
            clear_user_state(message.from_user.id, "button_interruption")
            # Continue to handle the button click below
        else:
            await continue_state_flow(message, state, bot)
            return

    # Handle remaining buttons not covered by specific decorators
    if button_match == BTN_ADMIN_TASKS: await message.answer("📂 Tasks Management", reply_markup=admin_tasks_menu())
    elif button_match == BTN_ADMIN_REVIEWS: await message.answer("🧾 Reviews Queue", reply_markup=admin_reviews_menu())
    elif button_match == BTN_ADMIN_PAYMENTS: await message.answer("💸 Payments Management", reply_markup=admin_payments_menu())
    elif button_match == BTN_ADMIN_MEMBERS: await message.answer("👥 Members Management", reply_markup=admin_members_menu())
    elif button_match == BTN_ADMIN_ANALYTICS: await message.answer("📊 System Analytics", reply_markup=admin_analytics_menu())
    elif button_match == BTN_TASKS_ARCHIVED: await archived_tasks(message)
    elif button_match == BTN_REVIEWS_HISTORY: await review_history(message)
    elif button_match == BTN_PAYMENTS_PAID: await paid_payments_history(message)
    elif button_match == BTN_PAYMENTS_STATS: await payment_stats_handler(message)
    elif button_match == BTN_MEMBERS_WARNED: await warned_members(message)
    elif button_match == BTN_MEMBERS_BANNED: await banned_members(message)
    elif button_match == BTN_ANALYTICS_SYSTEM: await system_stats_handler(message)
    elif button_match == BTN_ANALYTICS_EARNINGS: await earnings_stats_handler(message)
    elif button_match == BTN_PAYMENT_HISTORY: await payment_history(message)
    elif button_match == BTN_TOTAL_EARNINGS: await total_earnings(message)
    
    # Redundant but safe fallbacks for primary buttons if their decorators failed
    elif button_match == BTN_CLAIM: await claim(message)
    elif button_match == BTN_SUBMIT: await start_submit_flow(message)
    elif button_match == BTN_MY_STATS: await my_stats(message)
    elif button_match == BTN_PAYMENTS: await payments_menu_open(message)
    elif button_match == BTN_REDDIT_ACCOUNTS: await open_reddit_accounts(message)
    elif button_match == BTN_RULES: await rules(message)
    elif button_match == BTN_HELP: await help_button(message)

    else:
        # Default behavior: show menu
        logging.info("Fallback handler: user=%s text=%r", message.from_user.id, message.text)
        await message.answer("Choose an option from the menu.", reply_markup=await main_menu(message.from_user.id))


async def continue_state_flow(message, state, bot):
    flow = state.get("flow")
    text = message.text.strip()
    logging.info(
        "State input: user=%s active_state=%s text=%r",
        message.from_user.id,
        describe_state(state),
        text,
    )
    if flow == "new_task":
        await continue_new_task(message, state, text)
    elif flow == "add_comments":
        await continue_add_comments(message, state, text)
    elif flow == "task_status":
        await continue_task_status(message, state, text)
    elif flow == "submit_proof":
        await handle_submit_link(message, text)
    elif flow == "add_reddit_account":
        await continue_add_reddit_account(message, bot)
    elif flow == "reddit_reject_reason":
        await _continue_reddit_reject_reason(message, state, bot)
    elif flow == "set_upi":
        if not valid_upi_id(text):
            await message.answer(
                "❌ That doesn't look like a valid UPI ID.\n\n"
                "Format: yourname@bank  (e.g. raju@okaxis, priya@upi)\n\n"
                "Try again:"
            )
            return
        await save_upi_id(message.from_user.id, text)
        clear_user_state(message.from_user.id, "upi_saved")
        logging.info("UPI saved: user=%s", message.from_user.id)
        await message.answer(
            f"✅ UPI ID saved!\n\n💳 {text}\n\nPayments will be sent to this address.",
            reply_markup=payments_menu(),
        )
    elif flow == "member_stats":
        member_id = None
        if text.startswith("@"):
            member_id = await get_user_by_username(text)
        elif text.isdigit():
            member_id = int(text)
            
        if not member_id:
            await message.answer("❌ Member not found.")
            return
            
        stats = await get_member_stats(member_id)
        if not stats:
            await message.answer("❌ Stats not found.")
            return
            
        status_line = "✅ Active"
        if stats["is_banned"]: status_line = "🚫 Banned"
        elif stats["is_shadowbanned"]: status_line = "👻 Shadowbanned"
        
        username = stats.get("username")
        name_line = f"@{username}" if username else "(no username)"
        await message.answer(
            "====================================\n\n"
            "👤 Member Information\n\n"
            f"Username:\n{name_line}\n\n"
            f"ID:\n`{member_id}`\n\n"
            f"Status:\n{status_line}\n\n"
            f"Completed:\n{stats['approved']}\n\n"
            f"Pending:\n{stats['pending']}\n\n"
            f"Rejected:\n{stats['rejected']}\n\n"
            f"Warnings:\n{stats['warnings']}\n\n"
            f"Notes:\n{stats['notes'] or 'No notes'}\n\n"
            "====================================",
            parse_mode=None,
        )
        clear_user_state(message.from_user.id, "member_stats_done")
    elif flow == _SEND_MSG_FLOW:
        await _handle_process_send_message(message, state, bot)
    elif flow == "broadcast_compose":
        audience = state["audience"]
        member_ids = await get_all_member_ids(active_only=audience == "active")
        count = len(member_ids)
        set_user_state(message.from_user.id, {"flow": "broadcast_confirm", "audience": audience, "text": text})
        confirm_keyboard = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text=f"📢 Send to {count} member(s)", callback_data="broadcast:send"),
            InlineKeyboardButton(text="❌ Cancel", callback_data="broadcast:cancel"),
        ]])
        await message.answer(
            f"📢 Broadcast Preview\n\n{text}\n\n"
            f"This will be sent to {count} member(s).",
            reply_markup=confirm_keyboard,
        )
    elif flow == "set_claim_timeout":
        if not text.isdigit() or int(text) <= 0 or int(text) > 1440:
            await message.answer("Send a number between 1 and 1440 (minutes).")
            return
        await set_setting("claim_timeout_minutes", text)
        clear_user_state(message.from_user.id, "timeout_set")
        await message.answer(f"✅ Claim timeout set to {text} minutes.")
    elif flow == "reject_reason":
        submission = await reject_submission(state["submission_id"], message.from_user.id, text)
        clear_user_state(message.from_user.id, "reject_reason_saved")
        if not submission:
            await message.answer("Submission not found.")
            return
        logging.info("Submission rejected: id=%s admin=%s", state["submission_id"], message.from_user.id)
        try:
            await bot.send_message(
                submission["user_id"],
                f"❌ Submission Not Approved\n\n"
                f"Unfortunately your submission for Task #{submission.get('task_id', '?')} was not approved.\n\n"
                f"Reason: {text}\n\n"
                "If you believe this is a mistake, please contact an admin.",
            )
        except Exception:
            logging.exception("Could not notify member about rejection")
        await message.answer(f"✅ Submission rejected and member notified.")
    elif flow == "payment_session":
        # Payment Session is callback-driven (inline buttons). Don't destroy
        # the session if the admin types free text by accident — just nudge them.
        await message.answer(
            "💸 Payment Session is active. Use the inline buttons on the card "
            "(Mark Paid / Skip / Next / End Session) — text input is ignored here."
        )
    elif flow == "upload_qr":
        # User is expected to send a photo, not text. Don't clear state.
        await message.answer(
            "📸 Please send a photo of your payment QR code, not text.\n\n"
            "Tap the attachment icon → Gallery, then choose your QR image.\n"
            "Or tap ❌ Cancel to abort."
        )
    elif flow == "broadcast_confirm":
        # Waiting for inline Send/Cancel — don't clear state on stray text.
        await message.answer("Tap the inline 📢 Send or ❌ Cancel button to finish.")
    else:
        logging.warning("Unknown flow in continue_state_flow: user=%s flow=%s", message.from_user.id, flow)
        clear_user_state(message.from_user.id, "unknown_flow")
        await show_home(message)


def back_cancel_keyboard():
    buttons = [
        [KeyboardButton(text=BTN_BACK), KeyboardButton(text=BTN_CANCEL)],
        [KeyboardButton(text=BTN_HOME)],
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


async def prompt_for_step(message, state):
    """Re-show the input prompt for the current step without trying to validate any input.

    Called by the Back button handler so the user gets the correct question
    for the step they reverted to, instead of seeing a validation error.
    """
    flow = state.get("flow")
    step = state.get("step")

    if flow == "new_task":
        prompts = {
            "post_url": ("Send the Reddit POST URL.", back_cancel_keyboard()),
            "payout":   ("💰 Send payout amount (e.g., ₹10):", back_cancel_keyboard()),
            "slots":    ("👥 Send total slots as a number:", back_cancel_keyboard()),
        }
        if step in prompts:
            text, markup = prompts[step]
            await message.answer(text, reply_markup=markup)
            return
        if step == "category":
            await message.answer("Choose task category.", reply_markup=category_keyboard())
            await message.answer("Or use buttons below to navigate:", reply_markup=back_cancel_keyboard())
            return
        if step == "priority":
            await message.answer("Choose task priority:", reply_markup=priority_keyboard())
            return
        if step == "instructions":
            await message.answer(
                "Send task instructions for members (or /skip to leave blank).",
                reply_markup=back_cancel_keyboard(),
            )
            return
        if step == "preview":
            await _show_task_preview(message, state)
            return

    if flow == "add_comments":
        if step == "task_id":
            await message.answer("Send the task ID.", reply_markup=back_cancel_keyboard())
            return
        if step == "comments":
            task_id = state.get("task_id", "?")
            await message.answer(
                f"📝 Task #{task_id} selected.\n\n" + _ADD_COMMENTS_INSTRUCTIONS,
                reply_markup=back_cancel_keyboard(),
            )
            return
        if step == "preview":
            await message.answer(
                "🔍 Preview is active — use the inline buttons above to ✅ Confirm or ❌ Cancel.",
                parse_mode=None,
            )
            return

    # For any other flow, going back just returns home
    clear_user_state(message.from_user.id, "back_to_home")
    await show_home(message)


async def continue_new_task(message, state, text):
    if state["step"] == "post_url":
        details, parse_error = await parse_reddit_post_url_for_task(text)
        if not details:
            if parse_error == "resolve_failed":
                await message.answer(
                    "I could not open that Reddit share link. Send the full Reddit post URL, or try the share link again.",
                    reply_markup=back_cancel_keyboard(),
                )
            else:
                await message.answer("Invalid Reddit post URL. Please send a valid link or use buttons below.", reply_markup=back_cancel_keyboard())
            return

        existing_tasks = await get_tasks_by_post_id(details["post_id"])
        if existing_tasks and not state.get("allow_duplicate"):
            state["post_url"] = details.get("resolved_url", text)
            state["details"] = details
            state["duplicate_tasks"] = existing_tasks
            state["step"] = "duplicate_confirm"
            summary = duplicate_task_summary(existing_tasks)
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="✅ Post Again", callback_data="task:duplicate:confirm")],
                [InlineKeyboardButton(text="🔒 Manage Existing", callback_data="task:duplicate:manage")],
                [InlineKeyboardButton(text="❌ Cancel", callback_data="task:duplicate:cancel")],
            ])
            await message.answer(
                "⚠️ This Reddit post already exists in the system.\n\n"
                f"{summary}\n\n"
                "Admin control: choose **Post Again** if you want to create a fresh task for the same post.",
                reply_markup=keyboard,
                parse_mode=None,
            )
            return

        state["post_url"] = details.get("resolved_url", text)
        state["details"] = details
        state["previous_step"] = "post_url"
        state["step"] = "payout"
        logging.info("Task creation advanced: admin=%s step=payout", message.from_user.id)
        await message.answer("Send payout amount. Example: ₹10", reply_markup=back_cancel_keyboard())
        return
    if state["step"] == "duplicate_confirm":
        await message.answer("Use the confirmation buttons: ✅ Post Again, 🔒 Manage Existing, or ❌ Cancel.")
        return
    if state["step"] == "payout":
        if len(text) > 30:
            await message.answer("Payout text is too long. Keep it under 30 chars.", reply_markup=back_cancel_keyboard())
            return
        state["payout"] = text
        state["previous_step"] = "payout"
        state["step"] = "slots"
        logging.info("Task creation advanced: admin=%s step=slots", message.from_user.id)
        await message.answer("Send total slots as a number.", reply_markup=back_cancel_keyboard())
        return
    if state["step"] == "slots":
        if not text.isdigit() or int(text) <= 0:
            await message.answer("Slots must be a positive number.", reply_markup=back_cancel_keyboard())
            return
        state["slots"] = int(text)
        state["previous_step"] = "slots"
        state["step"] = "category"
        logging.info("Task creation advanced: admin=%s step=category", message.from_user.id)
        await message.answer("Choose task category.", reply_markup=category_keyboard())
        # We also send the back/cancel keyboard so they can exit if they want
        await message.answer("Or use buttons below to navigate:", reply_markup=back_cancel_keyboard())
        return
    if state["step"] == "priority":
        # Handled by callback
        await message.answer("Please use the buttons above to choose priority.", reply_markup=priority_keyboard())
        return
    if state["step"] == "instructions":
        instructions = "" if text == "/skip" else text
        state["instructions"] = instructions
        state["previous_step"] = "instructions"
        state["step"] = "preview"
        await _show_task_preview(message, state)
        return
    if state["step"] == "preview":
        # Wait for inline button — text input here just re-shows the preview
        await _show_task_preview(message, state)
        return


async def _show_task_preview(message, state):
    details = state["details"]
    timeout_min = int(await get_setting("claim_timeout_minutes", "30"))
    instructions = (state.get("instructions") or "(none)").strip() or "(none)"
    text = (
        "====================================\n\n"
        "📌 TASK PREVIEW\n\n"
        f"📍 Subreddit: r/{details['subreddit']}\n"
        f"🏷 Category: {state['category']}\n"
        f"💸 Payout: {state['payout']}\n"
        f"👥 Slots: {state['slots']}\n"
        f"⏰ Claim Timeout: {timeout_min} min\n"
        f"🚦 Priority: {state.get('priority', 'normal').title()}\n\n"
        f"📋 Instructions:\n{instructions}\n\n"
        f"🔗 Post: {state['post_url']}\n\n"
        "===================================="
    )
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Publish", callback_data="task:preview:publish"),
            InlineKeyboardButton(text="✏ Edit", callback_data="task:preview:edit"),
        ],
        [InlineKeyboardButton(text="❌ Cancel", callback_data="task:preview:cancel")],
    ])
    await message.answer(text, reply_markup=keyboard)


@dp.callback_query(F.data == "task:preview:publish")
async def task_preview_publish(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True)
        return
    state = user_states.get(callback.from_user.id)
    if not state or state.get("flow") != "new_task" or state.get("step") != "preview":
        await callback.answer("Preview not active.", show_alert=True)
        return
    details = state["details"]
    task_id = await create_task(
        state["post_url"], details["normalized_url"], details["subreddit"],
        details["post_id"], details["post_path"], state["payout"],
        state["slots"], state["category"], state.get("instructions", ""),
        callback.from_user.id,
        priority=state.get("priority", "normal"),
        minimum_level=state.get("min_level", "Beginner"),
    )
    logging.info("Task created via preview: id=%s admin=%s", task_id, callback.from_user.id)
    await log_audit_action(callback.from_user.id, "task_create", f"Task #{task_id} via preview")
    set_user_state(callback.from_user.id, {
        "flow": "add_comments", "step": "comments", "task_id": task_id, "buffer": [],
        "session_id": _addc_new_session_id(),
    })
    await callback.message.edit_text(f"✅ Task #{task_id} published!")
    await callback.message.answer(
        f"📝 Now send the comments for Task #{task_id}.\n\n"
        + _ADD_COMMENTS_INSTRUCTIONS
        + "\n\nSend /skip to add comments later.",
        reply_markup=back_cancel_keyboard(),
    )
    await callback.answer("Published.")


@dp.callback_query(F.data == "task:preview:edit")
async def task_preview_edit(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True)
        return
    state = user_states.get(callback.from_user.id)
    if not state or state.get("flow") != "new_task":
        await callback.answer("Preview not active.", show_alert=True)
        return
    state["step"] = "instructions"
    state["previous_step"] = "priority"
    await callback.message.edit_text("✏ Editing — re-send instructions (or /skip):")
    await callback.answer()


@dp.callback_query(F.data == "task:preview:cancel")
async def task_preview_cancel(callback: CallbackQuery):
    clear_user_state(callback.from_user.id, "task_preview_cancel")
    await callback.message.edit_text("❌ Task creation cancelled.")
    await callback.answer("Cancelled.")


SEPARATOR_LITERAL = "//ADD//"
SEPARATOR_RE = re.compile(r"//\s*ADD\s*//", re.IGNORECASE)
TELEGRAM_MSG_LIMIT = 4096
SAFE_MSG_LIMIT = 4000  # leave headroom for our own framing


def parse_comments_strict(text):
    """Strict //ADD// parser. Returns dict.

    Splits ONLY on the //ADD// marker (case-insensitive, optional whitespace inside slashes).
    Preserves all internal formatting of each chunk — newlines, paragraphs, markdown,
    emojis, quotes. Only leading/trailing whitespace of each chunk is trimmed.

    Returns:
        comments: list[str]
        malformed: list[dict(index, reason, sample)]
        separator_count: int — number of //ADD// markers
        chunk_count: int — separator_count + 1
        raw_chunks: list[str] — original chunks for error reporting
    """
    text = text or ""
    raw_chunks = SEPARATOR_RE.split(text)
    separator_count = len(raw_chunks) - 1

    comments = []
    malformed = []
    for idx, chunk in enumerate(raw_chunks):
        clean = chunk.strip()
        if not clean:
            # Empty leading/trailing chunk is fine (admin started/ended with //ADD//).
            # Empty BETWEEN separators is malformed.
            if 0 < idx < len(raw_chunks) - 1:
                malformed.append({
                    "index": idx,
                    "reason": "empty",
                    "sample": "(blank between two //ADD// markers)",
                })
            continue
        comments.append(clean)

    return {
        "comments": comments,
        "malformed": malformed,
        "separator_count": separator_count,
        "chunk_count": len(raw_chunks),
        "raw_chunks": raw_chunks,
    }


def normalize_for_dedup(text):
    """Whitespace + case normalization for duplicate detection.

    Does NOT modify the stored text — only used for equivalence comparison.
    Collapses any run of whitespace (incl. newlines/tabs) to a single space,
    strips edges, lowercases.
    """
    return re.sub(r"\s+", " ", text or "").strip().lower()


def split_long_message(text, limit=SAFE_MSG_LIMIT):
    """Split a long outgoing message into <=limit-char chunks safely.

    Prefers split points at //ADD// markers (between comments), then paragraph
    breaks (\\n\\n), then single newlines. Never breaks mid-word/URL/markdown
    unless a single chunk is itself longer than the limit (rare; falls back to
    hard slice with explicit marker).
    """
    if not text:
        return []
    if len(text) <= limit:
        return [text]

    parts = []
    remaining = text
    while len(remaining) > limit:
        # Window of allowed split content
        window = remaining[:limit]

        # Best: split at the last //ADD// boundary within the window
        cut = max(
            [m.start() for m in SEPARATOR_RE.finditer(window)] or [-1]
        )
        if cut < 0:
            # Fall back to a paragraph boundary
            cut = window.rfind("\n\n")
        if cut < 0:
            # Then a single newline
            cut = window.rfind("\n")
        if cut < 0:
            # Hard split with explicit marker
            parts.append(window + "\n…[continued]")
            remaining = "…[continued]\n" + remaining[limit:]
            continue

        parts.append(remaining[:cut].rstrip())
        remaining = remaining[cut:].lstrip()
    if remaining:
        parts.append(remaining)
    return parts


# Legacy alias for any old callers — strictly parses now.
def parse_comments(text):
    return parse_comments_strict(text)["comments"]


_ADD_COMMENTS_INSTRUCTIONS = (
    "📝 Send your comments using the //ADD// separator.\n\n"
    "Example:\n"
    "First comment here…\n"
    "Can span multiple paragraphs.\n"
    "\n"
    "//ADD//\n"
    "\n"
    "Second comment with emojis 🎉 or markdown.\n"
    "\n"
    "//ADD//\n"
    "\n"
    "Third comment.\n"
    "\n"
    "You can send the batch in MULTIPLE messages — they will be buffered. "
    "When finished, send /done or tap the button below to preview and confirm."
)


import uuid


def _addc_new_session_id():
    return uuid.uuid4().hex[:8]


def _addc_buffering_keyboard(sid):
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Process Now", callback_data=f"addc:process:{sid}"),
        InlineKeyboardButton(text="❌ Cancel", callback_data=f"addc:cancel:{sid}"),
    ]])


def _addc_preview_keyboard(sid, can_save):
    rows = []
    if can_save:
        rows.append([InlineKeyboardButton(text="✅ Confirm Save", callback_data=f"addc:confirm:{sid}")])
    rows.append([
        InlineKeyboardButton(text="✏️ Add more", callback_data=f"addc:more:{sid}"),
        InlineKeyboardButton(text="❌ Cancel", callback_data=f"addc:cancel:{sid}"),
    ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def _addc_buffer_status(state):
    buf = state.get("buffer", [])
    total_chars = sum(len(s) for s in buf)
    total_seps = sum(len(SEPARATOR_RE.findall(s)) for s in buf)
    return total_chars, total_seps, len(buf)


def _addc_log(tag, state, **extra):
    """Structured log line for comment upload diagnostics.

    Example:
      [comment_upload] tag=chunk_added state=add_comments:comments sid=abc123
        buffer_chars=7806 separators=29 chunks=2 parsed_est=30
    """
    if not state:
        logging.info("[comment_upload] tag=%s state=none extras=%s", tag, extra)
        return
    chars, seps, chunks = _addc_buffer_status(state)
    parsed_est = chunks and (sum(len(SEPARATOR_RE.findall(s)) for s in state.get("buffer", [])) + 1)
    fields = {
        "state": describe_state(state),
        "sid": state.get("session_id"),
        "task_id": state.get("task_id"),
        "buffer_chars": chars,
        "separators": seps,
        "chunks": chunks,
        "parsed_est": parsed_est,
        **extra,
    }
    logging.info("[comment_upload] tag=%s %s", tag, " ".join(f"{k}={v}" for k, v in fields.items()))


async def _addc_strip_previous_buttons(message_or_callback, state):
    """Remove the inline keyboard from the previous chunk-status message,
    so stale Cancel/Process clicks can't fire on an older message."""
    prev_id = state.pop("last_buffer_msg_id", None)
    if not prev_id:
        return
    chat_id = (
        message_or_callback.from_user.id
        if isinstance(message_or_callback, Message)
        else message_or_callback.from_user.id
    )
    bot = message_or_callback.bot if isinstance(message_or_callback, Message) else message_or_callback.message.bot
    try:
        await bot.edit_message_reply_markup(chat_id=chat_id, message_id=prev_id, reply_markup=None)
    except Exception:
        # Editing a message older than 48h or already-edited fails — non-fatal.
        pass


async def _addc_show_preview(message_or_callback, state):
    """Parse the joined buffer and present the preview card."""
    text_target = (
        message_or_callback.message if isinstance(message_or_callback, CallbackQuery)
        else message_or_callback
    )
    joined = "\n".join(state.get("buffer", []))
    parsed = parse_comments_strict(joined)
    state["parsed"] = parsed  # cache for confirm step

    # Cross-check duplicates against existing DB rows
    task_id = state["task_id"]
    existing_norms = set()
    try:
        import aiosqlite
        async with aiosqlite.connect(DB_NAME) as db:
            cur = await db.execute("SELECT text FROM comments WHERE task_id = ?", (task_id,))
            for row in await cur.fetchall():
                existing_norms.add(normalize_for_dedup(row[0]))
    except Exception:
        logging.exception("Preview dedup-check failed")

    seen = set()
    dup_in_upload = 0
    dup_existing = 0
    fresh = []
    for c in parsed["comments"]:
        n = normalize_for_dedup(c)
        if n in seen:
            dup_in_upload += 1
            continue
        seen.add(n)
        if n in existing_norms:
            dup_existing += 1
            continue
        fresh.append(c)
    state["preview_counts"] = {
        "detected": len(parsed["comments"]),
        "dup_in_upload": dup_in_upload,
        "dup_existing": dup_existing,
        "malformed": len(parsed["malformed"]),
        "fresh": len(fresh),
        "separators": parsed["separator_count"],
    }

    chunk_count_for_header = len(state.get("buffer", []))
    lines = [
        f"🔍 Preview — {len(parsed['comments'])} comment(s) parsed "
        f"from {chunk_count_for_header} chunk(s) across {parsed['separator_count']} //ADD// separator(s).",
        "",
        f"Task: #{task_id}",
        f"  • New (will save): {len(fresh)}",
        f"  • Duplicates in batch: {dup_in_upload}",
        f"  • Duplicates of existing: {dup_existing}",
        f"  • Malformed entries: {len(parsed['malformed'])}",
    ]

    if parsed["malformed"]:
        lines.append("")
        lines.append("⚠ Malformed sections:")
        for m in parsed["malformed"][:5]:
            lines.append(f"  ❌ near separator #{m['index']}: {m['sample']}")
        if len(parsed["malformed"]) > 5:
            lines.append(f"  …and {len(parsed['malformed']) - 5} more")

    if fresh:
        lines.append("")
        lines.append("📑 Sample (first 3 will-save comments):")
        for i, c in enumerate(fresh[:3], 1):
            preview = c.replace("\n", " ↵ ")
            if len(preview) > 120:
                preview = preview[:117] + "…"
            lines.append(f"  {i}. {preview}")

    state["step"] = "preview"
    state["previous_step"] = "comments"
    # Strip the previous chunk-status keyboard so its Cancel/Process can't fire any more
    await _addc_strip_previous_buttons(message_or_callback, state)
    set_user_state(_addc_admin_id(message_or_callback), state)
    _addc_log("preview_shown", state, fresh=len(fresh),
              dup_batch=dup_in_upload, dup_existing=dup_existing,
              malformed=len(parsed["malformed"]))

    body = "\n".join(lines)
    sid = state.get("session_id") or "?"
    keyboard = _addc_preview_keyboard(sid, can_save=len(fresh) > 0)
    if isinstance(message_or_callback, CallbackQuery):
        try:
            await message_or_callback.message.edit_text(body, reply_markup=keyboard, parse_mode=None)
        except Exception:
            await message_or_callback.message.answer(body, reply_markup=keyboard, parse_mode=None)
    else:
        chunks = split_long_message(body)
        for i, chunk in enumerate(chunks):
            if i < len(chunks) - 1:
                await message_or_callback.answer(chunk, parse_mode=None)
            else:
                await message_or_callback.answer(chunk, reply_markup=keyboard, parse_mode=None)


def _addc_admin_id(message_or_callback):
    return message_or_callback.from_user.id


async def continue_add_comments(message, state, text):
    """Handle text input for the add_comments flow.

    step=task_id  : parse a number → advance to comments step
    step=comments : append text to state.buffer (multi-message accumulation)
                    /done or /process → switch to preview
                    /skip            → abandon
    step=preview  : free-text is ignored here (preview is callback-driven)
                    unless admin sends /more to go back to buffering
    """
    if state["step"] == "task_id":
        match = re.search(r"\d+", text)
        if not match:
            await message.answer("Send a valid task ID.", reply_markup=back_cancel_keyboard())
            return
        state["task_id"] = int(match.group())
        state["previous_step"] = "task_id"
        state["step"] = "comments"
        state["buffer"] = []
        state["session_id"] = _addc_new_session_id()
        state.pop("last_buffer_msg_id", None)
        set_user_state(message.from_user.id, state)
        _addc_log("advanced_to_comments", state, admin=message.from_user.id)
        await message.answer(
            f"📝 Task #{state['task_id']} selected.\n\n" + _ADD_COMMENTS_INSTRUCTIONS,
            reply_markup=back_cancel_keyboard(),
        )
        return

    stripped = text.strip()
    low = stripped.lower()

    if state["step"] == "preview":
        if low in ("/more", "more"):
            state["step"] = "comments"
            set_user_state(message.from_user.id, state)
            await message.answer(
                "✏ Add more chunks. Send /done when finished.",
                reply_markup=back_cancel_keyboard(),
            )
            return
        # Otherwise ignore free text in preview step — admin should use the buttons.
        await message.answer(
            "Use the buttons above to ✅ Confirm or ❌ Cancel, or send /more to add more chunks.",
            parse_mode=None,
        )
        return

    # step == "comments"
    if low == "/skip":
        clear_user_state(message.from_user.id, "comments_skipped")
        await message.answer(
            "Comments skipped. You can add them later via 📝 Add Comments.",
            reply_markup=admin_tasks_menu(),
        )
        return

    if low in ("/done", "/process", "done", "process"):
        buf = state.get("buffer", [])
        _addc_log("done_via_text", state, raw=stripped)
        if not buf:
            await message.answer(
                "Buffer is empty — nothing to process. Send your comments first.",
            )
            return
        await _addc_show_preview(message, state)
        return

    # Strip previous chunk-status buttons so stale clicks can't fire on it
    await _addc_strip_previous_buttons(message, state)

    # Per-chunk metrics for transparency
    this_chunk_seps = len(SEPARATOR_RE.findall(text))

    # Append to buffer
    state.setdefault("buffer", []).append(text)
    set_user_state(message.from_user.id, state)
    chars, seps, chunks = _addc_buffer_status(state)
    sid = state.get("session_id") or "?"
    sent = await message.answer(
        f"📥 Chunk {chunks} added ({this_chunk_seps} //ADD// in this chunk).\n"
        f"Running total: {seps + 1} comment(s) across {chunks} chunk(s) "
        f"({seps} separators, {chars} chars).\n\n"
        f"Send more, /done to preview, or use the buttons (session {sid}).",
        reply_markup=_addc_buffering_keyboard(sid),
        parse_mode=None,
    )
    # Remember the latest chunk-status message so we can strip its buttons
    # when the next chunk comes in.
    state["last_buffer_msg_id"] = sent.message_id
    set_user_state(message.from_user.id, state)
    _addc_log("chunk_added", state, msg_id=sent.message_id)


def _addc_parse_callback(callback):
    """Returns (action, sid_from_cb) from 'addc:<action>:<sid>' or (action, None)
    for legacy 'addc:<action>' format. Used by all addc callback handlers."""
    parts = (callback.data or "").split(":")
    if len(parts) >= 3:
        return parts[1], parts[2]
    if len(parts) == 2:
        return parts[1], None
    return None, None


def _addc_check_session(callback, state, sid_from_cb, require_step=None):
    """Return (ok, reason). reason is human-readable for the alert.

    ok=False reasons:
        'no_admin'       → not an admin
        'no_state'       → state missing / not add_comments
        'wrong_step'     → state exists but step mismatch
        'stale_session'  → button is from a previous session_id
    """
    if not state or state.get("flow") != "add_comments":
        return False, "no_state"
    cur_sid = state.get("session_id")
    if sid_from_cb is not None and cur_sid and sid_from_cb != cur_sid:
        return False, "stale_session"
    if require_step and state.get("step") != require_step:
        return False, "wrong_step"
    return True, "ok"


@dp.callback_query(F.data.startswith("addc:process"))
async def addc_process_callback(callback: CallbackQuery):
    log_callback_click(callback, "addc_process")
    action, sid_from_cb = _addc_parse_callback(callback)
    state = user_states.get(callback.from_user.id)
    _addc_log("cb_process_entry", state, cb_sid=sid_from_cb, admin=callback.from_user.id)
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True); return
    ok, reason = _addc_check_session(callback, state, sid_from_cb, require_step="comments")
    if not ok:
        msg = {
            "no_state": "⚠ No active comment upload. Tap 📝 Add Comments to start a new one.",
            "wrong_step": "⚠ Upload is in preview step — use the buttons above.",
            "stale_session": "⚠ This message is from an older upload session and is no longer active.",
        }[reason]
        await callback.answer(msg, show_alert=True); return
    if not state.get("buffer"):
        await callback.answer("Buffer is empty — send comments first.", show_alert=True); return
    await callback.answer("Processing…")
    await _addc_show_preview(callback, state)


@dp.callback_query(F.data.startswith("addc:more"))
async def addc_more_callback(callback: CallbackQuery):
    log_callback_click(callback, "addc_more")
    action, sid_from_cb = _addc_parse_callback(callback)
    state = user_states.get(callback.from_user.id)
    _addc_log("cb_more_entry", state, cb_sid=sid_from_cb, admin=callback.from_user.id)
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True); return
    ok, reason = _addc_check_session(callback, state, sid_from_cb)
    if not ok:
        msg = {
            "no_state": "⚠ No active comment upload.",
            "wrong_step": "⚠ Upload step mismatch.",
            "stale_session": "⚠ This message is from an older upload session.",
        }[reason]
        await callback.answer(msg, show_alert=True); return
    state["step"] = "comments"
    state.pop("last_buffer_msg_id", None)
    set_user_state(callback.from_user.id, state)
    await callback.message.answer(
        "✏ Add more chunks. Send /done when finished.",
        reply_markup=back_cancel_keyboard(),
    )
    await callback.answer()


@dp.callback_query(F.data.startswith("addc:cancel"))
async def addc_cancel_callback(callback: CallbackQuery):
    log_callback_click(callback, "addc_cancel")
    action, sid_from_cb = _addc_parse_callback(callback)
    state = user_states.get(callback.from_user.id)
    _addc_log("cb_cancel_entry", state, cb_sid=sid_from_cb, admin=callback.from_user.id)
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True); return
    if not state or state.get("flow") != "add_comments":
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await callback.answer("⚠ No active upload to cancel.", show_alert=True); return
    cur_sid = state.get("session_id")
    if sid_from_cb is not None and cur_sid and sid_from_cb != cur_sid:
        # Stale Cancel from an older session — do NOT clear current state.
        try:
            await callback.message.edit_reply_markup(reply_markup=None)
        except Exception:
            pass
        await callback.answer(
            "⚠ Older session — current upload kept intact.",
            show_alert=True,
        )
        return
    task_id = state.get("task_id")
    clear_user_state(callback.from_user.id, "add_comments_cancel")
    try:
        await callback.message.edit_text(
            f"❌ Upload cancelled (Task #{task_id}). No changes saved.",
        )
    except Exception:
        await callback.message.answer(
            f"❌ Upload cancelled (Task #{task_id}). No changes saved.",
        )
    await callback.answer("Cancelled.")


@dp.callback_query(F.data.startswith("addc:confirm"))
async def addc_confirm_callback(callback: CallbackQuery):
    log_callback_click(callback, "addc_confirm")
    action, sid_from_cb = _addc_parse_callback(callback)
    state = user_states.get(callback.from_user.id)
    _addc_log("cb_confirm_entry", state, cb_sid=sid_from_cb, admin=callback.from_user.id)
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True); return
    ok, reason = _addc_check_session(callback, state, sid_from_cb, require_step="preview")
    if not ok:
        msg = {
            "no_state": "⚠ Preview expired — reopen the upload.",
            "wrong_step": "⚠ Preview no longer active.",
            "stale_session": "⚠ This preview is from an older session.",
        }[reason]
        await callback.answer(msg, show_alert=True); return

    parsed = state.get("parsed")
    if not parsed:
        await callback.answer("Nothing to save.", show_alert=True); return

    task_id = state["task_id"]
    await callback.answer("Saving…")

    result = await add_comments_to_task(task_id, parsed["comments"])
    if result.get("task_unavailable"):
        await callback.message.answer(
            "❌ Task Unavailable\n\nThe task was not found or is closed.",
            reply_markup=admin_tasks_menu(),
            parse_mode=None,
        )
        clear_user_state(callback.from_user.id, "add_comments_unavailable")
        return

    lines = [
        f"✅ Added: {result['added']} comment(s) to Task #{task_id}",
        f"⚠ Skipped duplicates (in batch): {result['skipped_dup_in_upload']}",
        f"⚠ Skipped duplicates (already in task): {result['skipped_dup_existing']}",
        f"❌ Failed parse (malformed): {len(parsed['malformed'])}",
    ]
    if result["errors"]:
        lines.append(f"❌ DB errors: {result['errors']}")
    await callback.message.answer(
        "\n".join(lines),
        reply_markup=admin_tasks_menu(),
        parse_mode=None,
    )
    logging.info(
        "Comments confirmed-save: admin=%s task=%s added=%s dup_batch=%s dup_existing=%s malformed=%s errors=%s",
        callback.from_user.id, task_id, result["added"],
        result["skipped_dup_in_upload"], result["skipped_dup_existing"],
        len(parsed["malformed"]), result["errors"],
    )
    clear_user_state(callback.from_user.id, "add_comments_saved")


async def continue_task_status(message, state, text):
    match = re.search(r'\d+', text)
    if not match:
        await message.answer("Send a valid task ID.", reply_markup=back_cancel_keyboard())
        return
    task_id = int(match.group())
    status = state["status"]
    ok = await update_task_status(task_id, status)
    clear_user_state(message.from_user.id, "task_status_done")
    if ok:
        logging.info("Task status changed: task=%s status=%s admin=%s", task_id, status, message.from_user.id)
        await message.answer(f"✅ Task #{task_id} status set to {status}.", reply_markup=await main_menu(message.from_user.id))
    else:
        await message.answer("Task not found.", reply_markup=await main_menu(message.from_user.id))


async def close_task_by_id(message, task_id):
    ok = await close_task(task_id)
    if ok:
        logging.info("Task closed: task=%s admin=%s", task_id, message.from_user.id)
        await message.answer(f"Task #{task_id} closed.")
    else:
        await message.answer("Task not found.")


async def save_comments_from_text(message, text):
    """One-shot /addcomment command — no preview, direct save with //ADD// parsing."""
    if not await is_admin(message.from_user.id):
        await message.answer("Only admins can add comments.")
        return
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer(
            "Use: /addcomment task_id <comments using //ADD// separator>",
        )
        return
    match = re.search(r"\d+", parts[0])
    if not match:
        await message.answer("Invalid task ID.")
        return
    task_id = int(match.group())

    parsed = parse_comments_strict(parts[1])
    if not parsed["comments"]:
        await message.answer("No valid comments found. Use //ADD// to separate entries.")
        return

    result = await add_comments_to_task(task_id, parsed["comments"])
    if result.get("task_unavailable"):
        await message.answer("Task not found, closed, or unavailable.")
        return

    lines = [
        f"✅ Added: {result['added']}",
        f"⚠ Skipped duplicates (in batch): {result['skipped_dup_in_upload']}",
        f"⚠ Skipped duplicates (existing): {result['skipped_dup_existing']}",
    ]
    if parsed["malformed"]:
        lines.append(f"❌ Failed parse: {len(parsed['malformed'])}")
    if result["errors"]:
        lines.append(f"❌ DB errors: {result['errors']}")
    await message.answer("\n".join(lines), parse_mode=None)


async def main():
    acquire_single_instance_lock()
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is missing in .env")
    if not ADMIN_IDS:
        logging.warning("ADMIN_IDS is empty. Admin buttons will be blocked.")
    await create_db()
    bot = Bot(token=BOT_TOKEN)
    install_bot_response_tracking(bot)
    logging.info("Bot startup initialized")
    logging.info("Handler registration: update_types=%s", dp.resolve_used_update_types())
    await bot.delete_webhook(drop_pending_updates=True)
    logging.info("Webhook deleted before polling; pending updates dropped")
    # Launch background tasks alongside polling
    checker_task = asyncio.create_task(comment_live_checker_task(bot))
    expiry_task = asyncio.create_task(claim_expiry_task(bot))
    logging.info("Background tasks started: comment_live_checker, claim_expiry")
    try:
        logging.info("Polling starting")
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        checker_task.cancel()
        expiry_task.cancel()
        logging.info("Background tasks cancelled; polling stopped")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
