import asyncio
import logging
import os
import re
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from urllib.parse import urlparse, urlunparse

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command, CommandObject
from aiogram.types import (
    CallbackQuery,
    ErrorEvent,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)
from dotenv import load_dotenv

from database import (
    add_comments_to_task,
    add_member_note,
    add_member_warning,
    approve_submission,
    archive_task,
    auto_cleanup_claims,
    ban_user,
    cancel_payment,
    claim_comment,
    close_task,
    create_db,
    create_task,
    flag_submission,
    get_active_claim,
    get_active_tasks,
    get_all_member_ids,
    get_daily_stats,
    get_flagged_submissions,
    get_leaderboard,
    get_member_stats,
    get_payment_history,
    get_pending_payments,
    get_pending_submissions,
    get_setting,
    get_submission_history,
    get_system_stats,
    get_task_stats,
    get_total_stats,
    get_user,
    get_user_by_username,
    log_audit_action,
    mark_payment_paid,
    post_exists,
    reddit_comment_id_exists,
    register_user,
    reject_submission,
    save_qr_file_id,
    save_submission,
    save_upi_id,
    set_setting,
    shadowban_user,
    submission_exists_for_comment,
    submission_link_exists,
    unban_user,
    update_reputation,
    update_task_status,
)


load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
dp = Dispatcher()
user_states = {}

# User Buttons
BTN_CLAIM = "📋 Claim Task"
BTN_SUBMIT = "📤 Submit Proof"
BTN_MY_STATS = "📊 My Stats"
BTN_PAYMENTS = "💰 Payments"
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
BTN_MEMBERS_TRUSTED = "🏅 Trusted Members"

# Analytics Submenu
BTN_ANALYTICS_DAILY = "📈 Daily Stats"
BTN_ANALYTICS_SYSTEM = "📊 System Stats"
BTN_ANALYTICS_TOP = "🏆 Top Members"
BTN_ANALYTICS_EARNINGS = "💰 Earnings Stats"

# Shared/Nav
BTN_SET_UPI = "💳 Set UPI ID"
BTN_UPLOAD_QR = "🖼 Upload QR"
BTN_PAYMENT_HISTORY = "📜 Payment History"
BTN_TOTAL_EARNINGS = "💰 Total Earnings"
BTN_BACK = "⬅️ Back"
BTN_HOME = "🏠 Home"
BTN_CANCEL = "❌ Cancel"

CATEGORIES = ["Comment", "Upvote", "Discussion", "Review", "Meme", "Advice", "Story", "Finance", "Tech", "Relationship"]
CLAIM_COOLDOWN_SECONDS = 30
SUBMIT_COOLDOWN_SECONDS = 60

BUTTON_ALIASES = {
    BTN_CLAIM: {"Claim Task", "Claim"},
    BTN_SUBMIT: {"Submit Proof", "Submit"},
    BTN_MY_STATS: {"My Stats", "Stats"},
    BTN_PAYMENTS: {"Payments"},
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
    BTN_CLAIM, BTN_SUBMIT, BTN_MY_STATS, BTN_PAYMENTS, BTN_RULES, BTN_HELP,
    BTN_BACK, BTN_HOME, BTN_CANCEL,
    BTN_ADMIN_TASKS, BTN_ADMIN_REVIEWS, BTN_ADMIN_PAYMENTS, BTN_ADMIN_MEMBERS,
    BTN_ADMIN_ANALYTICS, BTN_ADMIN_BROADCAST, BTN_ADMIN_SETTINGS,
    BTN_TASKS_NEW, BTN_TASKS_ADD_COMMENTS, BTN_TASKS_ACTIVE, BTN_TASKS_STATS, BTN_TASKS_MANAGE, BTN_TASKS_ARCHIVED,
    BTN_REVIEWS_PENDING, BTN_REVIEWS_FLAGGED, BTN_REVIEWS_HISTORY,
    BTN_PAYMENTS_PENDING, BTN_PAYMENTS_PAID, BTN_PAYMENTS_STATS,
    BTN_MEMBERS_SEARCH, BTN_MEMBERS_WARNED, BTN_MEMBERS_BANNED, BTN_MEMBERS_TRUSTED,
    BTN_ANALYTICS_DAILY, BTN_ANALYTICS_SYSTEM, BTN_ANALYTICS_TOP, BTN_ANALYTICS_EARNINGS,
    # Payment submenu buttons — were missing, causing UPI/QR to show "Choose an option from the menu."
    BTN_SET_UPI, BTN_UPLOAD_QR, BTN_PAYMENT_HISTORY, BTN_TOTAL_EARNINGS,
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


def log_button_click(message, button):
    if button_key(message.text) == button:
        logging.info(
            "Button click: user=%s button=%s active_state=%s",
            message.from_user.id,
            button,
            describe_state(user_states.get(message.from_user.id)),
        )


def log_callback_click(callback, action):
    logging.info(
        "Callback click: user=%s action=%s data=%s active_state=%s",
        callback.from_user.id,
        action,
        callback.data,
        describe_state(user_states.get(callback.from_user.id)),
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
        [KeyboardButton(text=BTN_RULES), KeyboardButton(text=BTN_HELP)],
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


async def admin_menu():
    buttons = [
        [KeyboardButton(text=BTN_ADMIN_TASKS), KeyboardButton(text=BTN_ADMIN_REVIEWS)],
        [KeyboardButton(text=BTN_ADMIN_PAYMENTS), KeyboardButton(text=BTN_ADMIN_MEMBERS)],
        [KeyboardButton(text=BTN_ADMIN_ANALYTICS), KeyboardButton(text=BTN_ADMIN_BROADCAST)],
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
        [KeyboardButton(text=BTN_PAYMENTS_PENDING), KeyboardButton(text=BTN_PAYMENTS_PAID)],
        [KeyboardButton(text=BTN_PAYMENTS_STATS), KeyboardButton(text=BTN_BACK)],
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def admin_members_menu():
    buttons = [
        [KeyboardButton(text=BTN_MEMBERS_SEARCH), KeyboardButton(text=BTN_MEMBERS_WARNED)],
        [KeyboardButton(text=BTN_MEMBERS_BANNED), KeyboardButton(text=BTN_MEMBERS_TRUSTED)],
        [KeyboardButton(text=BTN_BACK)],
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def admin_analytics_menu():
    buttons = [
        [KeyboardButton(text=BTN_ANALYTICS_DAILY), KeyboardButton(text=BTN_ANALYTICS_SYSTEM)],
        [KeyboardButton(text=BTN_ANALYTICS_TOP), KeyboardButton(text=BTN_ANALYTICS_EARNINGS)],
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
    
    nav_row.append(InlineKeyboardButton(text=f"{current_page}/{total_pages}", callback_data="noop"))
    
    if current_page < total_pages:
        nav_row.append(InlineKeyboardButton(text="Next ➡️", callback_data=f"{callback_prefix}:page:{current_page+1}"))
    
    if len(nav_row) > 1:
        buttons.append(nav_row)
    return buttons


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

        if host in {"www.reddit.com", "old.reddit.com", "np.reddit.com", "m.reddit.com", "reddit.com"}:
            host = "reddit.com"
        else:
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
            # Only treat as comment_id if it looks like a Reddit base-36 ID (3-8 alphanumeric chars)
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
        match = re.search(r"([0-9]+(?:\.[0-9]+)?)", amount)
        if not match:
            return "Not calculated"
        if not currency:
            currency = amount[:match.start()].strip()
        try:
            total += Decimal(match.group(1))
        except InvalidOperation:
            return "Not calculated"
    total_text = str(int(total)) if total == total.to_integral() else str(total)
    return f"{currency}{total_text}" if currency else total_text


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
    is_new = await register_user(message.from_user.id, message.from_user.username)
    if is_new:
        await message.answer(
            "👋 Welcome to VIRON Reddit Task Group!\n\n"
            "We're happy to have you here. Here's how to get started:\n\n"
            "1️⃣ Tap 📋 Claim Task to receive your first assignment.\n"
            "2️⃣ Go to Reddit and post the comment exactly as shown.\n"
            "3️⃣ Copy the link to your comment and tap 📤 Submit Proof.\n"
            "4️⃣ Our team reviews it. Once approved, your earnings are added automatically.\n\n"
            "💡 Tip: Set your UPI ID in 💰 Payments so we can pay you quickly.\n\n"
            "Take your time, follow the instructions carefully, and you'll do great! 🌟"
        )
    await message.answer(
        "🔥 VIRON Reddit Task Group\n\n"
        "Complete Reddit engagement tasks,\n"
        "submit proof,\n"
        "and earn real money — paid directly to your UPI.\n\n"
        "Choose an option below 👇",
        reply_markup=await main_menu(message.from_user.id),
    )


@dp.message(Command("start"))
async def start(message: Message):
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
    await message.answer("Send the Reddit POST URL.")


def level_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🥉 Beginner", callback_data="task:level:Beginner")],
        [InlineKeyboardButton(text="🥈 Trusted", callback_data="task:level:Trusted")],
        [InlineKeyboardButton(text="🥇 Elite", callback_data="task:level:Elite")],
    ])


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


@dp.callback_query(F.data.startswith("task:level:"))
async def choose_level(callback: CallbackQuery):
    log_callback_click(callback, "choose_level")
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True)
        return
    state = user_states.get(callback.from_user.id)
    if not state or state.get("step") != "min_level":
        await callback.answer("Wrong step.", show_alert=True)
        return
    level = callback.data.split(":")[2]
    state["min_level"] = level
    state["previous_step"] = "min_level"
    state["step"] = "priority"
    await callback.message.answer(f"Minimum level: {level}\n\nChoose task priority:", reply_markup=priority_keyboard())
    await callback.answer()


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
    state["previous_step"] = "category"
    state["step"] = "instructions"
    logging.info(
        "Task creation advanced: admin=%s category=%s step=instructions",
        callback.from_user.id,
        category,
    )
    await callback.message.answer(
        f"Category: {category}\n\nSend task instructions for workers (or /skip to leave blank).",
        reply_markup=back_cancel_keyboard(),
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
    state["step"] = "instructions"
    await callback.message.answer("Optional instructions? Send text, or send /skip.")
    await callback.answer()


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
    set_user_state(message.from_user.id, {"flow": "add_comments", "step": "task_id"})
    await message.answer("Send the task ID.")


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
    rows = await get_active_tasks()
    if not rows:
        await message.answer("No tasks available to manage.")
        return
    for task in rows:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"❌ Close", callback_data=f"task:status:closed:{task['id']}"),
             InlineKeyboardButton(text=f"⏸ Pause", callback_data=f"task:status:paused:{task['id']}"),
             InlineKeyboardButton(text=f"🔓 Reopen", callback_data=f"task:status:active:{task['id']}")],
            [InlineKeyboardButton(text=f"🟡 Review Mode", callback_data=f"task:status:under_review:{task['id']}"),
             InlineKeyboardButton(text=f"🗑 Archive", callback_data=f"task:status:archived:{task['id']}")],
        ])
        await message.answer(f"Task #{task['id']} - r/{task['subreddit']} [{task['status']}]", reply_markup=keyboard)


@dp.message(button_filter(BTN_ANALYTICS_SYSTEM))
async def system_stats_handler(message: Message):
    if not await is_admin(message.from_user.id):
        return
    stats = await get_system_stats()
    text = (
        "📊 **SYSTEM STATS**\n\n"
        f"👥 Total Members: {stats['total_members']}\n"
        f"📌 Total Tasks: {stats['total_tasks']}\n"
        f"📤 Total Submissions: {stats['total_submissions']}\n"
        f"💸 Total Payouts: ₹{stats['total_payouts']:.2f}\n"
        f"⏳ Pending Payouts: ₹{stats['pending_payouts_sum']:.2f}\n\n"
        f"📂 Active Tasks: {stats['active_tasks']}\n"
        f"⏳ Pending Reviews: {stats['pending_reviews']}\n"
        f"💸 Pending Payments: {stats['pending_payments']}\n"
    )
    await message.answer(text, parse_mode="Markdown")


@dp.message(button_filter(BTN_ADMIN_SETTINGS))
async def admin_settings_handler(message: Message):
    if not await is_admin(message.from_user.id):
        return
    maintenance = await get_setting("maintenance_mode", "0")
    maint_label = "🔴 Disable Maintenance Mode" if maintenance == "1" else "🟢 Enable Maintenance Mode"
    status_text = "🔴 MAINTENANCE MODE ON" if maintenance == "1" else "🟢 System Online"
    claim_timeout = await get_setting("claim_timeout_minutes", "30")
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=maint_label, callback_data="admin:config:maintenance")],
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


@dp.callback_query(F.data == "admin:config:timeout")
async def config_timeout(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("Admins only.", show_alert=True)
        return
    set_user_state(callback.from_user.id, {"flow": "set_claim_timeout"})
    await callback.message.answer("Send new claim timeout in minutes (e.g. 30):")
    await callback.answer()


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
    """Release expired claims and notify users."""
    expired = await auto_cleanup_claims()
    for claim in expired:
        try:
            await bot.send_message(
                claim["assigned_to"],
                f"⚠ **Claim Expired**\n\nYour claim for Task #{claim['task_id']} has expired because no proof was submitted within 30 minutes. The comment has been released back to the pool."
            )
        except Exception:
            logging.warning("Failed to notify user %s about expired claim", claim["assigned_to"])


@dp.message(button_filter(BTN_TASKS_ACTIVE))
async def active_tasks_handler(message: Message):
    await active_tasks(message, page=1)


async def active_tasks(message_or_callback, page=1):
    if isinstance(message_or_callback, Message):
        await cleanup_and_notify(message_or_callback.bot)
    else:
        await cleanup_and_notify(message_or_callback.message.bot)
        
    rows = await get_active_tasks()
    if not rows:
        text = "No active tasks."
        if isinstance(message_or_callback, CallbackQuery):
            await message_or_callback.message.edit_text(text)
        else:
            await message_or_callback.answer(text)
        return

    total = len(rows)
    if page > total: page = total
    if page < 1: page = 1
    task = rows[page-1]

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

    comments_warning = ""
    if total_comments < task["total_slots"] and status != "full":
        comments_warning = f"\n⚠ Only {total_comments} comment(s) added — add more to fill all slots."

    text = (
        f"📂 **Active Tasks ({page}/{total})**\n\n"
        f"Task #{task['id']} r/{task['subreddit']}\n"
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
    keyboard.extend(get_pagination_keyboard(page, total, "task"))
    markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    if isinstance(message_or_callback, CallbackQuery):
        await message_or_callback.message.edit_text(text, reply_markup=markup, parse_mode="Markdown")
    else:
        await message_or_callback.answer(text, reply_markup=markup, parse_mode="Markdown")


@dp.callback_query(F.data.startswith("task:page:"))
async def task_page_callback(callback: CallbackQuery):
    page = int(callback.data.split(":")[2])
    await active_tasks(callback, page)
    await callback.answer()


@dp.callback_query(F.data.startswith("task:manage_single:"))
async def manage_single_task(callback: CallbackQuery):
    task_id = int(callback.data.split(":")[2])
    # Re-use existing manage_tasks_button logic but for one task
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"❌ Close", callback_data=f"task:status:closed:{task_id}"),
         InlineKeyboardButton(text=f"⏸ Pause", callback_data=f"task:status:paused:{task_id}"),
         InlineKeyboardButton(text=f"🔓 Reopen", callback_data=f"task:status:active:{task_id}")],
        [InlineKeyboardButton(text=f"🟡 Review Mode", callback_data=f"task:status:under_review:{task_id}"),
         InlineKeyboardButton(text=f"🗑 Archive", callback_data=f"task:status:archived:{task_id}")],
        [InlineKeyboardButton(text="⬅️ Back to List", callback_data="task:page:1")]
    ])
    await callback.message.edit_text(f"🔒 Managing Task #{task_id}", reply_markup=keyboard)
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


@dp.message(Command("claim"))
@dp.message(button_filter(BTN_CLAIM))
async def claim(message: Message):
    log_button_click(message, BTN_CLAIM)
    clear_user_state(message.from_user.id, "claim")
    if await get_setting("maintenance_mode", "0") == "1":
        await message.answer(
            "⚠ System temporarily under maintenance.\n\nPlease try again later."
        )
        return
    await register_user(message.from_user.id, message.from_user.username)
    await cleanup_and_notify(message.bot)
    
    user = await get_user(message.from_user.id)
    if user and user["is_banned"]:
        await message.answer("🚫 Your account has been restricted.\n\nContact admin if this is a mistake.")
        return
    remaining = cooldown_left(user["last_claim_at"] if user else None)
    if remaining > 0 and not await get_active_claim(message.from_user.id):
        await message.answer(f"⏳ Please wait {remaining} seconds before claiming another task.")
        return

    claim_data = await claim_comment(message.from_user.id)
    if claim_data == "banned":
        await message.answer("🚫 Your account has been restricted.\n\nContact admin if this is a mistake.")
        return
    if not claim_data:
        await message.answer("No active tasks are available right now.")
        return

    title = "📌 YOUR ACTIVE TASK" if claim_data["already_claimed"] else "🎯 NEW TASK ASSIGNED"
    if not claim_data["already_claimed"]:
        logging.info("Claim created: user=%s task=%s comment=%s", message.from_user.id, claim_data["task_id"], claim_data["comment_id"])
    instructions = claim_data["instructions"] or "Post the comment exactly as written. Do not edit or paraphrase."
    await message.answer(
        f"{title}\n"
        f"{'─' * 32}\n\n"
        f"🆔 Task: #{claim_data['task_id']}\n"
        f"📍 Subreddit: r/{claim_data['subreddit']}\n"
        f"🏷 Category: {claim_data['category']}\n"
        f"💸 Payout: {claim_data['payout_amount']}\n"
        f"⏰ Expires in: 30 minutes\n\n"
        f"{'─' * 32}\n\n"
        f"📎 REDDIT POST:\n{claim_data['post_url']}\n\n"
        f"💬 POST THIS COMMENT:\n\n{claim_data['comment_text']}\n\n"
        f"{'─' * 32}\n\n"
        f"📋 INSTRUCTIONS:\n{instructions}\n\n"
        f"{'─' * 32}\n\n"
        "✅ STEPS:\n"
        "1. Open the Reddit post above.\n"
        "2. Post the comment exactly as shown.\n"
        "3. Copy the link to your comment.\n"
        "4. Tap 📤 Submit Proof and send the link."
    )


@dp.message(Command("submit"))
async def submit_command(message: Message, command: CommandObject):
    if command.args:
        await handle_submit_link(message, command.args.strip())
        return
    await start_submit_flow(message)


@dp.message(button_filter(BTN_SUBMIT))
async def start_submit_flow(message: Message):
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
    await message.answer("Send your Reddit COMMENT link.")


_INVALID_PROOF_MSG = (
    "====================================\n\n"
    "❌ Invalid Reddit proof.\n\n"
    "Your Reddit comment must belong to the assigned Reddit post.\n\n"
    "===================================="
)

_SUCCESS_MSG = (
    "====================================\n\n"
    "✅ Submission Received\n\n"
    "Your Reddit proof has been submitted successfully and is now pending admin review.\n\n"
    "===================================="
)


async def handle_submit_link(message, raw_text):
    await register_user(message.from_user.id, message.from_user.username)

    # Extract Reddit URL from message — user may include extra context text
    reddit_link = extract_reddit_url(raw_text)

    # Cooldown check
    user = await get_user(message.from_user.id)
    if user and user.get("last_submit_at"):
        try:
            last_submit = datetime.fromisoformat(
                user["last_submit_at"].replace(" ", "T")
            ).replace(tzinfo=timezone.utc)
            elapsed = (datetime.now(timezone.utc) - last_submit).total_seconds()
            if elapsed < SUBMIT_COOLDOWN_SECONDS:
                await message.answer(
                    f"⏳ Please wait {int(SUBMIT_COOLDOWN_SECONDS - elapsed)} seconds before submitting another proof."
                )
                return
        except ValueError:
            pass

    claim_data = await get_active_claim(message.from_user.id)
    if not claim_data:
        await message.answer("You do not have an active task. Use 📋 Claim Task first.")
        return

    # Parse the submitted URL
    comment_details = parse_reddit_url(reddit_link)
    if not comment_details:
        logging.warning(
            "Proof rejected - unparseable URL: user=%s task=%s raw=%r extracted=%r",
            message.from_user.id, claim_data["task_id"], raw_text, reddit_link,
        )
        await message.answer(_INVALID_PROOF_MSG)
        return

    # Validate: subreddit + post_id must match the assigned task.
    # comment_id is NOT required — validation ignores it per spec.
    if not comment_matches_task(comment_details, claim_data):
        logging.warning(
            "Proof rejected - post mismatch: user=%s task=%s "
            "submitted(sub=%s post=%s) expected(sub=%s post=%s)",
            message.from_user.id, claim_data["task_id"],
            comment_details["subreddit"], comment_details["post_id"],
            claim_data["subreddit"], claim_data["post_id"],
        )
        await message.answer(_INVALID_PROOF_MSG)
        return

    # Duplicate submission guards
    if await submission_link_exists(comment_details["normalized_url"]):
        await message.answer("❌ This proof link was already submitted.")
        return
    reddit_cid = comment_details.get("comment_id")
    if reddit_cid and await reddit_comment_id_exists(reddit_cid):
        await message.answer("❌ This Reddit comment was already used as proof.")
        return
    if await submission_exists_for_comment(claim_data["comment_id"]):
        await message.answer("❌ You already submitted proof for this task.")
        return

    submission_id = await save_submission(
        message.from_user.id,
        message.from_user.username,
        claim_data["task_id"],
        claim_data["comment_id"],
        claim_data["comment_text"],
        reddit_link,
        comment_details["normalized_url"],
        reddit_cid,
    )
    if submission_id is None:
        # Blocked by unique constraint — submission already exists
        await message.answer("❌ You already submitted proof for this task.")
        return
    clear_user_state(message.from_user.id, "submission_saved")
    logging.info(
        "Submission saved: id=%s user=%s task=%s comment_id=%s reddit_cid=%s",
        submission_id, message.from_user.id, claim_data["task_id"],
        claim_data["comment_id"], reddit_cid,
    )
    await message.answer(_SUCCESS_MSG, reply_markup=await main_menu(message.from_user.id))


@dp.message(button_filter(BTN_PAYMENTS_PENDING))
async def pending_payments_handler(message: Message, bot: Bot):
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


@dp.message(button_filter(BTN_MY_STATS))
@dp.message(Command("stats"))
async def my_stats(message: Message):
    log_button_click(message, BTN_MY_STATS)
    clear_user_state(message.from_user.id, "my_stats")
    await register_user(message.from_user.id, message.from_user.username)
    stats = await get_member_stats(message.from_user.id)
    if not stats:
        await message.answer("Could not load stats. Please try again.")
        return
    payments = await get_payment_history(message.from_user.id)
    total_reviewed = stats['approved'] + stats['rejected']
    rate_text = f"{stats['approval_rate']}%" if total_reviewed > 0 else "—"
    earned = total_amount(payments, paid_only=True) or "₹0"
    level_icons = {"Beginner": "🥉", "Trusted": "🥈", "Elite": "🥇"}
    level_icon = level_icons.get(stats['level'], "🏅")
    await message.answer(
        f"📊 YOUR STATS\n"
        f"{'─' * 28}\n\n"
        f"✅ Completed: {stats['approved']}\n"
        f"⏳ Pending Review: {stats['pending']}\n"
        f"❌ Rejected: {stats['rejected']}\n\n"
        f"💰 Total Earned: {earned}\n"
        f"⭐ Approval Rate: {rate_text}\n"
        f"{level_icon} Level: {stats['level']}\n"
        f"⚠ Warnings: {stats['warnings']}\n"
        f"🔥 Streak: {stats['streak'] or 0} days"
    )


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
    log_button_click(message, BTN_PAYMENTS)
    clear_user_state(message.from_user.id, "payments_menu")
    await register_user(message.from_user.id, message.from_user.username)
    await message.answer("Choose a payment option.", reply_markup=payments_menu())


@dp.message(button_filter(BTN_SET_UPI))
async def set_upi(message: Message):
    log_button_click(message, BTN_SET_UPI)
    set_user_state(message.from_user.id, {"flow": "set_upi"})
    await message.answer("Send your UPI ID. Example: name@okaxis")


@dp.message(button_filter(BTN_UPLOAD_QR))
async def upload_qr(message: Message):
    log_button_click(message, BTN_UPLOAD_QR)
    set_user_state(message.from_user.id, {"flow": "upload_qr"})
    await message.answer("Upload your payment QR image.")


@dp.message(button_filter(BTN_PAYMENT_HISTORY))
async def payment_history(message: Message):
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
    log_button_click(message, BTN_TOTAL_EARNINGS)
    clear_user_state(message.from_user.id, "total_earnings")
    rows = await get_payment_history(message.from_user.id)
    await message.answer(f"💵 Total Earned:\n{total_amount(rows, paid_only=True)}")


@dp.message(button_filter(BTN_RULES))
async def rules(message: Message):
    log_button_click(message, BTN_RULES)
    clear_user_state(message.from_user.id, "rules")
    await message.answer(
        "📜 COMMUNITY GUIDELINES\n\n"
        "Following these simple rules keeps the platform fair for everyone:\n\n"
        "✅ Post only the comment text assigned to you — no edits or paraphrasing.\n"
        "✅ Each person may use one account only.\n"
        "✅ Keep your comment live — do not delete it after submitting proof.\n"
        "✅ Submit real proof links. Only your own comment qualifies.\n"
        "✅ Payments are processed after admin review and approval.\n\n"
        "Following these guidelines ensures smooth payments and a healthy community for everyone. 🙌"
    )


@dp.message(button_filter(BTN_HELP))
async def help_button(message: Message):
    log_button_click(message, BTN_HELP)
    clear_user_state(message.from_user.id, "help")
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📋 Claiming Tasks", callback_data="help:topic:claim"),
         InlineKeyboardButton(text="📤 Submitting Proof", callback_data="help:topic:submit")],
        [InlineKeyboardButton(text="💰 Payments", callback_data="help:topic:payments"),
         InlineKeyboardButton(text="📜 Rules", callback_data="help:topic:rules")],
    ])
    await message.answer(
        "❓ HELP CENTER\n\n"
        "Hi! What do you need help with? Choose a topic below 👇",
        reply_markup=keyboard,
    )

@dp.callback_query(F.data.startswith("help:topic:"))
async def help_callback(callback: CallbackQuery):
    topic = callback.data.split(":")[2]
    if topic == "claim":
        text = (
            "📋 **How to Claim a Task**\n\n"
            "1. Tap 📋 **Claim Task** from the main menu.\n"
            "2. You'll receive a Reddit post link and the exact comment to post.\n"
            "3. Read the instructions carefully — post the comment exactly as given.\n"
            "4. You have **30 minutes** to complete and submit proof.\n\n"
            "💡 If no tasks are available, check back soon — new tasks are added regularly!"
        )
    elif topic == "submit":
        text = (
            "📤 **How to Submit Proof**\n\n"
            "1. After posting your comment on Reddit, open it.\n"
            "2. Tap **Share** (or the 3-dot menu) → **Copy Link**.\n"
            "3. Come back here and tap 📤 **Submit Proof**.\n"
            "4. Paste the Reddit comment link.\n"
            "5. Our team will review it and approve within 24 hours.\n\n"
            "✅ Make sure the link is to your comment, not just the post!"
        )
    elif topic == "payments":
        text = (
            "💰 **How Payments Work**\n\n"
            "1. Once your submission is approved, earnings are added to your balance.\n"
            "2. Go to 💰 **Payments** → **Set UPI ID** to add your UPI address.\n"
            "3. Optionally upload a QR code via 🖼 **Upload QR**.\n"
            "4. Our team processes payouts directly to your UPI — no waiting around!\n\n"
            "📊 You can track your earnings anytime in 📊 **My Stats**."
        )
    elif topic == "rules":
        text = (
            "📜 **Community Guidelines**\n\n"
            "✅ Post only the assigned comment — word for word.\n"
            "✅ One account per person.\n"
            "✅ Keep your comment live after submitting proof.\n"
            "✅ Submit the link to your own comment only.\n\n"
            "These guidelines keep the platform fair and payments flowing smoothly for everyone. 🙌\n\n"
            "If you have questions or something seems wrong, reach out to an admin."
        )
    else:
        text = "❓ Unknown topic."
    await callback.message.edit_text(text, parse_mode="Markdown")
    await callback.answer()


@dp.message(button_filter(BTN_BACK))
async def back(message: Message):
    log_button_click(message, BTN_BACK)
    clear_user_state(message.from_user.id, "back")
    await show_home(message)


@dp.message(F.photo)
async def handle_photo(message: Message):
    state = user_states.get(message.from_user.id)
    if not state or state.get("flow") != "upload_qr":
        await message.answer("Use 💰 Payments → 🖼 Upload QR before sending a QR image.")
        return
    await register_user(message.from_user.id, message.from_user.username)
    file_id = message.photo[-1].file_id
    logging.info("QR upload received: user=%s file_id=%s", message.from_user.id, file_id)
    saved_qr = await save_qr_file_id(message.from_user.id, file_id)
    logging.info(
        "QR file_id saved: user=%s db_file_id=%s uploaded_at=%s",
        message.from_user.id,
        saved_qr["qr_file_id"] if saved_qr else None,
        saved_qr["qr_uploaded_at"] if saved_qr else None,
    )
    clear_user_state(message.from_user.id, "qr_saved")
    await message.answer(
        "✅ QR code saved!\n\nAdmins will see your QR when processing your payment.",
        reply_markup=payments_menu(),
    )


@dp.message()
async def handle_text(message: Message, bot: Bot):
    if not message.text:
        await message.answer("Please use the menu buttons.")
        return
    
    key = button_key(message.text)
    text_lower = message.text.strip().lower()
    
    # Global Admin bypass for maintenance check
    is_user_admin = await is_admin(message.from_user.id)
    if not is_user_admin and await get_setting("maintenance_mode", "0") == "1":
        await message.answer("⚠ System temporarily under maintenance.\n\nPlease try again later.")
        return

    # Global navigation handling
    if text_lower in {"home", "🏠 home"} or key == BTN_HOME:
        clear_user_state(message.from_user.id, "global_home")
        await show_home(message)
        return

    if text_lower in {"cancel"} or key == BTN_CANCEL:
        clear_user_state(message.from_user.id, "global_cancel")
        await message.answer("Action cancelled.")
        await show_home(message)
        return
        
    if text_lower in {"back", "⬅️ back"} or key == BTN_BACK:
        state = user_states.get(message.from_user.id)
        if state and "previous_step" in state:
            # Simple one-level back support
            state["step"] = state.pop("previous_step")
            await message.answer("Going back...")
            # Re-trigger the current flow with the reverted step
            await continue_state_flow(message, state, bot)
            return
        else:
            clear_user_state(message.from_user.id, "global_back")
            await show_home(message)
            return

    state = user_states.get(message.from_user.id)
    button_match = button_key(message.text)
    logging.info(
        "Text routed: user=%s text=%r active_state=%s button=%s",
        message.from_user.id,
        message.text,
        describe_state(state),
        button_match,
    )
    if state:
        if button_match and button_match not in {BTN_BACK, BTN_HOME, BTN_CANCEL}:
            clear_user_state(message.from_user.id, "button_interruption")
            await message.answer("Previous action interrupted.")
            # Continue to handle the button click below
        else:
            await continue_state_flow(message, state, bot)
            return

    # Handle main menu buttons if no state or state was cleared
    if button_match == BTN_CLAIM: await claim(message)
    elif button_match == BTN_SUBMIT: await start_submit_flow(message)
    elif button_match == BTN_MY_STATS: await my_stats(message)
    elif button_match == BTN_PAYMENTS: await payments_menu_open(message)
    elif button_match == BTN_RULES: await rules(message)
    elif button_match == BTN_HELP: await help_button(message)
    
    # Admin Top Level
    elif button_match == BTN_ADMIN_TASKS: await message.answer("📂 Tasks Management", reply_markup=admin_tasks_menu())
    elif button_match == BTN_ADMIN_REVIEWS: await message.answer("🧾 Reviews Queue", reply_markup=admin_reviews_menu())
    elif button_match == BTN_ADMIN_PAYMENTS: await message.answer("💸 Payments Management", reply_markup=admin_payments_menu())
    elif button_match == BTN_ADMIN_MEMBERS: await message.answer("👥 Members Management", reply_markup=admin_members_menu())
    elif button_match == BTN_ADMIN_ANALYTICS: await message.answer("📊 System Analytics", reply_markup=admin_analytics_menu())
    elif button_match == BTN_ADMIN_BROADCAST: await broadcast_start(message)
    elif button_match == BTN_ADMIN_SETTINGS: await admin_settings_handler(message)

    # Task Submenu
    elif button_match == BTN_TASKS_NEW: await start_new_task(message)
    elif button_match == BTN_TASKS_ADD_COMMENTS: await start_add_comments(message)
    elif button_match == BTN_TASKS_ACTIVE: await active_tasks(message)
    elif button_match == BTN_TASKS_STATS: await task_stats(message)
    elif button_match == BTN_TASKS_MANAGE: await manage_tasks_button(message)
    elif button_match == BTN_TASKS_ARCHIVED: await archived_tasks(message)

    # Review Submenu
    elif button_match == BTN_REVIEWS_PENDING: await review_button(message)
    elif button_match == BTN_REVIEWS_FLAGGED: await flagged_submissions(message)
    elif button_match == BTN_REVIEWS_HISTORY: await review_history(message)

    # Payment Submenu
    elif button_match == BTN_PAYMENTS_PENDING: await pending_payments(message, bot)
    elif button_match == BTN_PAYMENTS_PAID: await paid_payments_history(message)
    elif button_match == BTN_PAYMENTS_STATS: await payment_stats_handler(message)

    # Member Submenu
    elif button_match == BTN_MEMBERS_SEARCH: await member_stats_admin(message)
    elif button_match == BTN_MEMBERS_WARNED: await warned_members(message)
    elif button_match == BTN_MEMBERS_BANNED: await banned_members(message)
    elif button_match == BTN_MEMBERS_TRUSTED: await trusted_members_handler(message)

    # Analytics Submenu
    elif button_match == BTN_ANALYTICS_DAILY: await command_dailystats(message)
    elif button_match == BTN_ANALYTICS_SYSTEM: await system_stats_handler(message)
    elif button_match == BTN_ANALYTICS_TOP: await command_leaderboard(message)
    elif button_match == BTN_ANALYTICS_EARNINGS: await earnings_stats_handler(message)

    # User Payments
    elif button_match == BTN_SET_UPI: await set_upi(message)
    elif button_match == BTN_UPLOAD_QR: await upload_qr(message)
    elif button_match == BTN_PAYMENT_HISTORY: await payment_history(message)
    elif button_match == BTN_TOTAL_EARNINGS: await total_earnings(message)
    elif button_match == BTN_BACK: await show_home(message)
    else:
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
        
        await message.answer(
            f"👥 **Member Profile: {member_id}**\n\n"
            f"Status: {status_line}\n"
            f"Level: {stats['level']}\n"
            f"Reputation: {stats['reputation']} ⭐\n"
            f"Streak: 🔥 {stats['streak']}\n"
            f"Badges: {stats['badges'] or 'None'}\n\n"
            f"✅ Approved: {stats['approved']}\n"
            f"⏳ Pending: {stats['pending']}\n"
            f"❌ Rejected: {stats['rejected']}\n"
            f"⭐ Approval Rate: {stats['approval_rate']}%\n\n"
            f"⚠ Warnings: {stats['warnings']}\n"
            f"📝 Notes: {stats['notes'] or 'No notes'}",
            parse_mode="Markdown"
        )
        clear_user_state(message.from_user.id, "member_stats_done")
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
            logging.exception("Could not notify worker about rejection")
        await message.answer(f"✅ Submission rejected and member notified.")
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


async def continue_new_task(message, state, text):
    if state["step"] == "post_url":
        details = parse_reddit_url(text)
        if not details:
            await message.answer("Invalid Reddit post URL. Please send a valid link or use buttons below.", reply_markup=back_cancel_keyboard())
            return
            
        if await post_exists(details["post_id"]):
            await message.answer("⚠️ This Reddit post is already in the system. Duplicate tasks are not allowed.", reply_markup=back_cancel_keyboard())
            return

        state["post_url"] = text
        state["details"] = details
        state["previous_step"] = "post_url"
        state["step"] = "payout"
        logging.info("Task creation advanced: admin=%s step=payout", message.from_user.id)
        await message.answer("Send payout amount. Example: ₹10", reply_markup=back_cancel_keyboard())
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
    if state["step"] == "min_level":
        # Usually handled by callback, but if text input:
        await message.answer("Please use the buttons below to choose a minimum level:", reply_markup=level_keyboard())
        return
    if state["step"] == "priority":
        # Handled by callback
        await message.answer("Please use the buttons above to choose priority.", reply_markup=priority_keyboard())
        return
    if state["step"] == "instructions":
        instructions = "" if text == "/skip" else text
        details = state["details"]
        task_id = await create_task(
            state["post_url"], details["normalized_url"], details["subreddit"],
            details["post_id"], details["post_path"], state["payout"],
            state["slots"], state["category"], instructions, message.from_user.id,
            priority=state.get("priority", "normal"),
            minimum_level=state.get("min_level", "Beginner")
        )
        logging.info("Task created: id=%s admin=%s", task_id, message.from_user.id)
        set_user_state(message.from_user.id, {"flow": "add_comments", "step": "comments", "task_id": task_id})
        await message.answer(
            f"✅ Task #{task_id} created!\n\n"
            f"📝 Now send the comments for this task.\n\n"
            "You can use any format:\n"
            "• ---COMMENT--- separator\n"
            "• Numbered list (1. 2. 3.)\n"
            "• Bullet points (* or •)\n\n"
            "Normal line breaks inside a comment are kept.\n"
            "Send /skip to add comments later.",
            reply_markup=back_cancel_keyboard(),
        )


def parse_comments(text):
    """
    Advanced comment parser supporting:
    - ---COMMENT--- separator
    - Numbered lists (1. comment)
    - Bullet points (* comment, • comment, - comment)
    Regex-based splitting ensures internal paragraph breaks are preserved.
    """
    # Split by explicit separator first
    initial_chunks = text.split("---COMMENT---")
    
    all_extracted = []
    # Pattern to match numbered list (1. ) or bullets (* , • , - ) at start of line
    split_pattern = r"(?m)^(?:\d+\.\s+|[\*•\-]\s+)"
    
    for chunk in initial_chunks:
        # Standardize by marking split points
        marker = "|||VIRON_SPLIT|||"
        # Pre-strip the chunk to avoid issues with leading whitespace
        chunk = chunk.strip()
        if not chunk:
            continue
            
        # If the chunk starts with a bullet/number, re.sub will replace it.
        # If it doesn't, the first part is a comment.
        marked = re.sub(split_pattern, marker, chunk)
        
        # Split by marker
        sub_chunks = marked.split(marker)
        for sc in sub_chunks:
            clean_sc = sc.strip()
            # Filter: minimum length 15, not empty
            if clean_sc and len(clean_sc) >= 15:
                all_extracted.append(clean_sc)

    # De-duplicate while preserving order
    seen = set()
    unique_comments = []
    for comment in all_extracted:
        if comment not in seen:
            unique_comments.append(comment)
            seen.add(comment)
            
    return unique_comments


async def continue_add_comments(message, state, text):
    if state["step"] == "task_id":
        match = re.search(r'\d+', text)
        if not match:
            await message.answer("Send a valid task ID.", reply_markup=back_cancel_keyboard())
            return
        state["task_id"] = int(match.group())
        state["previous_step"] = "task_id"
        state["step"] = "comments"
        logging.info("Comment upload advanced: admin=%s task=%s step=comments", message.from_user.id, state["task_id"])
        await message.answer(
            f"📝 Task #{state['task_id']} selected. Send the comments now.\n\n"
            "Supported formats:\n"
            "• ---COMMENT--- separator\n"
            "• Numbered list (1. 2. 3.)\n"
            "• Bullet points (* or •)\n\n"
            "Line breaks inside each comment are preserved.",
            reply_markup=back_cancel_keyboard(),
        )
        return
    
    if text.strip() == "/skip":
        clear_user_state(message.from_user.id, "comments_skipped")
        await message.answer("Comments skipped. You can add them later via 📝 Add Comments.", reply_markup=admin_tasks_menu())
        return

    comments = parse_comments(text)
    if not comments:
        await message.answer("No valid comments found. Each comment must be at least 15 characters long.")
        return

    added = await add_comments_to_task(state["task_id"], comments)
    if added is None:
        await message.answer("Task not found, closed, or unavailable.", reply_markup=admin_tasks_menu())
        return
    clear_user_state(message.from_user.id, "comments_added")
    logging.info("Comments added: task=%s count=%s", state["task_id"], added)
    await message.answer(
        f"✅ {added} comment(s) added to Task #{state['task_id']}.\n\n"
        "The task is now live for workers to claim! 🚀",
        reply_markup=admin_tasks_menu(),
    )


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
    if not await is_admin(message.from_user.id):
        await message.answer("Only admins can add comments.")
        return
    parts = text.split(maxsplit=1)
    if len(parts) < 2:
        await message.answer("Use: /addcomment task_id comment text")
        return
    match = re.search(r'\d+', parts[0])
    if not match:
        await message.answer("Invalid task ID.")
        return
    task_id = int(match.group())
    
    comments = parse_comments(parts[1])
    if not comments:
        await message.answer("No valid comments found.")
        return
        
    added = await add_comments_to_task(task_id, comments)
    if added is None:
        await message.answer("Task not found, closed, or unavailable.")
        return
    await message.answer(f"✅ Successfully added {added} new comment(s).")


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
        await message_or_callback.message.edit_text(text, reply_markup=markup, parse_mode="Markdown", disable_web_page_preview=True)
    else:
        await message_or_callback.answer(text, reply_markup=markup, parse_mode="Markdown", disable_web_page_preview=True)


@dp.callback_query(F.data.startswith("review:page:"))
async def review_page_callback(callback: CallbackQuery):
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
    parts = command.args.split(maxsplit=1)
    user_id = int(parts[0])
    reason = parts[1] if len(parts) > 1 else "No reason"
    await ban_user(user_id, reason)
    await log_audit_action(message.from_user.id, "ban", f"Banned {user_id}: {reason}")
    await message.answer(f"User {user_id} banned.")

@dp.message(Command("unban"))
async def unban_command(message: Message, command: CommandObject):
    if not await is_admin(message.from_user.id): return
    if not command.args:
        await message.answer("Use: /unban user_id")
        return
    user_id = int(command.args.strip())
    await unban_user(user_id)
    await log_audit_action(message.from_user.id, "unban", f"Unbanned {user_id}")
    await message.answer(f"User {user_id} unbanned.")

@dp.message(Command("addnote"))
async def note_command(message: Message, command: CommandObject):
    if not await is_admin(message.from_user.id): return
    if not command.args:
        await message.answer("Use: /addnote user_id note text")
        return
    parts = command.args.split(maxsplit=1)
    if len(parts) < 2: return
    user_id = int(parts[0])
    note = parts[1]
    await add_member_note(user_id, note)
    await message.answer(f"Note added to {user_id}.")

@dp.message(Command("warn"))
async def warn_command(message: Message, command: CommandObject):
    if not await is_admin(message.from_user.id): return
    if not command.args:
        await message.answer("Use: /warn user_id")
        return
    user_id = int(command.args.strip())
    count = await add_member_warning(user_id)
    await log_audit_action(message.from_user.id, "warn", f"Warned {user_id}")
    await message.answer(f"User {user_id} warned. Total warnings: {count}")
    if count >= 3:
        await ban_user(user_id, "Auto-banned due to excessive warnings.")
        await message.answer(f"User {user_id} auto-banned due to reaching {count} warnings.")

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
        parse_mode="Markdown"
    )

@dp.message(button_filter(BTN_MEMBERS_TRUSTED))
async def trusted_members_handler(message: Message):
    # This could be a filtered list, but for now we'll just list members
    await list_members(message, page=1)


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
        username = f"@{stats.get('username')}" if stats and stats.get('username') else str(mid)
        level = stats.get('level', 'Beginner') if stats else "N/A"
        lines.append(f"• {username} ({level}) - `{mid}`")
        
    text = "\n".join(lines)
    keyboard = get_pagination_keyboard(page, total_pages, "members")
    markup = InlineKeyboardMarkup(inline_keyboard=keyboard)
    
    if isinstance(message_or_callback, CallbackQuery):
        await message_or_callback.message.edit_text(text, reply_markup=markup, parse_mode="Markdown")
    else:
        await message_or_callback.answer(text, reply_markup=markup, parse_mode="Markdown")


@dp.callback_query(F.data.startswith("members:page:"))
async def members_page_callback(callback: CallbackQuery):
    page = int(callback.data.split(":")[2])
    await list_members(callback, page)
    await callback.answer()

@dp.message(Command("leaderboard"))
@dp.message(button_filter(BTN_ANALYTICS_TOP))
async def command_leaderboard(message: Message):
    rows = await get_leaderboard()
    lines = ["🏆 **TOP MEMBERS**\n"]
    for i, row in enumerate(rows, 1):
        lines.append(f"{i}. @{row['username'] or row['telegram_id']} - {row['level']}\n"
                     f"   ✅ {row['approved_tasks']} tasks | 💰 ₹{row['total_earned'] or 0}")
    await message.answer("\n".join(lines) if rows else "No leaderboard data.", parse_mode="Markdown")

async def archived_tasks(message: Message):
    if not await is_admin(message.from_user.id):
        return
    async with __import__('aiosqlite').connect('bot.db') as db:
        db.row_factory = __import__('aiosqlite').Row
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
    await message.answer("\n".join(lines), parse_mode="Markdown")


async def review_history(message: Message):
    if not await is_admin(message.from_user.id):
        return
    async with __import__('aiosqlite').connect('bot.db') as db:
        db.row_factory = __import__('aiosqlite').Row
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
    await message.answer("\n".join(lines), parse_mode="Markdown")


async def paid_payments_history(message: Message):
    if not await is_admin(message.from_user.id):
        return
    async with __import__('aiosqlite').connect('bot.db') as db:
        db.row_factory = __import__('aiosqlite').Row
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
    await message.answer("\n".join(lines), parse_mode="Markdown")


async def payment_stats_handler(message: Message):
    if not await is_admin(message.from_user.id):
        return
    async with __import__('aiosqlite').connect('bot.db') as db:
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
    total = sum(float(re.search(r'[\d.]+', r[0]).group()) for r in paid_rows if re.search(r'[\d.]+', r[0]))
    await message.answer(
        f"📊 **Payment Stats**\n\n"
        f"⏳ Pending: {row[0]}\n"
        f"✅ Paid: {row[1]}\n"
        f"❌ Failed/Cancelled: {row[2]}\n"
        f"💰 Total Paid Out: ₹{total:.2f}",
        parse_mode="Markdown",
    )


async def warned_members(message: Message):
    if not await is_admin(message.from_user.id):
        return
    async with __import__('aiosqlite').connect('bot.db') as db:
        db.row_factory = __import__('aiosqlite').Row
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
    await message.answer("\n".join(lines), parse_mode="Markdown")


async def banned_members(message: Message):
    if not await is_admin(message.from_user.id):
        return
    async with __import__('aiosqlite').connect('bot.db') as db:
        db.row_factory = __import__('aiosqlite').Row
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
    await message.answer("\n".join(lines), parse_mode="Markdown")


async def earnings_stats_handler(message: Message):
    if not await is_admin(message.from_user.id):
        return
    async with __import__('aiosqlite').connect('bot.db') as db:
        db.row_factory = __import__('aiosqlite').Row
        cursor = await db.execute("""
        SELECT payments.username, payments.user_id,
               SUM(CAST(REPLACE(REPLACE(amount, '₹', ''), ' ', '') AS REAL)) AS total_earned,
               COUNT(*) AS payment_count
        FROM payments
        WHERE status = 'paid'
        GROUP BY payments.user_id
        ORDER BY total_earned DESC
        LIMIT 10
        """)
        rows = [dict(r) for r in await cursor.fetchall()]
    if not rows:
        await message.answer("No earnings data yet.")
        return
    lines = ["💰 **Top Earners**\n"]
    for i, r in enumerate(rows, 1):
        name = f"@{r['username']}" if r['username'] else f"ID:{r['user_id']}"
        lines.append(f"{i}. {name} — ₹{r['total_earned'] or 0:.2f} ({r['payment_count']} payments)")
    await message.answer("\n".join(lines), parse_mode="Markdown")


@dp.error()
async def handle_error(event: ErrorEvent):
    logging.error(
        "Update failed",
        exc_info=(type(event.exception), event.exception, event.exception.__traceback__),
    )
    return True


async def main():
    if not BOT_TOKEN:
        raise RuntimeError("BOT_TOKEN is missing in .env")
    if not ADMIN_IDS:
        logging.warning("ADMIN_IDS is empty. Admin buttons will be blocked.")
    await create_db()
    bot = Bot(token=BOT_TOKEN)
    logging.info("Bot started")
    await dp.start_polling(bot)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
