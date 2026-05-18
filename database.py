import asyncio
import logging
import re
from decimal import Decimal, InvalidOperation

import aiosqlite


def _sum_payment_amounts(payments):
    """Sum a list of {amount: '<currency><num>'} dicts; return '<currency><num>' string.

    Mirrors the simple summation in main.total_amount so database.py stays
    self-contained (no import-cycle with main.py).
    """
    total = Decimal("0")
    currency = ""
    for p in payments:
        raw = (p.get("amount") or "").strip()
        m = re.search(r"([0-9]+(?:\.[0-9]+)?)", raw)
        if not m:
            continue
        if not currency:
            currency = raw[:m.start()].strip() or raw[m.end():].strip()
        try:
            total += Decimal(m.group(1))
        except InvalidOperation:
            continue
    n_text = str(int(total)) if total == total.to_integral() else f"{total:.2f}".rstrip("0").rstrip(".")
    return f"{currency}{n_text}" if currency else n_text

DB_NAME = "bot.db"
_db_lock = asyncio.Lock()


async def create_db():
    """Create tables and safely upgrade older local databases."""
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA synchronous=NORMAL")

        # 1. CREATE ALL TABLES (BASE SCHEMA)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_id INTEGER UNIQUE NOT NULL,
            username TEXT,
            upi_id TEXT,
            qr_file_id TEXT,
            qr_uploaded_at TIMESTAMP,
            is_banned INTEGER DEFAULT 0,
            last_claim_at TIMESTAMP,
            last_submit_at TIMESTAMP,
            total_claims INTEGER DEFAULT 0,
            total_submissions INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            notes TEXT,
            warnings INTEGER DEFAULT 0,
            role TEXT DEFAULT 'member',
            reputation_score INTEGER DEFAULT 100,
            is_shadowbanned INTEGER DEFAULT 0,
            badges TEXT,
            streak_count INTEGER DEFAULT 0,
            last_active_at TIMESTAMP,
            max_reddit_accounts INTEGER DEFAULT 1
        )
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS tasks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            post_url TEXT NOT NULL,
            normalized_post_url TEXT NOT NULL,
            subreddit TEXT NOT NULL,
            post_id TEXT NOT NULL,
            post_path TEXT NOT NULL,
            payout_amount TEXT NOT NULL,
            category TEXT DEFAULT 'Comment',
            total_slots INTEGER NOT NULL,
            instructions TEXT,
            status TEXT DEFAULT 'active',
            created_by INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            closed_at TIMESTAMP,
            minimum_level TEXT DEFAULT 'Beginner',
            admin_notes TEXT,
            priority TEXT DEFAULT 'normal',
            is_boosted INTEGER DEFAULT 0,
            tags TEXT,
            min_reputation INTEGER DEFAULT 0,
            archived INTEGER DEFAULT 0
        )
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS comments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            task_id INTEGER,
            text TEXT NOT NULL,
            assigned INTEGER DEFAULT 0,
            assigned_to INTEGER,
            assigned_at TIMESTAMP,
            times_assigned INTEGER DEFAULT 0,
            times_approved INTEGER DEFAULT 0,
            times_rejected INTEGER DEFAULT 0,
            times_removed INTEGER DEFAULT 0,
            last_used_at TIMESTAMP,
            status TEXT DEFAULT 'available',
            reused_count INTEGER DEFAULT 0,
            UNIQUE(task_id, text),
            FOREIGN KEY (task_id) REFERENCES tasks(id)
        )
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS submissions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            username TEXT,
            task_id INTEGER,
            comment_id INTEGER,
            comment_text TEXT NOT NULL,
            reddit_link TEXT NOT NULL,
            normalized_reddit_link TEXT,
            reddit_comment_id TEXT,
            status TEXT DEFAULT 'pending_review',
            rejection_reason TEXT,
            submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            reviewed_at TIMESTAMP,
            reviewed_by INTEGER,
            is_disputed INTEGER DEFAULT 0,
            admin_note TEXT,
            archived INTEGER DEFAULT 0,
            comment_alive INTEGER,
            comment_last_checked_at TIMESTAMP,
            comment_check_count INTEGER DEFAULT 0,
            live_status TEXT DEFAULT 'unchecked',
            last_live_check TIMESTAMP,
            live_duration_hours REAL DEFAULT 0,
            is_payable INTEGER DEFAULT 0,
            first_seen_live_at TIMESTAMP,
            reddit_account_id INTEGER,
            reddit_author TEXT,
            FOREIGN KEY (task_id) REFERENCES tasks(id),
            FOREIGN KEY (comment_id) REFERENCES comments(id)
        )
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            username TEXT,
            task_id INTEGER NOT NULL,
            submission_id INTEGER NOT NULL,
            amount TEXT NOT NULL,
            payment_method TEXT DEFAULT 'UPI',
            status TEXT DEFAULT 'pending',
            paid_at TIMESTAMP,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            archived INTEGER DEFAULT 0,
            FOREIGN KEY (submission_id) REFERENCES submissions(id)
        )
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS worker_levels (
            user_id INTEGER PRIMARY KEY,
            level TEXT DEFAULT 'Beginner',
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS flags (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            submission_id INTEGER,
            reason TEXT NOT NULL,
            status TEXT DEFAULT 'open',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS audit_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            admin_id INTEGER NOT NULL,
            action TEXT NOT NULL,
            details TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS comment_assignment_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            task_id INTEGER NOT NULL,
            comment_id INTEGER NOT NULL,
            assigned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(telegram_id),
            FOREIGN KEY (task_id) REFERENCES tasks(id),
            FOREIGN KEY (comment_id) REFERENCES comments(id)
        )
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS reddit_accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            telegram_user_id INTEGER NOT NULL,
            reddit_username TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'pending',
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            approved_at TIMESTAMP,
            approved_by INTEGER,
            rejected_reason TEXT,
            last_used_at TIMESTAMP,
            notes TEXT,
            warnings INTEGER DEFAULT 0,
            FOREIGN KEY (telegram_user_id) REFERENCES users(telegram_id)
        )
        """)

        await db.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """)

        # 2. RUN MIGRATIONS (ADD MISSING COLUMNS TO OLD DATABASES)
        await _upgrade_columns(db)

        # 3. CREATE ALL INDEXES (SAFE NOW THAT COLUMNS EXIST)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_submissions_user_id ON submissions(user_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_submissions_task_id ON submissions(task_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_submissions_status ON submissions(status)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_submissions_submitted_at ON submissions(submitted_at)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_tasks_archived ON tasks(archived)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_comments_task_id ON comments(task_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_comments_assigned_to ON comments(assigned_to)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_comments_assigned ON comments(assigned)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_payments_user_id ON payments(user_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_payments_status ON payments(status)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_users_telegram_id ON users(telegram_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)")
        
        await db.execute("CREATE INDEX IF NOT EXISTS idx_tasks_priority ON tasks(priority)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_submissions_comment_id ON submissions(comment_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_submissions_reddit_cid ON submissions(reddit_comment_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_payments_submission_id ON payments(submission_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_submissions_comment_alive ON submissions(comment_alive)")

        await db.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_submissions_user_comment
        ON submissions(user_id, comment_id)
        WHERE comment_id IS NOT NULL
        """)
        await db.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_submissions_normalized_link
        ON submissions(normalized_reddit_link)
        WHERE normalized_reddit_link IS NOT NULL
        """)
        await db.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_submissions_reddit_comment_id
        ON submissions(reddit_comment_id)
        WHERE reddit_comment_id IS NOT NULL
        """)

        await db.execute("CREATE INDEX IF NOT EXISTS idx_c_assignment_user ON comment_assignment_history(user_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_c_assignment_comment ON comment_assignment_history(comment_id)")

        await db.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_reddit_accounts_username_unique
        ON reddit_accounts(reddit_username COLLATE NOCASE)
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS idx_reddit_accounts_user ON reddit_accounts(telegram_user_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_reddit_accounts_status ON reddit_accounts(status)")

        await db.execute("CREATE INDEX IF NOT EXISTS idx_submissions_live_status ON submissions(live_status)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_submissions_is_payable ON submissions(is_payable)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_submissions_last_live_check ON submissions(last_live_check)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_submissions_reddit_account ON submissions(reddit_account_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_submissions_reddit_author ON submissions(reddit_author)")

        # 4. DATA MIGRATIONS
        await _migrate_comment_statuses(db)
        await _backfill_live_status(db)
        await db.commit()


async def _upgrade_columns(db):
    """Add missing columns when this project already has an older bot.db."""
    columns = {
        "users": [
            ("upi_id", "TEXT"),
            ("qr_file_id", "TEXT"),
            ("qr_uploaded_at", "TIMESTAMP"),
            ("is_banned", "INTEGER DEFAULT 0"),
            ("last_claim_at", "TIMESTAMP"),
            ("last_submit_at", "TIMESTAMP"),
            ("notes", "TEXT"),
            ("warnings", "INTEGER DEFAULT 0"),
            ("role", "TEXT DEFAULT 'member'"),
            ("reputation_score", "INTEGER DEFAULT 100"),
            ("is_shadowbanned", "INTEGER DEFAULT 0"),
            ("badges", "TEXT"),
            ("streak_count", "INTEGER DEFAULT 0"),
            ("last_active_at", "TIMESTAMP"),
            ("max_reddit_accounts", "INTEGER DEFAULT 1"),
        ],
        "tasks": [
            ("category", "TEXT DEFAULT 'Comment'"),
            ("instructions", "TEXT"),
            ("minimum_level", "TEXT DEFAULT 'Beginner'"),
            ("admin_notes", "TEXT"),
            ("priority", "TEXT DEFAULT 'normal'"),
            ("is_boosted", "INTEGER DEFAULT 0"),
            ("tags", "TEXT"),
            ("min_reputation", "INTEGER DEFAULT 0"),
            ("archived", "INTEGER DEFAULT 0"),
        ],
        "comments": [
            ("task_id", "INTEGER"),
            ("times_assigned", "INTEGER DEFAULT 0"),
            ("times_approved", "INTEGER DEFAULT 0"),
            ("times_rejected", "INTEGER DEFAULT 0"),
            ("times_removed", "INTEGER DEFAULT 0"),
            ("last_used_at", "TIMESTAMP"),
            ("status", "TEXT DEFAULT 'available'"),
            ("reused_count", "INTEGER DEFAULT 0"),
        ],
        "submissions": [
            ("task_id", "INTEGER"),
            ("comment_id", "INTEGER"),
            ("normalized_reddit_link", "TEXT"),
            ("reddit_comment_id", "TEXT"),
            ("status", "TEXT DEFAULT 'pending_review'"),
            ("rejection_reason", "TEXT"),
            ("reviewed_at", "TIMESTAMP"),
            ("reviewed_by", "INTEGER"),
            ("is_disputed", "INTEGER DEFAULT 0"),
            ("admin_note", "TEXT"),
            ("archived", "INTEGER DEFAULT 0"),
            ("comment_alive", "INTEGER"),
            ("comment_last_checked_at", "TIMESTAMP"),
            ("comment_check_count", "INTEGER DEFAULT 0"),
            ("live_status", "TEXT DEFAULT 'unchecked'"),
            ("last_live_check", "TIMESTAMP"),
            ("live_duration_hours", "REAL DEFAULT 0"),
            ("is_payable", "INTEGER DEFAULT 0"),
            ("first_seen_live_at", "TIMESTAMP"),
            ("reddit_account_id", "INTEGER"),
            ("reddit_author", "TEXT"),
        ],
        "payments": [
            ("payment_method", "TEXT DEFAULT 'UPI'"),
            ("archived", "INTEGER DEFAULT 0"),
        ],
        "reddit_accounts": [
            ("approved_at", "TIMESTAMP"),
            ("approved_by", "INTEGER"),
            ("rejected_reason", "TEXT"),
            ("last_used_at", "TIMESTAMP"),
            ("notes", "TEXT"),
            ("warnings", "INTEGER DEFAULT 0"),
        ],
    }

    for table, table_columns in columns.items():
        for column_name, column_type in table_columns:
            await _add_missing_column(db, table, column_name, column_type)

    await db.execute("""
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT
    )
    """)


async def _migrate_comment_statuses(db):
    """One-time migration to set comment status based on assigned flag and submissions."""
    # 1. New comments (assigned=0) -> available
    await db.execute("UPDATE comments SET status = 'available' WHERE assigned = 0 AND status IS NULL")
    
    # 2. Assigned comments (assigned=1)
    # Check if there is an approved submission
    await db.execute("""
        UPDATE comments
        SET status = 'approved'
        WHERE assigned = 1 AND id IN (
            SELECT comment_id FROM submissions WHERE status = 'approved'
        )
    """)
    
    # Check if there is a pending submission
    await db.execute("""
        UPDATE comments
        SET status = 'submitted'
        WHERE assigned = 1 AND status != 'approved' AND id IN (
            SELECT comment_id FROM submissions WHERE status = 'pending_review'
        )
    """)
    
    # Otherwise just claimed
    await db.execute("""
        UPDATE comments
        SET status = 'claimed'
        WHERE assigned = 1 AND status NOT IN ('approved', 'submitted')
    """)
    
    # Default everything else to available if still NULL
    await db.execute("UPDATE comments SET status = 'available' WHERE status IS NULL")


async def _backfill_live_status(db):
    """One-shot: map legacy comment_alive (0/1/NULL) onto the new live_status column.

    Skips rows that already have a non-default live_status so reruns are no-ops.
    Also recomputes is_payable from current state (approved + live + 24h survived).
    """
    await db.execute("""
        UPDATE submissions
        SET live_status = 'removed'
        WHERE comment_alive = 0 AND (live_status IS NULL OR live_status = 'unchecked')
    """)
    await db.execute("""
        UPDATE submissions
        SET live_status = 'live',
            first_seen_live_at = COALESCE(first_seen_live_at, comment_last_checked_at, submitted_at)
        WHERE comment_alive = 1 AND (live_status IS NULL OR live_status = 'unchecked')
    """)
    await db.execute("""
        UPDATE submissions
        SET last_live_check = COALESCE(last_live_check, comment_last_checked_at)
        WHERE last_live_check IS NULL AND comment_last_checked_at IS NOT NULL
    """)
    # Compute is_payable for approved+live+24h-old submissions
    await db.execute("""
        UPDATE submissions
        SET is_payable = 1
        WHERE status = 'approved'
          AND archived = 0
          AND live_status = 'live'
          AND datetime(submitted_at) <= datetime('now', '-24 hours')
    """)
    await db.execute("""
        UPDATE submissions
        SET is_payable = 0
        WHERE is_payable = 1
          AND NOT (status = 'approved' AND archived = 0 AND live_status = 'live'
                   AND datetime(submitted_at) <= datetime('now', '-24 hours'))
    """)


async def _add_missing_column(db, table_name, column_name, column_type):
    cursor = await db.execute(f"PRAGMA table_info({table_name})")
    rows = await cursor.fetchall()
    if column_name not in [row[1] for row in rows]:
        await db.execute(
            f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type}"
        )


async def register_user(telegram_id, username):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("BEGIN IMMEDIATE")
        cursor = await db.execute("SELECT id FROM users WHERE telegram_id = ?", (telegram_id,))
        is_new = await cursor.fetchone() is None
        await db.execute("""
        INSERT INTO users (telegram_id, username)
        VALUES (?, ?)
        ON CONFLICT(telegram_id) DO UPDATE SET username = excluded.username
        """, (telegram_id, username))
        await db.execute("""
        INSERT OR IGNORE INTO worker_levels (user_id, level)
        VALUES (?, 'Beginner')
        """, (telegram_id,))
        await db.commit()
        return is_new


async def get_user(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
        SELECT users.*, worker_levels.level
        FROM users
        LEFT JOIN worker_levels ON worker_levels.user_id = users.telegram_id
        WHERE telegram_id = ?
        """, (user_id,))
        row = await cursor.fetchone()
        return dict(row) if row else None


async def save_upi_id(user_id, upi_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("BEGIN IMMEDIATE")
        await db.execute("UPDATE users SET upi_id = ? WHERE telegram_id = ?", (upi_id, user_id))
        await db.commit()


async def save_qr_file_id(user_id, qr_file_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("BEGIN IMMEDIATE")
        await db.execute("""
        INSERT INTO users (telegram_id, qr_file_id, qr_uploaded_at)
        VALUES (?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(telegram_id) DO UPDATE SET
            qr_file_id = excluded.qr_file_id,
            qr_uploaded_at = CURRENT_TIMESTAMP
        """, (user_id, qr_file_id))
        await db.commit()
        cursor = await db.execute(
            "SELECT qr_file_id, qr_uploaded_at FROM users WHERE telegram_id = ?",
            (user_id,),
        )
        row = await cursor.fetchone()
        logging.info(
            "QR saved: user=%s file_id=%s uploaded_at=%s",
            user_id,
            row[0] if row else None,
            row[1] if row else None,
        )
        return {
            "qr_file_id": row[0],
            "qr_uploaded_at": row[1],
        } if row else None


async def create_task(post_url, normalized_post_url, subreddit, post_id, post_path,
                      payout_amount, total_slots, category, instructions, created_by, priority='normal', minimum_level='Beginner'):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("BEGIN IMMEDIATE")
        cursor = await db.execute("""
        INSERT INTO tasks (
            post_url, normalized_post_url, subreddit, post_id, post_path,
            payout_amount, total_slots, category, instructions, created_by, priority, minimum_level
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            post_url, normalized_post_url, subreddit, post_id, post_path,
            payout_amount, total_slots, category, instructions, created_by, priority, minimum_level
        ))
        await db.commit()
        return cursor.lastrowid


def _normalize_for_dedup(text):
    """Whitespace + case normalization for duplicate detection (DB-side mirror).

    Mirrors main.normalize_for_dedup so database.py stays self-contained.
    Collapses runs of whitespace to a single space, strips edges, lowercases.
    """
    return re.sub(r"\s+", " ", text or "").strip().lower()


async def add_comments_to_task(task_id, comments):
    """Add a batch of comments to a task with whitespace-normalized dedup.

    Returns dict:
        added                       : int — newly inserted rows
        skipped_dup_in_upload       : int — duplicates within the batch itself
        skipped_dup_existing        : int — duplicates of comments already in this task
        errors                      : int — DB errors during insert (rare)
        total_input                 : int — comments received
        task_unavailable            : bool — task not found or not in active/paused

    The ORIGINAL text (with all formatting) is stored — dedup is purely
    on the normalized form. So "Hello world" and "hello\\n world  " are
    considered duplicates of each other, but only the first one is saved
    (with whichever exact text the admin sent first).
    """
    result = {
        "added": 0,
        "skipped_dup_in_upload": 0,
        "skipped_dup_existing": 0,
        "errors": 0,
        "total_input": len(comments),
        "task_unavailable": False,
    }

    # Pass 1: trim, drop empties, dedup within the upload using normalized form
    seen_norms = set()
    keep = []
    for raw in comments:
        cleaned = (raw or "").strip()
        if not cleaned:
            continue
        norm = _normalize_for_dedup(cleaned)
        if not norm:
            continue
        if norm in seen_norms:
            result["skipped_dup_in_upload"] += 1
            logging.info("Skipped in-upload duplicate: task=%s norm_len=%s", task_id, len(norm))
            continue
        seen_norms.add(norm)
        keep.append((cleaned, norm))

    if not keep:
        logging.info("Comment upload empty after trimming: task=%s", task_id)
        return result

    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("BEGIN IMMEDIATE")
        task = await _get_task_by_id(db, task_id)
        if not task or task["status"] not in {"active", "paused"}:
            logging.info("Comment upload rejected: task=%s unavailable", task_id)
            result["task_unavailable"] = True
            await db.commit()
            return result

        # Load existing comments' normalized forms once for O(1) dedup checks
        cursor = await db.execute(
            "SELECT text FROM comments WHERE task_id = ?", (task_id,)
        )
        existing_norms = {_normalize_for_dedup(row[0]) for row in await cursor.fetchall()}

        for original, norm in keep:
            if norm in existing_norms:
                result["skipped_dup_existing"] += 1
                logging.info("Skipped existing duplicate: task=%s norm_len=%s", task_id, len(norm))
                continue
            try:
                await db.execute(
                    "INSERT INTO comments (task_id, text) VALUES (?, ?)",
                    (task_id, original),
                )
                existing_norms.add(norm)
                result["added"] += 1
            except aiosqlite.IntegrityError:
                # Race-safe fallback if a UNIQUE constraint ever gets added
                result["skipped_dup_existing"] += 1
                logging.info("IntegrityError treated as duplicate: task=%s", task_id)
            except Exception:
                logging.exception("Comment insert failed: task=%s", task_id)
                result["errors"] += 1

        await db.commit()
        logging.info(
            "Comment upload saved: task=%s added=%s dup_in_upload=%s dup_existing=%s errors=%s total=%s",
            task_id, result["added"], result["skipped_dup_in_upload"],
            result["skipped_dup_existing"], result["errors"], result["total_input"],
        )
        return result


async def get_active_claim(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
        SELECT
            comments.id AS comment_id,
            comments.text AS comment_text,
            tasks.id AS task_id,
            tasks.post_url,
            tasks.subreddit,
            tasks.post_id,
            tasks.post_path,
            tasks.payout_amount,
            tasks.category,
            tasks.instructions
        FROM comments
        JOIN tasks ON tasks.id = comments.task_id
        LEFT JOIN submissions ON submissions.comment_id = comments.id
        WHERE comments.assigned_to = ?
          AND comments.assigned = 1
          AND submissions.id IS NULL
        ORDER BY comments.assigned_at DESC
        LIMIT 1
        """, (user_id,))
        row = await cursor.fetchone()
        return dict(row) if row else None


async def claim_comment(user_id, force_reuse=False):
    """Assign one unique comment, respecting priority, rotation, and reputation.

    Concurrent-claim cap is re-checked inside BEGIN IMMEDIATE so rapid double-
    clicks cannot exceed the user's allowed simultaneous-claim count. A user is
    permitted ``min(active_reddit_accounts, users.max_reddit_accounts)`` claims
    at once, floored at 1 so members with no registered Reddit account still
    behave like the legacy single-claim flow.
    """
    async with _db_lock:
        async with aiosqlite.connect(DB_NAME) as db:
            db.row_factory = aiosqlite.Row
            await db.execute("BEGIN IMMEDIATE")

            user = await _get_user_by_id(db, user_id)
            if not user or user["is_banned"]:
                await db.commit()
                return "banned"

            max_allowed = user.get("max_reddit_accounts") or 1
            cur = await db.execute(
                "SELECT COUNT(*) FROM reddit_accounts "
                "WHERE telegram_user_id = ? AND status = 'active'",
                (user_id,),
            )
            active_accounts = (await cur.fetchone())[0] or 0
            allowed_active = max(1, min(active_accounts, max_allowed))

            cur = await db.execute("""
            SELECT comments.id AS comment_id,
                   comments.text AS comment_text,
                   tasks.id AS task_id,
                   tasks.post_url,
                   tasks.subreddit,
                   tasks.post_id,
                   tasks.post_path,
                   tasks.payout_amount,
                   tasks.category,
                   tasks.instructions
            FROM comments
            LEFT JOIN submissions ON submissions.comment_id = comments.id
            JOIN tasks ON tasks.id = comments.task_id
            WHERE comments.assigned_to = ?
              AND comments.assigned = 1
              AND submissions.id IS NULL
            ORDER BY comments.assigned_at DESC
            """, (user_id,))
            existing_rows = [dict(r) for r in await cur.fetchall()]
            current_active = len(existing_rows)
            if current_active >= allowed_active:
                await db.commit()
                first = existing_rows[0] if existing_rows else None
                payload = {
                    "at_limit": True,
                    "current": current_active,
                    "max": allowed_active,
                    "active_claims": existing_rows,
                }
                if first:
                    payload.update({
                        "comment_id": first["comment_id"],
                        "comment_text": first["comment_text"],
                        "task_id": first["task_id"],
                        "post_url": first["post_url"],
                        "subreddit": first["subreddit"],
                        "post_id": first["post_id"],
                        "post_path": first["post_path"],
                        "payout_amount": first["payout_amount"],
                        "category": first["category"],
                        "instructions": first["instructions"],
                        "already_claimed": True,
                    })
                return payload

            # Pick one eligible comment under the IMMEDIATE lock — single source of truth.
            # LOGIC:
            # 1. Available (never used) comments first
            # 2. Reusable comments second (last_used_at > 24h ago)
            # 3. Prevent same comment to same user (bypassed if force_reuse)
            
            history_clause = ""
            if not force_reuse:
                history_clause = """
                AND NOT EXISTS (
                    SELECT 1 FROM comment_assignment_history cah
                    WHERE cah.comment_id = c.id AND cah.user_id = ?
                )
                """
            
            query = f"""
            SELECT
                c.id AS comment_id,
                c.text AS comment_text,
                c.status AS comment_status,
                t.id AS task_id,
                t.post_url,
                t.subreddit,
                t.post_id,
                t.post_path,
                t.payout_amount,
                t.category,
                t.instructions,
                t.priority,
                t.is_boosted
            FROM comments c
            JOIN tasks t ON t.id = c.task_id
            JOIN users u ON u.telegram_id = ?
            WHERE c.status IN ('available', 'reusable')
              AND t.status = 'active'
              AND t.archived = 0
              AND (t.min_reputation <= u.reputation_score)
              AND (
                  SELECT COUNT(*)
                  FROM comments AS claimed_comments
                  WHERE claimed_comments.task_id = t.id
                    AND claimed_comments.status IN ('claimed', 'submitted', 'approved')
              ) < t.total_slots
              AND NOT EXISTS (
                  SELECT 1 FROM submissions s
                  WHERE s.task_id = t.id AND s.user_id = ?
              )
              {history_clause}
              AND (c.status = 'available' OR datetime(c.last_used_at, '+24 hours') < CURRENT_TIMESTAMP)
            ORDER BY
                CASE c.status WHEN 'available' THEN 0 ELSE 1 END ASC,
                CASE t.priority
                    WHEN 'urgent' THEN 1
                    WHEN 'high' THEN 2
                    WHEN 'normal' THEN 3
                    WHEN 'low' THEN 4
                    ELSE 5
                END ASC,
                t.is_boosted DESC,
                c.times_assigned ASC,
                RANDOM()
            LIMIT 1
            """
            
            params = [user_id, user_id]
            if not force_reuse:
                params.append(user_id)

            cursor = await db.execute(query, params)
            row = await cursor.fetchone()
            if not row:
                await db.commit()
                return None

            claim = dict(row)
            is_reused = 1 if claim["comment_status"] == "reusable" else 0
            
            await db.execute("""
            UPDATE comments
            SET assigned = 1, assigned_to = ?, assigned_at = CURRENT_TIMESTAMP,
                times_assigned = times_assigned + 1, last_used_at = CURRENT_TIMESTAMP,
                status = 'claimed', reused_count = reused_count + ?
            WHERE id = ?
            """, (user_id, is_reused, claim["comment_id"]))
            
            await db.execute("""
            INSERT INTO comment_assignment_history (user_id, task_id, comment_id, status)
            VALUES (?, ?, ?, 'CLAIMED')
            """, (user_id, claim["task_id"], claim["comment_id"]))

            await db.execute("""
            UPDATE users
            SET total_claims = total_claims + 1,
                last_claim_at = CURRENT_TIMESTAMP,
                last_active_at = CURRENT_TIMESTAMP
            WHERE telegram_id = ?
            """, (user_id,))
            
            await _mark_task_full_if_needed(db, claim["task_id"])
            await db.commit()

            claim["already_claimed"] = False
            return claim


async def save_submission(user_id, username, task_id, comment_id, comment_text,
                          reddit_link, normalized_reddit_link, reddit_comment_id):
    """
    Save proof submission and update member stats.

    The comment row intentionally stays assigned=1 so the slot is not
    released back to the pool.  get_active_claim filters it out via the
    submissions LEFT JOIN (submissions.id IS NULL guard).
    """
    async with _db_lock:
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("BEGIN IMMEDIATE")
            user = await _get_user_by_id(db, user_id)
            status = 'shadow_pending' if user and user.get('is_shadowbanned') else 'pending_review'

            try:
                cursor = await db.execute("""
                INSERT INTO submissions (
                    user_id, username, task_id, comment_id, comment_text,
                    reddit_link, normalized_reddit_link, reddit_comment_id, status
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    user_id, username, task_id, comment_id, comment_text,
                    reddit_link, normalized_reddit_link, reddit_comment_id, status,
                ))
                submission_id = cursor.lastrowid
            except aiosqlite.IntegrityError:
                await db.rollback()
                logging.warning(
                    "Duplicate submission blocked at DB level: user=%s task=%s comment_id=%s reddit_cid=%s",
                    user_id, task_id, comment_id, reddit_comment_id,
                )
                return None

            await db.execute("""
            UPDATE comments SET status = 'submitted' WHERE id = ?
            """, (comment_id,))

            await db.execute("""
            INSERT INTO comment_assignment_history (user_id, task_id, comment_id, status)
            VALUES (?, ?, ?, 'SUBMITTED')
            """, (user_id, task_id, comment_id))

            await db.execute("""
            UPDATE users
            SET total_submissions = total_submissions + 1,
                last_submit_at = CURRENT_TIMESTAMP,
                last_active_at = CURRENT_TIMESTAMP
            WHERE telegram_id = ?
            """, (user_id,))
            await db.commit()
            logging.info(
                "Submission committed: id=%s user=%s task=%s status=%s",
                submission_id, user_id, task_id, status,
            )
            return submission_id


async def submission_link_exists(normalized_reddit_link):
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("""
        SELECT id FROM submissions
        WHERE normalized_reddit_link = ? OR reddit_link = ?
        LIMIT 1
        """, (normalized_reddit_link, normalized_reddit_link))
        return await cursor.fetchone() is not None


async def reddit_comment_id_exists(reddit_comment_id):
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("""
        SELECT id FROM submissions
        WHERE reddit_comment_id = ?
        LIMIT 1
        """, (reddit_comment_id,))
        return await cursor.fetchone() is not None


async def submission_exists_for_comment(comment_id):
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT id FROM submissions WHERE comment_id = ? LIMIT 1", (comment_id,))
        return await cursor.fetchone() is not None


async def get_pending_submissions():
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
        SELECT submissions.*, tasks.payout_amount, tasks.subreddit
        FROM submissions
        JOIN tasks ON tasks.id = submissions.task_id
        WHERE submissions.status = 'pending_review'
          AND submissions.archived = 0
        ORDER BY submissions.submitted_at ASC
        LIMIT 200
        """)
        return [dict(row) for row in await cursor.fetchall()]


async def get_flagged_submissions():
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
        SELECT flags.id AS flag_id, flags.reason, submissions.*, tasks.payout_amount
        FROM flags
        LEFT JOIN submissions ON submissions.id = flags.submission_id
        LEFT JOIN tasks ON tasks.id = submissions.task_id
        WHERE flags.status = 'open'
          AND (submissions.archived IS NULL OR submissions.archived = 0)
        ORDER BY flags.created_at DESC
        LIMIT 50
        """)
        return [dict(row) for row in await cursor.fetchall()]


async def approve_submission(submission_id, admin_id):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("BEGIN IMMEDIATE")
        cursor = await db.execute("""
        SELECT submissions.*, tasks.payout_amount
        FROM submissions
        JOIN tasks ON tasks.id = submissions.task_id
        WHERE submissions.id = ?
        LIMIT 1
        """, (submission_id,))
        submission = await cursor.fetchone()
        if not submission or submission["status"] == "approved":
            return None

        submission = dict(submission)
        await db.execute("""
        UPDATE submissions
        SET status = 'approved', reviewed_at = CURRENT_TIMESTAMP, reviewed_by = ?
        WHERE id = ?
        """, (admin_id, submission_id))
        
        await db.execute("""
        UPDATE comments SET status = 'approved', times_approved = times_approved + 1
        WHERE id = ?
        """, (submission["comment_id"],))

        await db.execute("""
        INSERT INTO payments (user_id, username, task_id, submission_id, amount, status)
        VALUES (?, ?, ?, ?, ?, 'pending')
        """, (
            submission["user_id"], submission["username"], submission["task_id"],
            submission_id, submission["payout_amount"],
        ))
        await _update_member_level(db, submission["user_id"])
        await db.execute("UPDATE users SET reputation_score = reputation_score + 5 WHERE telegram_id = ?", (submission["user_id"],))
        await db.execute("""
        INSERT INTO audit_logs (admin_id, action, details)
        VALUES (?, 'approve_submission', ?)
        """, (admin_id, f"Approved submission {submission_id} for user {submission['user_id']}"))
        await db.commit()
        return submission


async def reject_submission(submission_id, admin_id, reason):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("BEGIN IMMEDIATE")
        cursor = await db.execute("SELECT * FROM submissions WHERE id = ? LIMIT 1", (submission_id,))
        submission = await cursor.fetchone()
        if not submission:
            return None

        await db.execute("""
        UPDATE submissions
        SET status = 'rejected',
            rejection_reason = ?,
            reviewed_at = CURRENT_TIMESTAMP,
            reviewed_by = ?
        WHERE id = ?
        """, (reason, admin_id, submission_id))
        
        await db.execute("""
        UPDATE comments 
        SET status = 'reusable', assigned = 0, assigned_to = NULL, assigned_at = NULL,
            times_rejected = times_rejected + 1
        WHERE id = ?
        """, (submission["comment_id"],))

        await _update_member_level(db, submission["user_id"])
        await db.execute("UPDATE users SET reputation_score = MAX(0, reputation_score - 10) WHERE telegram_id = ?", (submission["user_id"],))
        await db.execute("""
        INSERT INTO audit_logs (admin_id, action, details)
        VALUES (?, 'reject_submission', ?)
        """, (admin_id, f"Rejected submission {submission_id} for user {submission['user_id']}. Reason: {reason}"))
        await db.commit()
        return dict(submission)


async def flag_submission(submission_id, user_id, reason):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("BEGIN IMMEDIATE")
        await db.execute("""
        INSERT INTO flags (user_id, submission_id, reason)
        VALUES (?, ?, ?)
        """, (user_id, submission_id, reason))
        await db.execute("UPDATE submissions SET status = 'flagged' WHERE id = ?", (submission_id,))
        await db.commit()


async def close_task(task_id):
    return await update_task_status(task_id, "closed")


async def archive_task(task_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("BEGIN IMMEDIATE")
        await db.execute("UPDATE tasks SET status = 'archived', closed_at = CURRENT_TIMESTAMP WHERE id = ?", (task_id,))
        await db.commit()


async def update_task_status(task_id, status):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("BEGIN IMMEDIATE")
        closed_sql = ", closed_at = CURRENT_TIMESTAMP" if status == "closed" else ""
        cursor = await db.execute(f"""
        UPDATE tasks
        SET status = ? {closed_sql}
        WHERE id = ?
        """, (status, task_id))
        await db.commit()
        return cursor.rowcount > 0


async def get_task_eligibility(user_id, task_id):
    """Check if a user is eligible to claim a specific task.
    Returns (eligible, reason).
    """
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        user = await _get_user_by_id(db, user_id)
        if not user:
            return False, "User not found"
        if user["is_banned"]:
            return False, "You are restricted from claiming tasks."

        task = await _get_task_by_id(db, task_id)
        if not task or task["status"] != "active":
            return False, "This task is no longer active."
        
        if task["min_reputation"] > (user["reputation_score"] or 0):
            return False, f"Reputation too low (requires {task['min_reputation']})."

        # Check if already submitted
        cur = await db.execute(
            "SELECT id FROM submissions WHERE user_id = ? AND task_id = ? AND status != 'rejected'",
            (user_id, task_id)
        )
        if await cur.fetchone():
            return False, "You already completed this task."

        # Check if already claimed
        cur = await db.execute("""
            SELECT comments.id FROM comments
            JOIN tasks ON tasks.id = comments.task_id
            LEFT JOIN submissions ON submissions.comment_id = comments.id
            WHERE comments.assigned_to = ?
              AND comments.assigned = 1
              AND tasks.id = ?
              AND submissions.id IS NULL
        """, (user_id, task_id))
        if await cur.fetchone():
            return False, "You already have an active claim for this task."

        # Check simultaneous claim limit
        max_allowed = user.get("max_reddit_accounts") or 1
        cur = await db.execute(
            "SELECT COUNT(*) FROM reddit_accounts "
            "WHERE telegram_user_id = ? AND status = 'active'",
            (user_id,),
        )
        active_accounts = (await cur.fetchone())[0] or 0
        allowed_active = max(1, min(active_accounts, max_allowed))

        cur = await db.execute("""
            SELECT COUNT(*) FROM comments
            LEFT JOIN submissions ON submissions.comment_id = comments.id
            WHERE comments.assigned_to = ?
              AND comments.assigned = 1
              AND submissions.id IS NULL
        """, (user_id,))
        current_active = (await cur.fetchone())[0] or 0
        if current_active >= allowed_active:
            return False, f"Active claim limit reached ({current_active}/{allowed_active})."

        # Check if task is full
        cur = await db.execute("""
            SELECT COUNT(*) FROM comments
            WHERE task_id = ? AND status IN ('claimed', 'submitted', 'approved')
        """, (task_id,))
        claims = (await cur.fetchone())[0] or 0
        if claims >= task["total_slots"]:
            return False, "Task is full."

        # Check if any comments are available
        cur = await db.execute("""
            SELECT 1 FROM comments
            WHERE task_id = ? AND status IN ('available', 'reusable')
              AND (status = 'available' OR datetime(last_used_at, '+24 hours') < CURRENT_TIMESTAMP)
        """, (task_id,))
        if not await cur.fetchone():
            return False, "No unused comments remain for your account."

        return True, None


async def get_active_tasks():
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
        SELECT
            tasks.*,
            COUNT(DISTINCT comments.id) AS total_comments,
            COUNT(DISTINCT CASE WHEN comments.assigned = 1 THEN comments.id END) AS claims,
            COUNT(DISTINCT submissions.id) AS submissions
        FROM tasks
        LEFT JOIN comments ON comments.task_id = tasks.id
        LEFT JOIN submissions ON submissions.task_id = tasks.id
        WHERE tasks.status IN ('active', 'paused', 'under_review', 'full')
          AND tasks.archived = 0
        GROUP BY tasks.id
        ORDER BY tasks.id DESC
        LIMIT 50
        """)
        return [dict(row) for row in await cursor.fetchall()]


async def get_task_stats(task_id=None):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        query = """
        SELECT
            tasks.id, tasks.post_url, tasks.subreddit, tasks.payout_amount,
            tasks.total_slots, tasks.status, tasks.category,
            COUNT(DISTINCT comments.id) AS total_comments,
            COUNT(DISTINCT CASE WHEN comments.assigned = 1 THEN comments.id END) AS claims,
            COUNT(DISTINCT submissions.id) AS submissions,
            COUNT(DISTINCT CASE WHEN submissions.status = 'pending_review' THEN submissions.id END) AS pending_reviews,
            COUNT(DISTINCT CASE WHEN payments.status = 'pending' THEN payments.id END) AS pending_payments,
            COUNT(DISTINCT CASE WHEN payments.status = 'paid' THEN payments.id END) AS completed_payments
        FROM tasks
        LEFT JOIN comments ON comments.task_id = tasks.id
        LEFT JOIN submissions ON submissions.task_id = tasks.id
        LEFT JOIN payments ON payments.task_id = tasks.id
        """
        params = []
        if task_id is not None:
            query += " WHERE tasks.id = ?"
            params.append(task_id)
        query += " GROUP BY tasks.id ORDER BY tasks.id DESC"
        cursor = await db.execute(query, params)
        return [dict(row) for row in await cursor.fetchall()]


async def get_member_stats(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("""
        SELECT
            (SELECT COUNT(*) FROM submissions WHERE user_id = ? AND status = 'approved') AS approved,
            (SELECT COUNT(*) FROM submissions WHERE user_id = ? AND status = 'pending_review') AS pending,
            (SELECT COUNT(*) FROM submissions WHERE user_id = ? AND status = 'rejected') AS rejected,
            (SELECT COUNT(*) FROM payments WHERE user_id = ? AND status = 'paid') AS paid_count,
            (SELECT level FROM worker_levels WHERE user_id = ?) AS level,
            warnings, notes, is_banned, reputation_score, is_shadowbanned, streak_count, badges,
            username
        FROM users
        WHERE telegram_id = ?
        """, (user_id, user_id, user_id, user_id, user_id, user_id))
        row = await cursor.fetchone()
        if not row:
            return None
        approved, pending, rejected, paid_count, level, warnings, notes, is_banned, reputation, is_shadowbanned, streak, badges, username = row
        total_reviewed = approved + rejected
        approval_rate = 100 if total_reviewed == 0 else round((approved / total_reviewed) * 100)
        return {
            "approved": approved,
            "pending": pending,
            "rejected": rejected,
            "paid_count": paid_count,
            "approval_rate": approval_rate,
            "level": level or "Beginner",
            "warnings": warnings,
            "notes": notes,
            "is_banned": is_banned,
            "reputation": reputation,
            "is_shadowbanned": is_shadowbanned,
            "streak": streak,
            "badges": badges,
            "username": username,
        }


async def get_submissions_today_count(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT COUNT(*) FROM submissions WHERE user_id = ? AND date(submitted_at) = date('now')",
            (user_id,),
        )
        row = await cursor.fetchone()
        return row[0] if row else 0


async def get_submission_history(user_id, limit=10):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
        SELECT submissions.*, tasks.payout_amount
        FROM submissions
        JOIN tasks ON tasks.id = submissions.task_id
        WHERE submissions.user_id = ?
        ORDER BY submissions.submitted_at DESC
        LIMIT ?
        """, (user_id, limit))
        return [dict(row) for row in await cursor.fetchall()]

async def get_total_stats():
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("""
        SELECT
            (SELECT COUNT(*) FROM tasks WHERE status = 'active'),
            (SELECT COUNT(*) FROM comments WHERE assigned = 1),
            (SELECT COUNT(*) FROM submissions),
            (SELECT COUNT(*) FROM submissions WHERE status = 'pending_review'),
            (SELECT COUNT(*) FROM payments WHERE status = 'pending'),
            (SELECT COUNT(*) FROM payments WHERE status = 'paid')
        """)
        row = await cursor.fetchone()
        return {
            "active_tasks": row[0],
            "claims": row[1],
            "submissions": row[2],
            "pending_reviews": row[3],
            "pending_payments": row[4],
            "completed_payments": row[5],
        }


async def get_payment_history(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
        SELECT amount, status, task_id, created_at, paid_at
        FROM payments
        WHERE user_id = ?
        UNION ALL
        SELECT tasks.payout_amount AS amount, 'rejected' AS status, submissions.task_id, submissions.reviewed_at AS created_at, submissions.reviewed_at AS paid_at
        FROM submissions
        JOIN tasks ON tasks.id = submissions.task_id
        WHERE submissions.user_id = ? AND submissions.status = 'rejected'
        ORDER BY created_at DESC
        """, (user_id, user_id))
        return [dict(row) for row in await cursor.fetchall()]


async def get_pending_payments():
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
        SELECT payments.*, users.upi_id, users.qr_file_id, users.qr_uploaded_at
        FROM payments
        JOIN users ON users.telegram_id = payments.user_id
        WHERE payments.status = 'pending'
          AND payments.archived = 0
        ORDER BY payments.created_at ASC
        LIMIT 200
        """)
        return [dict(row) for row in await cursor.fetchall()]


async def mark_payment_paid(payment_id):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("BEGIN IMMEDIATE")
        cursor = await db.execute("SELECT * FROM payments WHERE id = ? LIMIT 1", (payment_id,))
        payment = await cursor.fetchone()
        if not payment or payment["status"] == "paid":
            return None
        await db.execute("""
        UPDATE payments SET status = 'paid', paid_at = CURRENT_TIMESTAMP
        WHERE id = ?
        """, (payment_id,))
        await db.commit()
        return dict(payment)


async def cancel_payment(payment_id):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("BEGIN IMMEDIATE")
        cursor = await db.execute("SELECT * FROM payments WHERE id = ? LIMIT 1", (payment_id,))
        payment = await cursor.fetchone()
        if not payment or payment["status"] == "paid":
            return None
        await db.execute("UPDATE payments SET status = 'failed' WHERE id = ?", (payment_id,))
        await db.commit()
        return dict(payment)


async def get_all_member_ids(active_only=False):
    async with aiosqlite.connect(DB_NAME) as db:
        if active_only:
            cursor = await db.execute("""
            SELECT DISTINCT user_id FROM submissions
            UNION
            SELECT DISTINCT assigned_to FROM comments WHERE assigned_to IS NOT NULL
            """)
        else:
            cursor = await db.execute("SELECT telegram_id FROM users")
        return [row[0] for row in await cursor.fetchall() if row[0]]


async def get_setting(key, default=None):
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = await cursor.fetchone()
        return row[0] if row else default


async def set_setting(key, value):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("BEGIN IMMEDIATE")
        await db.execute("INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)", (key, str(value)))
        await db.commit()


async def get_system_stats():
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
        SELECT
            (SELECT COUNT(*) FROM users) AS total_members,
            (SELECT COUNT(*) FROM tasks WHERE archived = 0) AS total_tasks,
            (SELECT COUNT(*) FROM submissions WHERE archived = 0) AS total_submissions,
            (SELECT COUNT(*) FROM tasks WHERE status = 'active' AND archived = 0) AS active_tasks,
            (SELECT COUNT(*) FROM submissions WHERE status = 'pending_review' AND archived = 0) AS pending_reviews,
            (SELECT COUNT(*) FROM payments WHERE status = 'pending' AND archived = 0) AS pending_payments,
            (SELECT COUNT(*) FROM payments WHERE status = 'paid') AS completed_payments,
            (SELECT COUNT(*) FILTER (WHERE comment_alive = 1) FROM submissions WHERE status = 'approved' AND archived = 0) AS live_count,
            (SELECT COUNT(*) FILTER (WHERE comment_alive = 0) FROM submissions WHERE status = 'approved' AND archived = 0) AS removed_count
        """)
        row = dict(await cursor.fetchone())

        async def sum_amount(status):
            p_cursor = await db.execute("SELECT amount FROM payments WHERE status = ? AND archived = 0", (status,))
            total = 0.0
            for p_row in await p_cursor.fetchall():
                match = re.search(r"([0-9]+(?:\.[0-9]+)?)", p_row[0])
                if match: total += float(match.group(1))
            return total

        row["total_payouts"] = await sum_amount('paid')
        row["pending_payouts_sum"] = await sum_amount('pending')
        return row

async def shadowban_user(user_id, status=1):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("BEGIN IMMEDIATE")
        await db.execute("UPDATE users SET is_shadowbanned = ? WHERE telegram_id = ?", (status, user_id))
        await db.commit()


async def update_reputation(user_id, score_change):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("BEGIN IMMEDIATE")
        await db.execute("UPDATE users SET reputation_score = reputation_score + ? WHERE telegram_id = ?", (score_change, user_id))
        await db.commit()


async def get_user_by_username(username):
    username = username.lstrip("@").lower()
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT telegram_id FROM users WHERE LOWER(username) = ?", (username,))
        row = await cursor.fetchone()
        return row[0] if row else None


async def post_exists(post_id):
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT id FROM tasks WHERE post_id = ? LIMIT 1", (post_id,))
        return await cursor.fetchone() is not None


async def get_tasks_by_post_id(post_id):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
        SELECT id, status, total_slots, payout_amount, created_at
        FROM tasks
        WHERE post_id = ?
          AND archived = 0
        ORDER BY id DESC
        LIMIT 10
        """, (post_id,))
        return [dict(row) for row in await cursor.fetchall()]


# ============================================================
# Reddit account management
# ============================================================

REDDIT_USERNAME_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{2,19}$")


def normalize_reddit_username(raw):
    """Strip u/, /u/, leading @, lower-case, return None if invalid."""
    if not raw:
        return None
    name = raw.strip().lstrip("@")
    for prefix in ("/u/", "u/", "/user/", "user/"):
        if name.lower().startswith(prefix):
            name = name[len(prefix):]
            break
    name = name.strip("/").lower()
    if not REDDIT_USERNAME_RE.match(name):
        return None
    return name


async def _count_owned_accounts(db, telegram_user_id):
    cursor = await db.execute(
        "SELECT COUNT(*) FROM reddit_accounts "
        "WHERE telegram_user_id = ? AND status IN ('pending', 'active')",
        (telegram_user_id,),
    )
    row = await cursor.fetchone()
    return row[0] if row else 0


async def add_reddit_account(telegram_user_id, raw_username):
    """Register a Reddit account for a Telegram user.

    Returns a tuple (code, payload):
      ('invalid_format', None)
      ('limit_reached', {'max': N, 'current': M})
      ('taken_by_other', {'other_user_id': X, 'reddit_username': name, 'status': S})
      ('duplicate_self', {'reddit_username': name, 'status': S})
      ('pending',       {'account_id': id, 'reddit_username': name})
    """
    name = normalize_reddit_username(raw_username)
    if not name:
        return ("invalid_format", None)

    async with _db_lock:
        async with aiosqlite.connect(DB_NAME) as db:
            db.row_factory = aiosqlite.Row
            await db.execute("BEGIN IMMEDIATE")

            cursor = await db.execute(
                "SELECT * FROM reddit_accounts WHERE reddit_username = ? COLLATE NOCASE LIMIT 1",
                (name,),
            )
            existing = await cursor.fetchone()
            if existing:
                existing = dict(existing)
                await db.commit()
                if existing["telegram_user_id"] == telegram_user_id:
                    return ("duplicate_self", {
                        "reddit_username": name,
                        "status": existing["status"],
                    })
                return ("taken_by_other", {
                    "other_user_id": existing["telegram_user_id"],
                    "reddit_username": name,
                    "status": existing["status"],
                })

            user_cursor = await db.execute(
                "SELECT COALESCE(max_reddit_accounts, 1) FROM users WHERE telegram_id = ?",
                (telegram_user_id,),
            )
            row = await user_cursor.fetchone()
            max_allowed = row[0] if row else 1

            current = await _count_owned_accounts(db, telegram_user_id)
            if current >= max_allowed:
                await db.commit()
                return ("limit_reached", {"max": max_allowed, "current": current})

            insert_cursor = await db.execute(
                "INSERT INTO reddit_accounts (telegram_user_id, reddit_username, status) "
                "VALUES (?, ?, 'pending')",
                (telegram_user_id, name),
            )
            account_id = insert_cursor.lastrowid
            await db.commit()
            return ("pending", {"account_id": account_id, "reddit_username": name})


async def remove_reddit_account(telegram_user_id, raw_username):
    """Hard-delete a Reddit account row owned by the given Telegram user.

    Frees the username slot so it can be re-added later. Returns True if removed.
    Submissions are unaffected — historical reddit_author text remains on them.
    """
    name = normalize_reddit_username(raw_username)
    if not name:
        return False
    async with _db_lock:
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("BEGIN IMMEDIATE")
            cursor = await db.execute(
                "DELETE FROM reddit_accounts "
                "WHERE telegram_user_id = ? AND reddit_username = ? COLLATE NOCASE",
                (telegram_user_id, name),
            )
            await db.commit()
            return cursor.rowcount > 0


async def list_reddit_accounts(telegram_user_id, include_disabled=True):
    """Return all Reddit accounts owned by a Telegram user, newest first."""
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        if include_disabled:
            query = ("SELECT * FROM reddit_accounts WHERE telegram_user_id = ? "
                     "ORDER BY added_at DESC")
            params = (telegram_user_id,)
        else:
            query = ("SELECT * FROM reddit_accounts WHERE telegram_user_id = ? "
                     "AND status != 'disabled' ORDER BY added_at DESC")
            params = (telegram_user_id,)
        cursor = await db.execute(query, params)
        return [dict(r) for r in await cursor.fetchall()]


async def get_reddit_account_by_username(raw_username):
    """Lookup ANY Reddit account row by username, regardless of owner. Returns row or None."""
    name = normalize_reddit_username(raw_username)
    if not name:
        return None
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM reddit_accounts WHERE reddit_username = ? COLLATE NOCASE LIMIT 1",
            (name,),
        )
        row = await cursor.fetchone()
        return dict(row) if row else None


async def get_reddit_account_by_id(account_id):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM reddit_accounts WHERE id = ? LIMIT 1",
            (account_id,),
        )
        row = await cursor.fetchone()
        return dict(row) if row else None


async def get_pending_reddit_accounts(limit=50):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT r.*, u.username AS tg_username "
            "FROM reddit_accounts r "
            "LEFT JOIN users u ON u.telegram_id = r.telegram_user_id "
            "WHERE r.status = 'pending' "
            "ORDER BY r.added_at ASC LIMIT ?",
            (limit,),
        )
        return [dict(r) for r in await cursor.fetchall()]


async def approve_reddit_account(account_id, admin_id):
    async with _db_lock:
        async with aiosqlite.connect(DB_NAME) as db:
            db.row_factory = aiosqlite.Row
            await db.execute("BEGIN IMMEDIATE")
            cursor = await db.execute(
                "SELECT * FROM reddit_accounts WHERE id = ? LIMIT 1", (account_id,),
            )
            row = await cursor.fetchone()
            if not row:
                await db.commit()
                return None
            await db.execute(
                "UPDATE reddit_accounts SET status = 'active', approved_at = CURRENT_TIMESTAMP, "
                "approved_by = ? WHERE id = ?",
                (admin_id, account_id),
            )
            await db.commit()
            return dict(row)


async def reject_reddit_account(account_id, admin_id, reason):
    async with _db_lock:
        async with aiosqlite.connect(DB_NAME) as db:
            db.row_factory = aiosqlite.Row
            await db.execute("BEGIN IMMEDIATE")
            cursor = await db.execute(
                "SELECT * FROM reddit_accounts WHERE id = ? LIMIT 1", (account_id,),
            )
            row = await cursor.fetchone()
            if not row:
                await db.commit()
                return None
            # Hard-delete on reject so the username slot frees up for an honest re-application.
            await db.execute("DELETE FROM reddit_accounts WHERE id = ?", (account_id,))
            await db.commit()
            result = dict(row)
            result["_admin_id"] = admin_id
            result["_reason"] = reason
            return result


async def disable_reddit_account(account_id, admin_id, reason=None):
    """Admin action: mark a Reddit account as disabled. Slot remains occupied."""
    async with _db_lock:
        async with aiosqlite.connect(DB_NAME) as db:
            db.row_factory = aiosqlite.Row
            await db.execute("BEGIN IMMEDIATE")
            cursor = await db.execute(
                "SELECT * FROM reddit_accounts WHERE id = ? LIMIT 1", (account_id,),
            )
            row = await cursor.fetchone()
            if not row:
                await db.commit()
                return None
            await db.execute(
                "UPDATE reddit_accounts SET status = 'disabled', "
                "notes = COALESCE(notes || char(10), '') || ? WHERE id = ?",
                (f"[disabled by admin {admin_id}] {reason or ''}".strip(), account_id),
            )
            await db.commit()
            return dict(row)


async def set_max_reddit_accounts(telegram_user_id, value):
    if value < 1:
        value = 1
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("BEGIN IMMEDIATE")
        await db.execute(
            "UPDATE users SET max_reddit_accounts = ? WHERE telegram_id = ?",
            (value, telegram_user_id),
        )
        await db.commit()


async def get_active_reddit_accounts(telegram_user_id):
    """Return only approved/active accounts for a Telegram user."""
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM reddit_accounts "
            "WHERE telegram_user_id = ? AND status = 'active' "
            "ORDER BY added_at ASC",
            (telegram_user_id,),
        )
        return [dict(r) for r in await cursor.fetchall()]


async def touch_reddit_account_last_used(account_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("BEGIN IMMEDIATE")
        await db.execute(
            "UPDATE reddit_accounts SET last_used_at = CURRENT_TIMESTAMP WHERE id = ?",
            (account_id,),
        )
        await db.commit()


async def get_reddit_account_health(account_id):
    """Return per-account performance metrics for the admin dashboard."""
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        acc_cur = await db.execute(
            "SELECT * FROM reddit_accounts WHERE id = ? LIMIT 1", (account_id,),
        )
        account = await acc_cur.fetchone()
        if not account:
            return None
        account = dict(account)
        stats_cur = await db.execute("""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN status = 'approved' THEN 1 ELSE 0 END) AS approved,
                SUM(CASE WHEN status = 'rejected' THEN 1 ELSE 0 END) AS rejected,
                SUM(CASE WHEN status = 'pending_review' THEN 1 ELSE 0 END) AS pending,
                SUM(CASE WHEN live_status = 'live' THEN 1 ELSE 0 END) AS live,
                SUM(CASE WHEN live_status IN ('removed', 'deleted', 'shadow_removed') THEN 1 ELSE 0 END) AS dead
            FROM submissions
            WHERE reddit_account_id = ?
               OR (reddit_account_id IS NULL AND reddit_author = ?)
        """, (account_id, account["reddit_username"]))
        s = await stats_cur.fetchone()
        s = dict(s) if s else {}
        total = s.get("total") or 0
        approved = s.get("approved") or 0
        rejected = s.get("rejected") or 0
        live = s.get("live") or 0
        dead = s.get("dead") or 0
        reviewed = approved + rejected
        approval_rate = 100 if reviewed == 0 else round(approved / reviewed * 100)
        live_rate = 100 if (live + dead) == 0 else round(live / (live + dead) * 100)
        return {
            "account": account,
            "total_submissions": total,
            "approved": approved,
            "rejected": rejected,
            "pending": s.get("pending") or 0,
            "live": live,
            "dead": dead,
            "approval_rate": approval_rate,
            "live_rate": live_rate,
            "warnings": account.get("warnings") or 0,
        }


async def warn_reddit_account(account_id, note=None):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("BEGIN IMMEDIATE")
        if note:
            await db.execute(
                "UPDATE reddit_accounts SET warnings = warnings + 1, "
                "notes = COALESCE(notes || char(10), '') || ? WHERE id = ?",
                (note, account_id),
            )
        else:
            await db.execute(
                "UPDATE reddit_accounts SET warnings = warnings + 1 WHERE id = ?",
                (account_id,),
            )
        await db.commit()


async def get_allowed_active_claim_count(telegram_user_id):
    """How many simultaneous active claims is this Telegram user permitted?

    Rule: min(active_reddit_accounts, max_reddit_accounts), floored at 1 so
    legacy members without a registered account can still claim once.
    """
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(
            "SELECT COALESCE(max_reddit_accounts, 1) FROM users WHERE telegram_id = ?",
            (telegram_user_id,),
        )
        row = await cursor.fetchone()
        max_allowed = row[0] if row else 1
        cursor = await db.execute(
            "SELECT COUNT(*) FROM reddit_accounts "
            "WHERE telegram_user_id = ? AND status = 'active'",
            (telegram_user_id,),
        )
        row = await cursor.fetchone()
        active_count = row[0] if row else 0
        return max(1, min(active_count, max_allowed))


async def get_active_claims(telegram_user_id):
    """Return ALL active (claimed-but-not-yet-submitted) claims for a Telegram user."""
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
        SELECT
            comments.id AS comment_id,
            comments.text AS comment_text,
            comments.assigned_at,
            tasks.id AS task_id,
            tasks.post_url,
            tasks.subreddit,
            tasks.post_id,
            tasks.post_path,
            tasks.payout_amount,
            tasks.category,
            tasks.instructions
        FROM comments
        JOIN tasks ON tasks.id = comments.task_id
        LEFT JOIN submissions ON submissions.comment_id = comments.id
        WHERE comments.assigned_to = ?
          AND comments.assigned = 1
          AND submissions.id IS NULL
        ORDER BY comments.assigned_at DESC
        """, (telegram_user_id,))
        return [dict(r) for r in await cursor.fetchall()]


# ============================================================


async def _get_user_by_id(db, user_id):
    db.row_factory = aiosqlite.Row
    cursor = await db.execute("SELECT * FROM users WHERE telegram_id = ? LIMIT 1", (user_id,))
    row = await cursor.fetchone()
    return dict(row) if row else None


async def _get_task_by_id(db, task_id):
    db.row_factory = aiosqlite.Row
    cursor = await db.execute("SELECT * FROM tasks WHERE id = ? LIMIT 1", (task_id,))
    row = await cursor.fetchone()
    return dict(row) if row else None


async def _mark_task_full_if_needed(db, task_id):
    cursor = await db.execute("""
    SELECT
        tasks.total_slots,
        COUNT(comments.id) AS total_comments,
        SUM(CASE WHEN comments.status IN ('claimed', 'submitted', 'approved') THEN 1 ELSE 0 END) AS assigned_comments
    FROM tasks
    LEFT JOIN comments ON comments.task_id = tasks.id
    WHERE tasks.id = ?
    GROUP BY tasks.id
    """, (task_id,))
    row = await cursor.fetchone()
    if not row:
        return

    total_slots = row[0]
    total_comments = row[1] or 0
    assigned_comments = row[2] or 0
    if total_comments > 0 and (
        assigned_comments >= total_comments or assigned_comments >= total_slots
    ):
        await db.execute("UPDATE tasks SET status = 'full' WHERE id = ? AND status = 'active'", (task_id,))


async def _update_member_level(db, user_id):
    cursor = await db.execute("""
    SELECT
        SUM(CASE WHEN status = 'approved' THEN 1 ELSE 0 END) AS approved,
        SUM(CASE WHEN status = 'rejected' THEN 1 ELSE 0 END) AS rejected
    FROM submissions
    WHERE user_id = ?
    """, (user_id,))
    row = await cursor.fetchone()
    approved = row[0] or 0
    rejected = row[1] or 0
    total = approved + rejected
    approval_rate = 100 if total == 0 else (approved / total) * 100

    level = "Beginner"
    if approved >= 50 and approval_rate >= 95:
        level = "Elite"
    elif approved >= 10 and approval_rate >= 90:
        level = "Trusted"

    await db.execute("""
    INSERT INTO worker_levels (user_id, level, updated_at)
    VALUES (?, ?, CURRENT_TIMESTAMP)
    ON CONFLICT(user_id) DO UPDATE SET
        level = excluded.level,
        updated_at = CURRENT_TIMESTAMP
    """, (user_id, level))


async def auto_cleanup_claims():
    """Release claims older than claim_timeout_minutes (default 30) with no submission.

    Also reopens any 'full' tasks that gain free slots after the release.
    Returns list of {assigned_to, task_id} dicts for notification.
    """
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("BEGIN IMMEDIATE")

        # Read timeout from settings; fall back to 30 minutes
        cursor = await db.execute("SELECT value FROM settings WHERE key = 'claim_timeout_minutes'")
        row = await cursor.fetchone()
        timeout_minutes = int(row[0]) if row and row[0] and str(row[0]).isdigit() else 30

        cursor = await db.execute(f"""
        SELECT assigned_to, task_id
        FROM comments
        WHERE assigned = 1
          AND assigned_at < datetime('now', '-{timeout_minutes} minutes')
          AND id NOT IN (SELECT comment_id FROM submissions WHERE comment_id IS NOT NULL)
        """)
        expired_claims = [dict(row) for row in await cursor.fetchall()]

        if expired_claims:
            await db.execute(f"""
            UPDATE comments
            SET assigned = 0, assigned_to = NULL, assigned_at = NULL, status = 'reusable'
            WHERE assigned = 1
              AND assigned_at < datetime('now', '-{timeout_minutes} minutes')
              AND id NOT IN (SELECT comment_id FROM submissions WHERE comment_id IS NOT NULL)
            """)
            # Reopen any 'full' tasks that now have unassigned comments available
            await db.execute("""
            UPDATE tasks SET status = 'active'
            WHERE status = 'full'
              AND id IN (
                  SELECT DISTINCT task_id FROM comments
                  WHERE status IN ('available', 'reusable')
              )
            """)

        await db.commit()
        return expired_claims, timeout_minutes


# ─── Archive / cleanup helpers ──────────────────────────────────────────────

async def count_archivable_records(min_age_days=7):
    """Return how many completed operational records are eligible for archiving.

    Eligibility:
      tasks       — status IN ('closed','full','archived') AND archived=0
      payments    — status IN ('paid','failed') AND archived=0 AND paid older than N days
      submissions — status IN ('approved','rejected','flagged') AND archived=0 AND reviewed older than N days
    """
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute(f"""
        SELECT
            (SELECT COUNT(*) FROM tasks
                WHERE archived = 0
                  AND status IN ('closed','full','archived')),
            (SELECT COUNT(*) FROM payments
                WHERE archived = 0
                  AND status IN ('paid','failed')
                  AND (paid_at IS NULL OR paid_at < datetime('now', '-{min_age_days} days'))),
            (SELECT COUNT(*) FROM submissions
                WHERE archived = 0
                  AND status IN ('approved','rejected','flagged')
                  AND (reviewed_at IS NULL OR reviewed_at < datetime('now', '-{min_age_days} days')))
        """)
        row = await cursor.fetchone()
        return {
            "tasks": row[0] or 0,
            "payments": row[1] or 0,
            "submissions": row[2] or 0,
        }


async def archive_completed_records(min_age_days=7):
    """Flag completed operational records as archived=1. Returns counts archived.

    Members, admins, QR data, UPI data, settings, warnings, bans and audit logs
    are never touched. Counters on the users table (total_claims, total_submissions)
    are preserved. Archived rows remain in the DB and still feed historical totals.
    """
    async with _db_lock:
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("BEGIN IMMEDIATE")

            cursor = await db.execute(f"""
            UPDATE tasks SET archived = 1
            WHERE archived = 0
              AND status IN ('closed','full','archived')
            """)
            tasks_n = cursor.rowcount

            cursor = await db.execute(f"""
            UPDATE payments SET archived = 1
            WHERE archived = 0
              AND status IN ('paid','failed')
              AND (paid_at IS NULL OR paid_at < datetime('now', '-{min_age_days} days'))
            """)
            payments_n = cursor.rowcount

            cursor = await db.execute(f"""
            UPDATE submissions SET archived = 1
            WHERE archived = 0
              AND status IN ('approved','rejected','flagged')
              AND (reviewed_at IS NULL OR reviewed_at < datetime('now', '-{min_age_days} days'))
            """)
            subs_n = cursor.rowcount

            await db.commit()
            return {
                "tasks": tasks_n,
                "payments": payments_n,
                "submissions": subs_n,
            }


# ─── Payment session helpers ────────────────────────────────────────────────

async def get_pending_payments_grouped():
    """Pending payments grouped per user with full live-status breakdown.

    Each bundle contains, on top of the raw payments list:
      - approved_count       : total approved + pending-payment items in this bundle
      - live_count           : submissions currently live
      - removed_count        : submissions reddit-removed
      - deleted_count        : submissions deleted by author
      - shadow_removed_count : submissions vanished from reddit
      - unchecked_count      : never checked yet
      - awaiting_24h_count   : live but submitted < 24h ago (not payable yet)
      - payable_count        : is_payable=1 (live + 24h survived)
      - oldest_pending_age_h : hours since the oldest payment was created
      - final_payable_amount : sum of payment.amount over payable items
      - total_pending_amount : sum of payment.amount across all rows (raw)
    """
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
        SELECT payments.id AS payment_id, payments.task_id, payments.amount,
               payments.submission_id, payments.user_id, payments.username,
               payments.created_at AS payment_created_at,
               users.upi_id, users.qr_file_id,
               submissions.comment_alive, submissions.reddit_link,
               submissions.live_status, submissions.is_payable,
               submissions.last_live_check, submissions.first_seen_live_at,
               submissions.submitted_at,
               tasks.payout_amount AS task_payout_amount
        FROM payments
        JOIN users ON users.telegram_id = payments.user_id
        LEFT JOIN submissions ON submissions.id = payments.submission_id
        LEFT JOIN tasks ON tasks.id = payments.task_id
        WHERE payments.status = 'pending'
          AND payments.archived = 0
        ORDER BY payments.user_id ASC, payments.created_at ASC
        """)
        rows = [dict(r) for r in await cursor.fetchall()]

    grouped = {}
    for r in rows:
        uid = r["user_id"]
        if uid not in grouped:
            grouped[uid] = {
                "user_id": uid,
                "username": r["username"],
                "upi_id": r["upi_id"],
                "qr_file_id": r["qr_file_id"],
                "payments": [],
                "approved_count": 0,
                "live_count": 0,
                "removed_count": 0,
                "deleted_count": 0,
                "shadow_removed_count": 0,
                "unchecked_count": 0,
                "error_count": 0,
                "awaiting_24h_count": 0,
                "payable_count": 0,
                "oldest_payment_created_at": r["payment_created_at"],
            }
        bundle = grouped[uid]
        bundle["approved_count"] += 1

        ls = r["live_status"] or "unchecked"
        if ls == "live":
            bundle["live_count"] += 1
        elif ls == "removed":
            bundle["removed_count"] += 1
        elif ls == "deleted":
            bundle["deleted_count"] += 1
        elif ls == "shadow_removed":
            bundle["shadow_removed_count"] += 1
        elif ls == "error":
            bundle["error_count"] += 1
        else:
            bundle["unchecked_count"] += 1

        if r["is_payable"] == 1:
            bundle["payable_count"] += 1
        elif ls == "live":
            # live but not yet payable means still within 24h
            bundle["awaiting_24h_count"] += 1

        bundle["payments"].append({
            "payment_id": r["payment_id"],
            "task_id": r["task_id"],
            "amount": r["amount"],
            "task_payout_amount": r["task_payout_amount"],
            "submission_id": r["submission_id"],
            "reddit_link": r["reddit_link"],
            "comment_alive": r["comment_alive"],
            "live_status": ls,
            "is_payable": r["is_payable"] or 0,
            "last_live_check": r["last_live_check"],
            "submitted_at": r["submitted_at"],
        })

    for bundle in grouped.values():
        payable_rows = [p for p in bundle["payments"] if p["is_payable"] == 1]
        bundle["final_payable_amount"] = _sum_payment_amounts(payable_rows) if payable_rows else "0"
        bundle["total_pending_amount"] = _sum_payment_amounts(bundle["payments"])
        # oldest pending age in hours (simple iso parse — created_at is sqlite UTC)
        try:
            from datetime import datetime, timezone
            oldest_dt = datetime.fromisoformat(bundle["oldest_payment_created_at"].replace(" ", "T"))
            if oldest_dt.tzinfo is None:
                oldest_dt = oldest_dt.replace(tzinfo=timezone.utc)
            bundle["oldest_pending_age_h"] = round(
                (datetime.now(timezone.utc) - oldest_dt).total_seconds() / 3600.0,
                1,
            )
        except Exception:
            bundle["oldest_pending_age_h"] = None
    return list(grouped.values())


async def mark_user_payments_paid(user_id):
    """Mark only PAYABLE pending payments for one user as paid.

    Payable = underlying submission.is_payable = 1 (status=approved + live + 24h survived + not archived).
    Returns (paid_count, paid_rows, blocked_count, waiting_count) where:
      - paid_rows is the list of payment dicts that just got marked paid (with amounts)
      - blocked_count is dead comments (removed/deleted/shadow_removed) — those get archived as 'failed'
      - waiting_count is live-but-under-24h or unchecked — those stay 'pending'
    """
    async with _db_lock:
        async with aiosqlite.connect(DB_NAME) as db:
            db.row_factory = aiosqlite.Row
            await db.execute("BEGIN IMMEDIATE")

            cursor = await db.execute("""
            SELECT p.id, p.amount, p.submission_id, p.task_id,
                   s.live_status, s.is_payable
            FROM payments p
            LEFT JOIN submissions s ON s.id = p.submission_id
            WHERE p.user_id = ?
              AND p.status = 'pending'
              AND p.archived = 0
            """, (user_id,))
            rows = [dict(r) for r in await cursor.fetchall()]

            payable = [r for r in rows if r["is_payable"] == 1]
            blocked = [r for r in rows if (r["live_status"] or "") in ("removed", "deleted", "shadow_removed")]
            waiting = [r for r in rows
                       if r["is_payable"] != 1
                       and (r["live_status"] or "") not in ("removed", "deleted", "shadow_removed")]

            if blocked:
                blocked_ids = [r["id"] for r in blocked]
                ph = ",".join("?" * len(blocked_ids))
                await db.execute(
                    f"UPDATE payments SET status = 'failed', archived = 1 WHERE id IN ({ph})",
                    blocked_ids,
                )

            if not payable:
                await db.commit()
                return 0, [], len(blocked), len(waiting)

            payable_ids = [r["id"] for r in payable]
            ph2 = ",".join("?" * len(payable_ids))
            await db.execute(
                f"UPDATE payments SET status = 'paid', paid_at = CURRENT_TIMESTAMP WHERE id IN ({ph2})",
                payable_ids,
            )
            await db.commit()
            return len(payable), payable, len(blocked), len(waiting)


# ─── Comment-live checker helpers ───────────────────────────────────────────

LIVE_STATUSES_ALIVE = {"live"}
LIVE_STATUSES_DEAD = {"removed", "deleted", "shadow_removed"}
LIVE_STATUSES_ALL = LIVE_STATUSES_ALIVE | LIVE_STATUSES_DEAD | {"unchecked", "error"}


async def get_submissions_to_check(max_age_days=7, recheck_after_hours=12, limit=50):
    """Submissions needing a Reddit liveness check.

    Picks submissions submitted within the last N days that either:
      - have never been checked (last_live_check IS NULL), OR
      - were last checked more than recheck_after_hours ago, OR
      - had an error on the previous check.
    Stops re-checking once a comment is confirmed dead.
    """
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(f"""
        SELECT s.id, s.reddit_comment_id, s.user_id, s.task_id,
               s.submitted_at, s.live_status, s.last_live_check,
               s.first_seen_live_at, t.subreddit, t.post_id
        FROM submissions s
        JOIN tasks t ON t.id = s.task_id
        WHERE s.reddit_comment_id IS NOT NULL
          AND s.archived = 0
          AND s.live_status NOT IN ('removed', 'deleted', 'shadow_removed')
          AND s.submitted_at > datetime('now', '-{max_age_days} days')
          AND (
              s.last_live_check IS NULL
              OR s.last_live_check < datetime('now', '-{recheck_after_hours} hours')
              OR s.live_status = 'error'
          )
        ORDER BY s.submitted_at DESC
        LIMIT {int(limit)}
        """)
        return [dict(r) for r in await cursor.fetchall()]


async def update_submission_comment_status(submission_id, live_status):
    """Record one liveness check. live_status: 'live'|'removed'|'deleted'|'shadow_removed'|'error'.

    Side effects:
      - bumps last_live_check + comment_check_count always
      - on 'live': sets first_seen_live_at (first time only), recomputes live_duration_hours,
        and sets is_payable=1 iff (status=approved AND submitted_at < now-24h AND not archived)
      - on dead statuses: sets is_payable=0, comment_alive=0, flags the submission
      - on 'error': leaves live_status unchanged, just updates timestamps
      - keeps legacy comment_alive in sync so older queries still work
    """
    if live_status not in LIVE_STATUSES_ALL:
        raise ValueError(f"invalid live_status: {live_status!r}")

    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("BEGIN IMMEDIATE")
        if live_status == "error":
            await db.execute("""
            UPDATE submissions
            SET last_live_check = CURRENT_TIMESTAMP,
                comment_last_checked_at = CURRENT_TIMESTAMP,
                comment_check_count = COALESCE(comment_check_count, 0) + 1,
                live_status = CASE
                    WHEN live_status IN ('live','removed','deleted','shadow_removed') THEN live_status
                    ELSE 'error'
                END
            WHERE id = ?
            """, (submission_id,))
        elif live_status == "live":
            await db.execute("""
            UPDATE submissions
            SET live_status = 'live',
                comment_alive = 1,
                last_live_check = CURRENT_TIMESTAMP,
                comment_last_checked_at = CURRENT_TIMESTAMP,
                comment_check_count = COALESCE(comment_check_count, 0) + 1,
                first_seen_live_at = COALESCE(first_seen_live_at, CURRENT_TIMESTAMP),
                live_duration_hours = ROUND(
                    (JULIANDAY(CURRENT_TIMESTAMP) - JULIANDAY(COALESCE(first_seen_live_at, CURRENT_TIMESTAMP))) * 24,
                    2
                )
            WHERE id = ?
            """, (submission_id,))
            # Compute is_payable: approved + alive + 24h survived + not archived
            await db.execute("""
            UPDATE submissions
            SET is_payable = CASE
                WHEN status = 'approved'
                 AND archived = 0
                 AND datetime(submitted_at) <= datetime('now', '-24 hours')
                THEN 1 ELSE 0
            END
            WHERE id = ?
            """, (submission_id,))
        else:  # removed | deleted | shadow_removed
            await db.execute("""
            UPDATE submissions
            SET live_status = ?,
                comment_alive = 0,
                is_payable = 0,
                last_live_check = CURRENT_TIMESTAMP,
                comment_last_checked_at = CURRENT_TIMESTAMP,
                comment_check_count = COALESCE(comment_check_count, 0) + 1
            WHERE id = ?
            """, (live_status, submission_id))
            await db.execute(
                "UPDATE submissions SET status = 'flagged' WHERE id = ? AND status NOT IN ('paid', 'flagged')",
                (submission_id,),
            )
        await db.commit()


async def refresh_payable_for_user(user_id):
    """Recompute is_payable for every approved+live submission for one user.

    Used by the admin 'Refresh Live Check' button so the 24-hour clock advances
    without waiting for a check sweep.
    """
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("BEGIN IMMEDIATE")
        await db.execute("""
        UPDATE submissions
        SET is_payable = CASE
            WHEN status = 'approved'
             AND archived = 0
             AND live_status = 'live'
             AND datetime(submitted_at) <= datetime('now', '-24 hours')
            THEN 1 ELSE 0
        END
        WHERE user_id = ?
        """, (user_id,))
        await db.commit()


async def get_user_submissions_for_live_check(user_id):
    """Return submission rows for a user that still need a live check
    (or could benefit from a refresh). Used by Refresh Live Check inline."""
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
        SELECT s.id, s.reddit_comment_id, s.user_id, s.task_id, s.submitted_at,
               s.live_status, s.last_live_check, s.first_seen_live_at
        FROM submissions s
        WHERE s.user_id = ?
          AND s.reddit_comment_id IS NOT NULL
          AND s.archived = 0
          AND s.status IN ('approved', 'pending_review', 'flagged')
        ORDER BY s.submitted_at DESC
        """, (user_id,))
        return [dict(r) for r in await cursor.fetchall()]


async def get_live_check_stats():
    """One-shot dashboard summary for the admin Live Check view."""
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
        SELECT
            SUM(CASE WHEN live_status = 'live' THEN 1 ELSE 0 END) AS total_live,
            SUM(CASE WHEN live_status IN ('removed','deleted','shadow_removed') THEN 1 ELSE 0 END) AS total_dead,
            SUM(CASE WHEN live_status = 'live' AND status = 'approved'
                      AND datetime(submitted_at) > datetime('now', '-24 hours') THEN 1 ELSE 0 END) AS awaiting_24h,
            SUM(CASE WHEN is_payable = 1 THEN 1 ELSE 0 END) AS payable,
            SUM(CASE WHEN live_status = 'unchecked' THEN 1 ELSE 0 END) AS unchecked,
            SUM(CASE WHEN live_status = 'error' THEN 1 ELSE 0 END) AS errors,
            SUM(CASE WHEN last_live_check IS NULL AND submitted_at < datetime('now', '-2 hours')
                      AND archived = 0 THEN 1 ELSE 0 END) AS stale,
            SUM(CASE WHEN live_status = 'removed' THEN 1 ELSE 0 END) AS removed_by_mod,
            SUM(CASE WHEN live_status = 'deleted' THEN 1 ELSE 0 END) AS deleted_by_user,
            SUM(CASE WHEN live_status = 'shadow_removed' THEN 1 ELSE 0 END) AS shadow_removed
        FROM submissions
        WHERE archived = 0
        """)
        row = await cursor.fetchone()
        return {k: (row[k] or 0) for k in row.keys()}


async def get_submission_for_payment(payment_id):
    """Fetch the submission tied to a payment, used to gate payment on comment_alive."""
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
        SELECT s.id, s.comment_alive, s.user_id
        FROM submissions s
        JOIN payments p ON p.submission_id = s.id
        WHERE p.id = ?
        """, (payment_id,))
        row = await cursor.fetchone()
        return dict(row) if row else None


async def log_audit_action(admin_id, action, details=None):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("BEGIN IMMEDIATE")
        await db.execute("""
        INSERT INTO audit_logs (admin_id, action, details)
        VALUES (?, ?, ?)
        """, (admin_id, action, details))
        await db.commit()


async def get_daily_stats():
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("""
        SELECT
            (SELECT COUNT(*) FROM submissions WHERE date(reviewed_at) = date('now') AND status = 'approved') AS tasks_completed,
            (SELECT COUNT(*) FROM submissions WHERE date(reviewed_at) = date('now') AND status = 'rejected') AS tasks_rejected,
            (SELECT COUNT(*) FROM comments WHERE date(assigned_at) = date('now') AND assigned = 1) AS claims_today,
            (SELECT COUNT(DISTINCT user_id) FROM submissions WHERE date(submitted_at) = date('now')) AS active_members
        """)
        row = await cursor.fetchone()
        
        # Sum payments sent today
        p_cursor = await db.execute("SELECT amount FROM payments WHERE status = 'paid' AND date(paid_at) = date('now')")
        total_sent = 0.0
        for p_row in await p_cursor.fetchall():
            match = re.search(r"([0-9]+(?:\.[0-9]+)?)", p_row[0])
            if match:
                total_sent += float(match.group(1))

        return {
            "tasks_completed": row[0] or 0,
            "tasks_rejected": row[1] or 0,
            "claims_today": row[2] or 0,
            "active_members": row[3] or 0,
            "payments_sent_today": total_sent
        }


async def ban_user(user_id, reason):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("BEGIN IMMEDIATE")
        await db.execute("UPDATE users SET is_banned = 1, notes = ? WHERE telegram_id = ?", (reason, user_id))
        await db.commit()

async def unban_user(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("BEGIN IMMEDIATE")
        await db.execute("UPDATE users SET is_banned = 0 WHERE telegram_id = ?", (user_id,))
        await db.commit()

async def add_member_note(user_id, note):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("BEGIN IMMEDIATE")
        cursor = await db.execute("SELECT notes FROM users WHERE telegram_id = ?", (user_id,))
        row = await cursor.fetchone()
        existing = row[0] if row and row[0] else ""
        new_notes = f"{existing}\n- {note}" if existing else f"- {note}"
        await db.execute("UPDATE users SET notes = ? WHERE telegram_id = ?", (new_notes, user_id))
        await db.commit()

async def add_member_warning(user_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("BEGIN IMMEDIATE")
        await db.execute("UPDATE users SET warnings = warnings + 1 WHERE telegram_id = ?", (user_id,))
        await db.commit()
        cursor = await db.execute("SELECT warnings FROM users WHERE telegram_id = ?", (user_id,))
        row = await cursor.fetchone()
        return row[0] if row else 0


async def reopen_task_with_comments(task_id, reuse_comments=True):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("BEGIN IMMEDIATE")
        await db.execute("UPDATE tasks SET status = 'active', closed_at = NULL WHERE id = ?", (task_id,))
        if reuse_comments:
            # Mark all non-approved comments as reusable
            await db.execute("""
            UPDATE comments 
            SET status = 'reusable', assigned = 0, assigned_to = NULL, assigned_at = NULL 
            WHERE task_id = ? AND status != 'approved'
            """, (task_id,))
        await db.commit()
        return True


async def clone_task(task_id, reuse_comments=True):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("BEGIN IMMEDIATE")
        cursor = await db.execute("SELECT * FROM tasks WHERE id = ?", (task_id,))
        task = await cursor.fetchone()
        if not task:
            return None
        
        # Insert new task
        cursor = await db.execute("""
        INSERT INTO tasks (
            post_url, normalized_post_url, subreddit, post_id, post_path,
            payout_amount, total_slots, category, instructions, created_by, priority, minimum_level
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            task["post_url"], task["normalized_post_url"], task["subreddit"], 
            task["post_id"], task["post_path"], task["payout_amount"], 
            task["total_slots"], task["category"], task["instructions"], 
            task["created_by"], task["priority"], task["minimum_level"]
        ))
        new_task_id = cursor.lastrowid
        
        if reuse_comments:
            # Copy comments
            cursor = await db.execute("SELECT text FROM comments WHERE task_id = ?", (task_id,))
            comments = await cursor.fetchall()
            for row in comments:
                await db.execute("""
                INSERT OR IGNORE INTO comments (task_id, text, status)
                VALUES (?, ?, 'available')
                """, (new_task_id, row["text"]))
        
        await db.commit()
        return new_task_id


async def reset_used_comments(task_id):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("BEGIN IMMEDIATE")
        # Reset reusable/claimed comments back to available if no submission
        await db.execute("""
        UPDATE comments 
        SET status = 'available', assigned = 0, assigned_to = NULL, assigned_at = NULL
        WHERE task_id = ? AND status IN ('reusable', 'claimed')
        """, (task_id,))
        await db.commit()
        return True


async def get_comment_analytics(task_id):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
        SELECT 
            status, COUNT(*) as count
        FROM comments
        WHERE task_id = ?
        GROUP BY status
        """, (task_id,))
        rows = await cursor.fetchall()
        stats = {row["status"]: row["count"] for row in rows}
        return stats
