import asyncio
import logging

import aiosqlite

DB_NAME = "bot.db"
_db_lock = asyncio.Lock()


async def create_db():
    """Create tables and safely upgrade older local databases."""
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA synchronous=NORMAL")
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
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
            closed_at TIMESTAMP
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

        await db.execute("CREATE INDEX IF NOT EXISTS idx_submissions_user_id ON submissions(user_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_submissions_task_id ON submissions(task_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_submissions_status ON submissions(status)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_comments_task_id ON comments(task_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_comments_assigned ON comments(assigned)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_payments_user_id ON payments(user_id)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_payments_status ON payments(status)")
        await db.execute("CREATE INDEX IF NOT EXISTS idx_users_telegram_id ON users(telegram_id)")

        # Duplicate submission protection at the DB level.
        # One submission per (user, comment slot) — prevents double-submit even under race.
        await db.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_submissions_user_comment
        ON submissions(user_id, comment_id)
        WHERE comment_id IS NOT NULL
        """)
        # One submission per normalized Reddit link globally (cross-user fraud guard).
        await db.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_submissions_normalized_link
        ON submissions(normalized_reddit_link)
        WHERE normalized_reddit_link IS NOT NULL
        """)
        # One submission per Reddit comment ID globally.
        await db.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_submissions_reddit_comment_id
        ON submissions(reddit_comment_id)
        WHERE reddit_comment_id IS NOT NULL
        """)

        await _upgrade_columns(db)
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
        ],
        "comments": [
            ("task_id", "INTEGER"),
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
        ],
        "payments": [
            ("payment_method", "TEXT DEFAULT 'UPI'"),
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
        logging.info("DB update success: QR saved for user=%s", user_id)
        logging.info("SELECT qr_file_id FROM users WHERE telegram_id = %s", user_id)
        cursor = await db.execute("""
        SELECT qr_file_id, qr_uploaded_at
        FROM users
        WHERE telegram_id = ?
        """, (user_id,))
        row = await cursor.fetchone()
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


async def add_comments_to_task(task_id, comments):
    clean_comments = []
    seen_in_upload = set()
    skipped_upload_duplicates = 0
    for comment in comments:
        clean_comment = comment.strip()
        if not clean_comment:
            continue
        if clean_comment in seen_in_upload:
            skipped_upload_duplicates += 1
            logging.info(
                "Skipped exact duplicate comment in upload: task=%s text=%r",
                task_id,
                clean_comment,
            )
            continue
        seen_in_upload.add(clean_comment)
        clean_comments.append(clean_comment)

    if not clean_comments:
        logging.info("Comment upload empty after trimming: task=%s", task_id)
        return 0

    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("BEGIN IMMEDIATE")
        task = await _get_task_by_id(db, task_id)
        if not task or task["status"] not in {"active", "paused"}:
            logging.info("Comment upload rejected: task=%s unavailable", task_id)
            return None

        added = 0
        skipped_existing = 0
        for comment in clean_comments:
            cursor = await db.execute("""
            SELECT id
            FROM comments
            WHERE task_id = ?
              AND text = ?
            LIMIT 1
            """, (task_id, comment))
            if await cursor.fetchone():
                skipped_existing += 1
                logging.info(
                    "Skipped exact duplicate comment already stored: task=%s text=%r",
                    task_id,
                    comment,
                )
                continue

            try:
                await db.execute("""
                INSERT INTO comments (task_id, text)
                VALUES (?, ?)
                """, (task_id, comment))
                added += 1
            except aiosqlite.IntegrityError:
                skipped_existing += 1
                logging.info(
                    "Skipped exact duplicate comment at insert: task=%s text=%r",
                    task_id,
                    comment,
                )
                continue

        await db.commit()
        logging.info(
            "Comment upload saved: task=%s added=%s skipped_exact_duplicates=%s",
            task_id,
            added,
            skipped_existing + skipped_upload_duplicates,
        )
        return added


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


async def claim_comment(user_id):
    """Assign one unique comment, respecting priority, rotation, and reputation."""
    active_claim = await get_active_claim(user_id)
    if active_claim:
        active_claim["already_claimed"] = True
        return active_claim

    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("BEGIN IMMEDIATE")

        user = await _get_user_by_id(db, user_id)
        if not user or user["is_banned"]:
            await db.commit()
            return "banned"

        # Reputation check and priority logic
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
            tasks.instructions,
            tasks.priority,
            tasks.is_boosted
        FROM comments
        JOIN tasks ON tasks.id = comments.task_id
        JOIN users ON users.telegram_id = ?
        WHERE comments.assigned = 0
          AND tasks.status = 'active'
          AND (tasks.min_reputation <= users.reputation_score)
          AND (tasks.minimum_level = 'Beginner' OR tasks.minimum_level = (SELECT level FROM worker_levels WHERE user_id = ?) OR (SELECT level FROM worker_levels WHERE user_id = ?) = 'Elite')
          AND (
              SELECT COUNT(*)
              FROM comments AS claimed_comments
              WHERE claimed_comments.task_id = tasks.id
                AND claimed_comments.assigned = 1
          ) < tasks.total_slots
          AND NOT EXISTS (
              SELECT 1 FROM comments c2
              WHERE c2.task_id = tasks.id AND c2.assigned_to = ?
          )
          AND NOT EXISTS (
              SELECT 1 FROM submissions s
              WHERE s.task_id = tasks.id AND s.user_id = ?
          )
        ORDER BY 
            CASE tasks.priority 
                WHEN 'urgent' THEN 1 
                WHEN 'high' THEN 2 
                WHEN 'normal' THEN 3 
                WHEN 'low' THEN 4 
                ELSE 5 
            END ASC,
            tasks.is_boosted DESC,
            RANDOM()
        LIMIT 1
        """, (user_id, user_id, user_id, user_id, user_id))
        row = await cursor.fetchone()
        if not row:
            await db.commit()
            return None

        claim = dict(row)
        await db.execute("""
        UPDATE comments
        SET assigned = 1, assigned_to = ?, assigned_at = CURRENT_TIMESTAMP
        WHERE id = ? AND assigned = 0
        """, (user_id, claim["comment_id"]))
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
        ORDER BY submissions.submitted_at ASC
        LIMIT 20
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
        ORDER BY flags.created_at DESC
        LIMIT 20
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
        GROUP BY tasks.id
        ORDER BY tasks.id DESC
        LIMIT 15
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
            warnings, notes, is_banned, reputation_score, is_shadowbanned, streak_count, badges
        FROM users
        WHERE telegram_id = ?
        """, (user_id, user_id, user_id, user_id, user_id, user_id))
        row = await cursor.fetchone()
        if not row:
            return None
        approved, pending, rejected, paid_count, level, warnings, notes, is_banned, reputation, is_shadowbanned, streak, badges = row
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
            "badges": badges
        }


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
        ORDER BY payments.created_at ASC
        LIMIT 20
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
        cursor = await db.execute("""
        SELECT
            (SELECT COUNT(*) FROM users),
            (SELECT COUNT(*) FROM tasks),
            (SELECT COUNT(*) FROM submissions),
            (SELECT COUNT(*) FROM tasks WHERE status = 'active'),
            (SELECT COUNT(*) FROM submissions WHERE status = 'pending_review'),
            (SELECT COUNT(*) FROM payments WHERE status = 'pending'),
            (SELECT COUNT(*) FROM payments WHERE status = 'paid')
        """)
        row = await cursor.fetchone()
        
        # Helper to sum amounts with currency
        async def sum_amount(status):
            p_cursor = await db.execute("SELECT amount FROM payments WHERE status = ?", (status,))
            total = 0.0
            import re
            for p_row in await p_cursor.fetchall():
                match = re.search(r"([0-9]+(?:\.[0-9]+)?)", p_row[0])
                if match:
                    total += float(match.group(1))
            return total

        total_payouts = await sum_amount('paid')
        pending_payouts = await sum_amount('pending')

        return {
            "total_members": row[0],
            "total_tasks": row[1],
            "total_submissions": row[2],
            "active_tasks": row[3],
            "pending_reviews": row[4],
            "pending_payments": row[5],
            "completed_payments": row[6],
            "total_payouts": total_payouts,
            "pending_payouts_sum": pending_payouts
        }


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
        SUM(CASE WHEN comments.assigned = 1 THEN 1 ELSE 0 END) AS assigned_comments
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
    """Release claims older than 30 minutes with no submission, then reopen any full tasks that now have free slots."""
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("BEGIN IMMEDIATE")

        cursor = await db.execute("""
        SELECT assigned_to, task_id
        FROM comments
        WHERE assigned = 1
          AND assigned_at < datetime('now', '-30 minutes')
          AND id NOT IN (SELECT comment_id FROM submissions WHERE comment_id IS NOT NULL)
        """)
        expired_claims = [dict(row) for row in await cursor.fetchall()]

        if expired_claims:
            await db.execute("""
            UPDATE comments
            SET assigned = 0, assigned_to = NULL, assigned_at = NULL
            WHERE assigned = 1
              AND assigned_at < datetime('now', '-30 minutes')
              AND id NOT IN (SELECT comment_id FROM submissions WHERE comment_id IS NOT NULL)
            """)
            # Reopen any 'full' tasks that now have unassigned comments available
            await db.execute("""
            UPDATE tasks SET status = 'active'
            WHERE status = 'full'
              AND id IN (
                  SELECT DISTINCT task_id FROM comments
                  WHERE assigned = 0
              )
            """)

        await db.commit()
        return expired_claims


async def log_audit_action(admin_id, action, details=None):
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("BEGIN IMMEDIATE")
        await db.execute("""
        INSERT INTO audit_logs (admin_id, action, details)
        VALUES (?, ?, ?)
        """, (admin_id, action, details))
        await db.commit()


async def get_leaderboard(limit=10):
    async with aiosqlite.connect(DB_NAME) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute("""
        SELECT users.telegram_id, users.username, worker_levels.level,
               (SELECT COUNT(*) FROM submissions WHERE user_id = users.telegram_id AND status = 'approved') AS approved_tasks,
               (SELECT SUM(amount) FROM payments WHERE user_id = users.telegram_id AND status = 'paid') AS total_earned
        FROM users
        LEFT JOIN worker_levels ON worker_levels.user_id = users.telegram_id
        ORDER BY total_earned DESC, approved_tasks DESC
        LIMIT ?
        """, (limit,))
        return [dict(row) for row in await cursor.fetchall()]


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
        import re
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


