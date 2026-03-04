import sqlite3, os, time, json

DB_PATH = os.environ.get("DB_PATH", os.path.join(os.path.dirname(__file__), "insta.db"))

def get_conn():
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn

def init_db():
    conn = get_conn()
    c = conn.cursor()

    c.execute("""
    CREATE TABLE IF NOT EXISTS influencers (
        pk                   TEXT PRIMARY KEY,
        username             TEXT,
        full_name            TEXT,
        biography            TEXT,
        follower_count       INTEGER DEFAULT 0,
        following_count      INTEGER DEFAULT 0,
        media_count          INTEGER DEFAULT 0,
        is_private           INTEGER DEFAULT 0,
        is_verified          INTEGER DEFAULT 0,
        is_business          INTEGER DEFAULT 0,
        category             TEXT,
        public_email         TEXT,
        public_phone         TEXT,
        external_url         TEXT,
        profile_pic_url      TEXT,
        profile_pic_local    TEXT DEFAULT '',
        hashtags             TEXT DEFAULT '',
        avg_likes            REAL DEFAULT 0,
        avg_comments         REAL DEFAULT 0,
        engagement_rate      REAL DEFAULT 0,
        avg_reel_views       REAL DEFAULT 0,
        avg_feed_likes       REAL DEFAULT 0,
        reel_count           INTEGER DEFAULT 0,
        feed_count           INTEGER DEFAULT 0,
        upload_frequency     TEXT DEFAULT '',
        active_hours         TEXT DEFAULT '',
        avg_posting_interval REAL DEFAULT 0,
        last_post_date       TEXT DEFAULT '',
        reels_ratio          REAL DEFAULT 0,
        sponsored_ratio      REAL DEFAULT 0,
        top_posts_likes      TEXT DEFAULT '[]',
        top_posts_comments   TEXT DEFAULT '[]',
        top_reels_views      TEXT DEFAULT '[]',
        stats_updated_at     REAL DEFAULT 0,
        created_at           REAL,
        updated_at           REAL
    )""")

    c.execute("""
    CREATE TABLE IF NOT EXISTS influencer_manual (
        pk              TEXT PRIMARY KEY,
        contact_name    TEXT DEFAULT '',
        contact_kakao   TEXT DEFAULT '',
        contact_line    TEXT DEFAULT '',
        contact_email   TEXT DEFAULT '',
        contact_phone   TEXT DEFAULT '',
        can_live        INTEGER DEFAULT 0,
        live_platforms  TEXT DEFAULT '',
        live_price      INTEGER DEFAULT 0,
        feed_price      INTEGER DEFAULT 0,
        reel_price      INTEGER DEFAULT 0,
        story_price     INTEGER DEFAULT 0,
        bundle_price    INTEGER DEFAULT 0,
        main_category   TEXT DEFAULT '',
        sub_categories  TEXT DEFAULT '',
        target_gender   TEXT DEFAULT '',
        target_age      TEXT DEFAULT '',
        target_region   TEXT DEFAULT '',
        collab_types    TEXT DEFAULT '',
        past_brands     TEXT DEFAULT '',
        quality_score   INTEGER DEFAULT 0,
        notes           TEXT DEFAULT '',
        is_approved     INTEGER DEFAULT 0,
        approved_at     REAL DEFAULT 0,
        updated_at      REAL DEFAULT 0
    )""")

    c.execute("""
    CREATE TABLE IF NOT EXISTS posts (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        influencer_pk   TEXT,
        post_id         TEXT UNIQUE,
        post_url        TEXT,
        post_type       TEXT,
        likes           INTEGER DEFAULT 0,
        comments        INTEGER DEFAULT 0,
        views           INTEGER DEFAULT 0,
        caption         TEXT DEFAULT '',
        hashtags_used   TEXT DEFAULT '',
        is_sponsored    INTEGER DEFAULT 0,
        thumbnail_url   TEXT DEFAULT '',
        thumbnail_local TEXT DEFAULT '',
        taken_at        REAL,
        crawled_at      REAL
    )""")

    c.execute("""
    CREATE TABLE IF NOT EXISTS hashtags (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        name            TEXT UNIQUE,
        status          TEXT DEFAULT 'idle',
        auto_collect    INTEGER DEFAULT 1,
        total_collected INTEGER DEFAULT 0,
        last_run_at     REAL,
        created_at      REAL
    )""")

    c.execute("""
    CREATE TABLE IF NOT EXISTS collect_jobs (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        hashtag         TEXT,
        status          TEXT DEFAULT 'running',
        requested_count INTEGER DEFAULT 0,
        collected_posts INTEGER DEFAULT 0,
        new_users       INTEGER DEFAULT 0,
        updated_users   INTEGER DEFAULT 0,
        started_at      REAL,
        finished_at     REAL,
        error_msg       TEXT
    )""")

    c.execute("""
    CREATE TABLE IF NOT EXISTS advertiser_accounts (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        username        TEXT UNIQUE,
        password_hash   TEXT,
        company_name    TEXT DEFAULT '',
        hashtag_access  TEXT DEFAULT '',
        min_followers   INTEGER DEFAULT 0,
        only_approved   INTEGER DEFAULT 1,
        created_at      REAL
    )""")

    c.execute("""
    CREATE TABLE IF NOT EXISTS refresh_jobs (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        status      TEXT DEFAULT 'running',
        total       INTEGER DEFAULT 0,
        done        INTEGER DEFAULT 0,
        current_user TEXT DEFAULT '',
        started_at  REAL,
        finished_at REAL,
        error_msg   TEXT
    )""")

    c.execute("CREATE INDEX IF NOT EXISTS idx_follower    ON influencers(follower_count DESC)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_updated     ON influencers(updated_at)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_posts_inf   ON posts(influencer_pk)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_posts_taken ON posts(taken_at DESC)")

    _migrate(conn)

    # engagement_rate 인덱스는 마이그레이션 후에 생성
    try:
        c.execute("CREATE INDEX IF NOT EXISTS idx_engagement ON influencers(engagement_rate DESC)")
    except:
        pass

    conn.commit()
    conn.close()


def _migrate(conn):
    existing = {row[1] for row in conn.execute("PRAGMA table_info(influencers)").fetchall()}
    new_cols = [
        ("profile_pic_local",    "TEXT DEFAULT ''"),
        ("avg_likes",            "REAL DEFAULT 0"),
        ("avg_comments",         "REAL DEFAULT 0"),
        ("engagement_rate",      "REAL DEFAULT 0"),
        ("avg_reel_views",       "REAL DEFAULT 0"),
        ("avg_feed_likes",       "REAL DEFAULT 0"),
        ("avg_feed_comments",    "REAL DEFAULT 0"),
        ("avg_reel_likes",       "REAL DEFAULT 0"),
        ("avg_reel_comments",    "REAL DEFAULT 0"),
        ("reel_count",           "INTEGER DEFAULT 0"),
        ("feed_count",           "INTEGER DEFAULT 0"),
        ("upload_frequency",     "TEXT DEFAULT ''"),
        ("active_hours",         "TEXT DEFAULT ''"),
        ("avg_posting_interval", "REAL DEFAULT 0"),
        ("last_post_date",       "TEXT DEFAULT ''"),
        ("reels_ratio",          "REAL DEFAULT 0"),
        ("sponsored_ratio",      "REAL DEFAULT 0"),
        ("top_posts_likes",      "TEXT DEFAULT '[]'"),
        ("top_posts_comments",   "TEXT DEFAULT '[]'"),
        ("top_reels_views",      "TEXT DEFAULT '[]'"),
        ("stats_updated_at",     "REAL DEFAULT 0"),
    ]
    for col, typedef in new_cols:
        if col not in existing:
            conn.execute(f"ALTER TABLE influencers ADD COLUMN {col} {typedef}")

    # influencer_manual 마이그레이션 (개인속성 + SNS)
    manual_existing = {row[1] for row in conn.execute("PRAGMA table_info(influencer_manual)").fetchall()}
    manual_new_cols = [
        ("has_pet",      "INTEGER DEFAULT 0"),
        ("is_married",   "INTEGER DEFAULT 0"),
        ("has_kids",     "INTEGER DEFAULT 0"),
        ("has_car",      "INTEGER DEFAULT 0"),
        ("pet_type",     "TEXT DEFAULT ''"),
        ("kids_age",     "TEXT DEFAULT ''"),
        ("is_brand",     "INTEGER DEFAULT 0"),  # 브랜드 계정 여부
        ("is_visual",    "INTEGER DEFAULT 0"),  # 비주얼 인플루언서 (얼굴노출 가능)
        ("face_exposed", "INTEGER DEFAULT 0"),  # 얼굴 노출 여부
        ("tiktok_url",   "TEXT DEFAULT ''"),
        ("youtube_url",  "TEXT DEFAULT ''"),
        ("facebook_url", "TEXT DEFAULT ''"),
        ("threads_url",  "TEXT DEFAULT ''"),
        ("tiktok_followers",  "INTEGER DEFAULT 0"),
        ("youtube_subscribers","INTEGER DEFAULT 0"),
    ]
    for col, typedef in manual_new_cols:
        if col not in manual_existing:
            conn.execute(f"ALTER TABLE influencer_manual ADD COLUMN {col} {typedef}")

    # influencers 마이그레이션 (top_hashtags)
    inf_existing = {row[1] for row in conn.execute("PRAGMA table_info(influencers)").fetchall()}
    if "top_hashtags" not in inf_existing:
        conn.execute("ALTER TABLE influencers ADD COLUMN top_hashtags TEXT DEFAULT '[]'")

    # refresh_jobs 마이그레이션
    rj_existing = {row[1] for row in conn.execute("PRAGMA table_info(refresh_jobs)").fetchall()}
    if "current_user" not in rj_existing:
        conn.execute("ALTER TABLE refresh_jobs ADD COLUMN current_user TEXT DEFAULT ''")


def upsert_influencer(data: dict) -> str:
    conn = get_conn()
    c = conn.cursor()
    now = time.time()
    pk = str(data.get("pk", ""))

    existing = c.execute("SELECT pk, hashtags FROM influencers WHERE pk=?", (pk,)).fetchone()
    new_tag = data.get("hashtag", "")

    if existing:
        old_tags = set(t.strip() for t in (existing["hashtags"] or "").split(",") if t.strip())
        if new_tag: old_tags.add(new_tag)
        merged_tags = ",".join(old_tags)
        c.execute("""
            UPDATE influencers SET
                username=?, full_name=?, biography=?,
                follower_count=?, following_count=?, media_count=?,
                is_private=?, is_verified=?, is_business=?,
                category=?, public_email=?, public_phone=?,
                external_url=?, profile_pic_url=?, hashtags=?, updated_at=?
            WHERE pk=?
        """, (
            data.get("username"), data.get("full_name"), data.get("biography"),
            data.get("follower_count", 0), data.get("following_count", 0), data.get("media_count", 0),
            int(data.get("is_private", False)), int(data.get("is_verified", False)), int(data.get("is_business", False)),
            data.get("category"), data.get("public_email"), data.get("public_phone"),
            data.get("external_url"), data.get("profile_pic_url"), merged_tags, now, pk
        ))
        result = "updated"
    else:
        c.execute("""
            INSERT INTO influencers
                (pk, username, full_name, biography,
                 follower_count, following_count, media_count,
                 is_private, is_verified, is_business,
                 category, public_email, public_phone,
                 external_url, profile_pic_url, hashtags, created_at, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            pk, data.get("username"), data.get("full_name"), data.get("biography"),
            data.get("follower_count", 0), data.get("following_count", 0), data.get("media_count", 0),
            int(data.get("is_private", False)), int(data.get("is_verified", False)), int(data.get("is_business", False)),
            data.get("category"), data.get("public_email"), data.get("public_phone"),
            data.get("external_url"), data.get("profile_pic_url"),
            new_tag, now, now
        ))
        result = "new"

    conn.commit()
    conn.close()
    return result


def update_influencer_stats(pk: str, stats: dict):
    conn = get_conn()
    conn.execute("""
        UPDATE influencers SET
            avg_likes=?, avg_comments=?, engagement_rate=?,
            avg_reel_views=?, avg_feed_likes=?,
            reel_count=?, feed_count=?,
            upload_frequency=?, active_hours=?, avg_posting_interval=?,
            last_post_date=?, reels_ratio=?, sponsored_ratio=?,
            top_posts_likes=?, top_posts_comments=?, top_reels_views=?,
            profile_pic_local=?, stats_updated_at=?
        WHERE pk=?
    """, (
        stats.get("avg_likes", 0), stats.get("avg_comments", 0), stats.get("engagement_rate", 0),
        stats.get("avg_reel_views", 0), stats.get("avg_feed_likes", 0),
        stats.get("reel_count", 0), stats.get("feed_count", 0),
        stats.get("upload_frequency", ""), stats.get("active_hours", ""),
        stats.get("avg_posting_interval", 0),
        stats.get("last_post_date", ""), stats.get("reels_ratio", 0), stats.get("sponsored_ratio", 0),
        json.dumps(stats.get("top_posts_likes", []), ensure_ascii=False),
        json.dumps(stats.get("top_posts_comments", []), ensure_ascii=False),
        json.dumps(stats.get("top_reels_views", []), ensure_ascii=False),
        stats.get("profile_pic_local", ""),
        time.time(), pk
    ))
    conn.commit()
    conn.close()


def upsert_post(data: dict):
    conn = get_conn()
    existing = conn.execute("SELECT id FROM posts WHERE post_id=?", (data["post_id"],)).fetchone()
    now = time.time()
    if existing:
        conn.execute("""
            UPDATE posts SET likes=?, comments=?, views=?, crawled_at=?
            WHERE post_id=?
        """, (data.get("likes", 0), data.get("comments", 0), data.get("views", 0), now, data["post_id"]))
    else:
        conn.execute("""
            INSERT INTO posts
                (influencer_pk, post_id, post_url, post_type, likes, comments, views,
                 caption, hashtags_used, is_sponsored, thumbnail_url, thumbnail_local, taken_at, crawled_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            data.get("influencer_pk"), data.get("post_id"), data.get("post_url"),
            data.get("post_type"), data.get("likes", 0), data.get("comments", 0), data.get("views", 0),
            data.get("caption", ""), data.get("hashtags_used", ""), data.get("is_sponsored", 0),
            data.get("thumbnail_url", ""), data.get("thumbnail_local", ""),
            data.get("taken_at"), now
        ))
    conn.commit()
    conn.close()


def get_manual(pk: str) -> dict:
    conn = get_conn()
    row = conn.execute("SELECT * FROM influencer_manual WHERE pk=?", (pk,)).fetchone()
    conn.close()
    return dict(row) if row else {}


def save_manual(pk: str, data: dict):
    conn = get_conn()
    existing = conn.execute("SELECT pk FROM influencer_manual WHERE pk=?", (pk,)).fetchone()
    now = time.time()
    if existing:
        sets = ", ".join(f"{k}=?" for k in data.keys())
        vals = list(data.values()) + [now, pk]
        conn.execute(f"UPDATE influencer_manual SET {sets}, updated_at=? WHERE pk=?", vals)
    else:
        data["pk"] = pk
        data["updated_at"] = now
        cols = ", ".join(data.keys())
        placeholders = ", ".join("?" * len(data))
        conn.execute(f"INSERT INTO influencer_manual ({cols}) VALUES ({placeholders})", list(data.values()))
    conn.commit()
    conn.close()


def get_influencers(keyword="", min_f=None, max_f=None,
                    only_verified=False, exclude_private=False,
                    hashtag_filter="", main_category="",
                    can_live=False, only_approved=False,
                    has_pet=False, is_married=False, has_kids=False, has_car=False,
                    is_visual=False,
                    sort="follower_count", order="desc",
                    page=1, per_page=50):
    conn = get_conn()
    conditions, params = [], []

    if keyword:
        conditions.append("(i.username LIKE ? OR i.full_name LIKE ? OR i.biography LIKE ?)")
        params += [f"%{keyword}%"] * 3
    if hashtag_filter:
        conditions.append("i.hashtags LIKE ?")
        params.append(f"%{hashtag_filter}%")
    if min_f is not None:
        conditions.append("i.follower_count >= ?"); params.append(min_f)
    if max_f is not None:
        conditions.append("i.follower_count <= ?"); params.append(max_f)
    if only_verified:
        conditions.append("i.is_verified = 1")
    if exclude_private:
        conditions.append("i.is_private = 0")
    if main_category:
        conditions.append("m.main_category = ?"); params.append(main_category)
    if can_live:
        conditions.append("m.can_live = 1")
    if only_approved:
        conditions.append("m.is_approved = 1")
    if has_pet:
        conditions.append("m.has_pet = 1")
    if is_married:
        conditions.append("m.is_married = 1")
    if has_kids:
        conditions.append("m.has_kids = 1")
    if has_car:
        conditions.append("m.has_car = 1")
    if is_visual:
        conditions.append("m.is_visual = 1")

    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    valid_sorts = {"follower_count", "engagement_rate", "avg_reel_views", "avg_likes",
                   "media_count", "updated_at", "stats_updated_at", "username"}
    sort = sort if sort in valid_sorts else "follower_count"
    order_sql = f"ORDER BY i.{sort} {'DESC' if order == 'desc' else 'ASC'}"
    offset = (page - 1) * per_page

    join = "LEFT JOIN influencer_manual m ON i.pk = m.pk"
    total = conn.execute(f"SELECT COUNT(*) FROM influencers i {join} {where}", params).fetchone()[0]
    rows = conn.execute(
        f"SELECT i.*, COALESCE(m.can_live,0) as can_live, COALESCE(m.is_approved,0) as is_approved, "
        f"COALESCE(m.main_category,'') as manual_category, COALESCE(m.quality_score,0) as quality_score, "
        f"COALESCE(m.contact_name,'') as contact_name, COALESCE(m.collab_types,'') as collab_types, "
        f"COALESCE(m.live_platforms,'') as live_platforms, COALESCE(m.feed_price,0) as feed_price, "
        f"COALESCE(m.reel_price,0) as reel_price, "
        f"COALESCE(m.has_pet,0) as has_pet, COALESCE(m.is_married,0) as is_married, "
        f"COALESCE(m.has_kids,0) as has_kids, COALESCE(m.has_car,0) as has_car, "
        f"COALESCE(m.is_visual,0) as is_visual, COALESCE(m.face_exposed,0) as face_exposed "
        f"FROM influencers i {join} {where} {order_sql} LIMIT ? OFFSET ?",
        params + [per_page, offset]
    ).fetchall()
    conn.close()
    return total, [dict(r) for r in rows]


def get_influencer(pk: str) -> dict:
    conn = get_conn()
    row = conn.execute(
        "SELECT i.* FROM influencers i WHERE i.pk=?", (pk,)
    ).fetchone()
    conn.close()
    return dict(row) if row else {}


def get_influencer_posts(pk: str) -> list:
    conn = get_conn()
    rows = conn.execute(
        "SELECT * FROM posts WHERE influencer_pk=? ORDER BY taken_at DESC",
        (pk,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_stats():
    conn = get_conn()
    total      = conn.execute("SELECT COUNT(*) FROM influencers").fetchone()[0]
    verified   = conn.execute("SELECT COUNT(*) FROM influencers WHERE is_verified=1").fetchone()[0]
    business   = conn.execute("SELECT COUNT(*) FROM influencers WHERE is_business=1").fetchone()[0]
    htags      = conn.execute("SELECT COUNT(*) FROM hashtags").fetchone()[0]
    with_stats = conn.execute("SELECT COUNT(*) FROM influencers WHERE stats_updated_at > 0").fetchone()[0]
    live_ok    = conn.execute("SELECT COUNT(*) FROM influencer_manual WHERE can_live=1").fetchone()[0]
    approved   = conn.execute("SELECT COUNT(*) FROM influencer_manual WHERE is_approved=1").fetchone()[0]
    has_url    = conn.execute("SELECT COUNT(*) FROM influencers WHERE external_url IS NOT NULL AND external_url != ''").fetchone()[0]
    linktree   = conn.execute("SELECT COUNT(*) FROM influencers WHERE external_url LIKE '%linktree%' OR external_url LIKE '%linktr.ee%'").fetchone()[0]
    has_tiktok = conn.execute("SELECT COUNT(*) FROM influencer_manual WHERE tiktok_url != '' AND tiktok_url IS NOT NULL").fetchone()[0]
    has_youtube= conn.execute("SELECT COUNT(*) FROM influencer_manual WHERE youtube_url != '' AND youtube_url IS NOT NULL").fetchone()[0]
    has_kids   = conn.execute("SELECT COUNT(*) FROM influencer_manual WHERE has_kids=1").fetchone()[0]
    has_pet    = conn.execute("SELECT COUNT(*) FROM influencer_manual WHERE has_pet=1").fetchone()[0]
    is_married = conn.execute("SELECT COUNT(*) FROM influencer_manual WHERE is_married=1").fetchone()[0]
    is_visual  = conn.execute("SELECT COUNT(*) FROM influencer_manual WHERE is_visual=1").fetchone()[0]
    conn.close()
    return {
        "total": total, "verified": verified, "business": business,
        "hashtags": htags, "with_stats": with_stats,
        "live_ok": live_ok, "approved": approved,
        "has_url": has_url, "linktree": linktree,
        "has_tiktok": has_tiktok, "has_youtube": has_youtube,
        "has_kids": has_kids, "has_pet": has_pet, "is_married": is_married,
        "is_visual": is_visual,
    }


def get_public_stats():
    """비로그인용 통계 (개수만)"""
    conn = get_conn()
    total    = conn.execute("SELECT COUNT(*) FROM influencers").fetchone()[0]
    verified = conn.execute("SELECT COUNT(*) FROM influencers WHERE is_verified=1").fetchone()[0]
    htags    = conn.execute("SELECT COUNT(*) FROM hashtags").fetchone()[0]
    # 팔로워 합계
    total_followers = conn.execute("SELECT SUM(follower_count) FROM influencers").fetchone()[0] or 0
    conn.close()
    return {"total": total, "verified": verified, "hashtags": htags, "total_followers": total_followers}


def get_public_influencers(page=1, per_page=30, sort="follower_count"):
    """비로그인용 인플루언서 목록 (제한 정보만)"""
    conn = get_conn()
    offset = (page - 1) * per_page
    total = conn.execute("SELECT COUNT(*) FROM influencers").fetchone()[0]
    rows = conn.execute(
        f"SELECT pk, username, full_name, follower_count, is_verified, is_business, "
        f"category, profile_pic_local, engagement_rate, avg_reel_views, hashtags "
        f"FROM influencers ORDER BY {sort} DESC LIMIT ? OFFSET ?",
        [per_page, offset]
    ).fetchall()
    conn.close()
    return total, [dict(r) for r in rows]


def get_advertisers():
    conn = get_conn()
    rows = conn.execute("SELECT * FROM advertiser_accounts ORDER BY created_at DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_refresh_status():
    conn = get_conn()
    job = conn.execute("SELECT * FROM refresh_jobs ORDER BY started_at DESC LIMIT 1").fetchone()
    conn.close()
    return dict(job) if job else {}
