"""
데이터베이스 레이어
- 프로덕션(Vercel): Supabase PostgREST API
- 개발(로컬): SQLite
"""
import os, time, json, requests as _req

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://kknkekgendsxwbsivcnq.supabase.co")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY", "")  # service_role key
_USE_SUPABASE = bool(os.environ.get("SUPABASE_KEY"))

# 로컬 SQLite 폴백
if not _USE_SUPABASE:
    import sqlite3
    DB_PATH = os.environ.get("DB_PATH", os.path.join(os.path.dirname(__file__), "insta.db"))

# 테이블 이름
T_INF  = "insta_influencers"
T_MAN  = "insta_influencer_manual"
T_POST = "insta_posts"
T_HASH = "insta_hashtags"
T_CJOB = "insta_collect_jobs"
T_ADV  = "insta_advertiser_accounts"
T_RJOB = "insta_refresh_jobs"


# ─── Supabase REST 헬퍼 ────────────────────────────────────────────
def _sb_headers():
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation",
    }

def _sb_url(table, filters=""):
    return f"{SUPABASE_URL}/rest/v1/{table}{filters}"

def _sb_get(table, params=None, count=False):
    headers = _sb_headers()
    if count:
        headers["Prefer"] = "count=exact"
    r = _req.get(_sb_url(table), headers=headers, params=params or {})
    data = r.json() if not count else (r.json(), int(r.headers.get("Content-Range","0").split("/")[-1] or 0))
    return data

def _sb_post(table, data):
    r = _req.post(_sb_url(table), headers=_sb_headers(), json=data)
    return r.json()

def _sb_patch(table, filters, data):
    r = _req.patch(_sb_url(table, filters), headers=_sb_headers(), json=data)
    return r.json()

def _sb_rpc(func, payload=None):
    r = _req.post(f"{SUPABASE_URL}/rest/v1/rpc/{func}", headers=_sb_headers(), json=payload or {})
    return r.json()


# ─── 로컬 SQLite 연결 ─────────────────────────────────────────────
def get_conn():
    if _USE_SUPABASE:
        raise RuntimeError("Supabase 모드에서 get_conn() 사용 불가")
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db():
    if _USE_SUPABASE:
        return  # Supabase에 이미 테이블 존재
    import sqlite3 as sq
    conn = sq.connect(DB_PATH)
    conn.row_factory = sq.Row
    conn.execute("PRAGMA journal_mode=WAL")
    c = conn.cursor()

    c.execute(f"""CREATE TABLE IF NOT EXISTS {T_INF} (
        pk TEXT PRIMARY KEY, username TEXT, full_name TEXT, biography TEXT,
        follower_count INTEGER DEFAULT 0, following_count INTEGER DEFAULT 0, media_count INTEGER DEFAULT 0,
        is_private INTEGER DEFAULT 0, is_verified INTEGER DEFAULT 0, is_business INTEGER DEFAULT 0,
        category TEXT, public_email TEXT, public_phone TEXT, external_url TEXT, profile_pic_url TEXT,
        profile_pic_local TEXT DEFAULT '', hashtags TEXT DEFAULT '',
        avg_likes REAL DEFAULT 0, avg_comments REAL DEFAULT 0, engagement_rate REAL DEFAULT 0,
        avg_reel_views REAL DEFAULT 0, avg_feed_likes REAL DEFAULT 0,
        avg_feed_comments REAL DEFAULT 0, avg_reel_likes REAL DEFAULT 0, avg_reel_comments REAL DEFAULT 0,
        reel_count INTEGER DEFAULT 0, feed_count INTEGER DEFAULT 0,
        upload_frequency TEXT DEFAULT '', active_hours TEXT DEFAULT '',
        avg_posting_interval REAL DEFAULT 0, last_post_date TEXT DEFAULT '',
        reels_ratio REAL DEFAULT 0, sponsored_ratio REAL DEFAULT 0,
        top_posts_likes TEXT DEFAULT '[]', top_posts_comments TEXT DEFAULT '[]',
        top_reels_views TEXT DEFAULT '[]', top_hashtags TEXT DEFAULT '[]',
        stats_updated_at REAL DEFAULT 0, created_at REAL, updated_at REAL
    )""")
    c.execute(f"""CREATE TABLE IF NOT EXISTS {T_MAN} (
        pk TEXT PRIMARY KEY, contact_name TEXT DEFAULT '', contact_kakao TEXT DEFAULT '',
        contact_line TEXT DEFAULT '', contact_email TEXT DEFAULT '', contact_phone TEXT DEFAULT '',
        can_live INTEGER DEFAULT 0, live_platforms TEXT DEFAULT '', live_price INTEGER DEFAULT 0,
        feed_price INTEGER DEFAULT 0, reel_price INTEGER DEFAULT 0, story_price INTEGER DEFAULT 0,
        bundle_price INTEGER DEFAULT 0, main_category TEXT DEFAULT '', sub_categories TEXT DEFAULT '',
        target_gender TEXT DEFAULT '', target_age TEXT DEFAULT '', target_region TEXT DEFAULT '',
        collab_types TEXT DEFAULT '', past_brands TEXT DEFAULT '', quality_score INTEGER DEFAULT 0,
        notes TEXT DEFAULT '', is_approved INTEGER DEFAULT 0, approved_at REAL DEFAULT 0,
        updated_at REAL DEFAULT 0, has_pet INTEGER DEFAULT 0, is_married INTEGER DEFAULT 0,
        has_kids INTEGER DEFAULT 0, has_car INTEGER DEFAULT 0, pet_type TEXT DEFAULT '',
        kids_age TEXT DEFAULT '', is_brand INTEGER DEFAULT 0, is_visual INTEGER DEFAULT 0,
        face_exposed INTEGER DEFAULT 0, tiktok_url TEXT DEFAULT '', youtube_url TEXT DEFAULT '',
        facebook_url TEXT DEFAULT '', threads_url TEXT DEFAULT '',
        tiktok_followers INTEGER DEFAULT 0, youtube_subscribers INTEGER DEFAULT 0
    )""")
    c.execute(f"""CREATE TABLE IF NOT EXISTS {T_POST} (
        id INTEGER PRIMARY KEY AUTOINCREMENT, influencer_pk TEXT, post_id TEXT UNIQUE,
        post_url TEXT, post_type TEXT, likes INTEGER DEFAULT 0, comments INTEGER DEFAULT 0,
        views INTEGER DEFAULT 0, caption TEXT DEFAULT '', hashtags_used TEXT DEFAULT '',
        is_sponsored INTEGER DEFAULT 0, thumbnail_url TEXT DEFAULT '', thumbnail_local TEXT DEFAULT '',
        taken_at REAL, crawled_at REAL
    )""")
    c.execute(f"""CREATE TABLE IF NOT EXISTS {T_HASH} (
        id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT UNIQUE, status TEXT DEFAULT 'idle',
        auto_collect INTEGER DEFAULT 1, total_collected INTEGER DEFAULT 0,
        last_run_at REAL, created_at REAL
    )""")
    c.execute(f"""CREATE TABLE IF NOT EXISTS {T_CJOB} (
        id INTEGER PRIMARY KEY AUTOINCREMENT, hashtag TEXT, status TEXT DEFAULT 'running',
        requested_count INTEGER DEFAULT 0, collected_posts INTEGER DEFAULT 0,
        new_users INTEGER DEFAULT 0, updated_users INTEGER DEFAULT 0,
        started_at REAL, finished_at REAL, error_msg TEXT
    )""")
    c.execute(f"""CREATE TABLE IF NOT EXISTS {T_ADV} (
        id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT UNIQUE, password_hash TEXT,
        company_name TEXT DEFAULT '', hashtag_access TEXT DEFAULT '',
        min_followers INTEGER DEFAULT 0, only_approved INTEGER DEFAULT 1,
        plan TEXT DEFAULT 'free', plan_expires_at REAL DEFAULT 0,
        monthly_collect_limit INTEGER DEFAULT 500, monthly_collected INTEGER DEFAULT 0,
        collect_reset_at REAL DEFAULT 0, created_at REAL
    )""")
    c.execute(f"""CREATE TABLE IF NOT EXISTS {T_RJOB} (
        id INTEGER PRIMARY KEY AUTOINCREMENT, status TEXT DEFAULT 'running',
        total_count INTEGER DEFAULT 0, done INTEGER DEFAULT 0,
        current_user_pk TEXT DEFAULT '', started_at REAL, finished_at REAL, error_msg TEXT
    )""")

    _migrate(conn)
    conn.commit()
    conn.close()


def _migrate(conn):
    existing = {row[1] for row in conn.execute(f"PRAGMA table_info({T_INF})").fetchall()}
    for col, td in [
        ("avg_feed_comments","REAL DEFAULT 0"),("avg_reel_likes","REAL DEFAULT 0"),
        ("avg_reel_comments","REAL DEFAULT 0"),("top_hashtags","TEXT DEFAULT '[]'"),
    ]:
        if col not in existing:
            conn.execute(f"ALTER TABLE {T_INF} ADD COLUMN {col} {td}")

    manual_existing = {row[1] for row in conn.execute(f"PRAGMA table_info({T_MAN})").fetchall()}
    for col, td in [
        ("has_pet","INTEGER DEFAULT 0"),("is_married","INTEGER DEFAULT 0"),
        ("has_kids","INTEGER DEFAULT 0"),("has_car","INTEGER DEFAULT 0"),
        ("pet_type","TEXT DEFAULT ''"),("kids_age","TEXT DEFAULT ''"),
        ("is_brand","INTEGER DEFAULT 0"),("is_visual","INTEGER DEFAULT 0"),
        ("face_exposed","INTEGER DEFAULT 0"),("tiktok_url","TEXT DEFAULT ''"),
        ("youtube_url","TEXT DEFAULT ''"),("facebook_url","TEXT DEFAULT ''"),
        ("threads_url","TEXT DEFAULT ''"),("tiktok_followers","INTEGER DEFAULT 0"),
        ("youtube_subscribers","INTEGER DEFAULT 0"),("bundle_price","INTEGER DEFAULT 0"),
    ]:
        if col not in manual_existing:
            conn.execute(f"ALTER TABLE {T_MAN} ADD COLUMN {col} {td}")

    adv_existing = {row[1] for row in conn.execute(f"PRAGMA table_info({T_ADV})").fetchall()}
    for col, td in [
        ("plan","TEXT DEFAULT 'free'"),("plan_expires_at","REAL DEFAULT 0"),
        ("monthly_collect_limit","INTEGER DEFAULT 500"),("monthly_collected","INTEGER DEFAULT 0"),
        ("collect_reset_at","REAL DEFAULT 0"),
    ]:
        if col not in adv_existing:
            conn.execute(f"ALTER TABLE {T_ADV} ADD COLUMN {col} {td}")

    rj_existing = {row[1] for row in conn.execute(f"PRAGMA table_info({T_RJOB})").fetchall()}
    if "current_user_pk" not in rj_existing and "current_user" in rj_existing:
        pass  # 이미 있음
    elif "current_user_pk" not in rj_existing:
        try: conn.execute(f"ALTER TABLE {T_RJOB} ADD COLUMN current_user_pk TEXT DEFAULT ''")
        except: pass
    if "total_count" not in rj_existing:
        try: conn.execute(f"ALTER TABLE {T_RJOB} ADD COLUMN total_count INTEGER DEFAULT 0")
        except: pass


# ─── SQLite 헬퍼 ──────────────────────────────────────────────────
def _sq_one(sql, params=()):
    conn = get_conn()
    try:
        r = conn.execute(sql, params).fetchone()
        return dict(r) if r else None
    finally: conn.close()

def _sq_all(sql, params=()):
    conn = get_conn()
    try:
        return [dict(r) for r in conn.execute(sql, params).fetchall()]
    finally: conn.close()

def _sq_run(sql, params=()):
    conn = get_conn()
    try:
        conn.execute(sql, params)
        conn.commit()
    finally: conn.close()


# ─── Public API ───────────────────────────────────────────────────

def upsert_influencer(data: dict) -> str:
    now = time.time()
    pk = str(data.get("pk", ""))
    new_tag = data.get("hashtag", "")

    if _USE_SUPABASE:
        existing = _sb_get(T_INF, {"pk": f"eq.{pk}", "select": "pk,hashtags"})
        if existing:
            old_tags = set(t.strip() for t in (existing[0].get("hashtags") or "").split(",") if t.strip())
            if new_tag: old_tags.add(new_tag)
            _sb_patch(T_INF, f"?pk=eq.{pk}", {
                "username": data.get("username"), "full_name": data.get("full_name"),
                "biography": data.get("biography"), "follower_count": data.get("follower_count",0),
                "following_count": data.get("following_count",0), "media_count": data.get("media_count",0),
                "hashtags": ",".join(old_tags), "updated_at": now,
            })
            return "updated"
        else:
            _sb_post(T_INF, {
                "pk": pk, "username": data.get("username"), "full_name": data.get("full_name"),
                "biography": data.get("biography"), "follower_count": data.get("follower_count",0),
                "following_count": data.get("following_count",0), "media_count": data.get("media_count",0),
                "is_private": int(data.get("is_private",False)), "is_verified": int(data.get("is_verified",False)),
                "is_business": int(data.get("is_business",False)), "category": data.get("category"),
                "external_url": data.get("external_url"), "profile_pic_url": data.get("profile_pic_url"),
                "hashtags": new_tag, "created_at": now, "updated_at": now,
            })
            return "new"
    else:
        conn = get_conn()
        try:
            existing = conn.execute(f"SELECT pk,hashtags FROM {T_INF} WHERE pk=?", (pk,)).fetchone()
            if existing:
                old_tags = set(t.strip() for t in (existing["hashtags"] or "").split(",") if t.strip())
                if new_tag: old_tags.add(new_tag)
                conn.execute(f"""UPDATE {T_INF} SET username=?,full_name=?,biography=?,
                    follower_count=?,following_count=?,media_count=?,
                    is_private=?,is_verified=?,is_business=?,category=?,
                    public_email=?,public_phone=?,external_url=?,profile_pic_url=?,
                    hashtags=?,updated_at=? WHERE pk=?""", (
                    data.get("username"),data.get("full_name"),data.get("biography"),
                    data.get("follower_count",0),data.get("following_count",0),data.get("media_count",0),
                    int(data.get("is_private",False)),int(data.get("is_verified",False)),int(data.get("is_business",False)),
                    data.get("category"),data.get("public_email"),data.get("public_phone"),
                    data.get("external_url"),data.get("profile_pic_url"),",".join(old_tags),now,pk
                ))
                result = "updated"
            else:
                conn.execute(f"""INSERT INTO {T_INF}
                    (pk,username,full_name,biography,follower_count,following_count,media_count,
                     is_private,is_verified,is_business,category,public_email,public_phone,
                     external_url,profile_pic_url,hashtags,created_at,updated_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                    pk,data.get("username"),data.get("full_name"),data.get("biography"),
                    data.get("follower_count",0),data.get("following_count",0),data.get("media_count",0),
                    int(data.get("is_private",False)),int(data.get("is_verified",False)),int(data.get("is_business",False)),
                    data.get("category"),data.get("public_email"),data.get("public_phone"),
                    data.get("external_url"),data.get("profile_pic_url"),new_tag,now,now
                ))
                result = "new"
            conn.commit()
            return result
        finally: conn.close()


def update_influencer_stats(pk: str, stats: dict):
    now = time.time()
    payload = {
        "avg_likes": stats.get("avg_likes",0), "avg_comments": stats.get("avg_comments",0),
        "engagement_rate": stats.get("engagement_rate",0), "avg_reel_views": stats.get("avg_reel_views",0),
        "avg_feed_likes": stats.get("avg_feed_likes",0), "avg_feed_comments": stats.get("avg_feed_comments",0),
        "avg_reel_likes": stats.get("avg_reel_likes",0), "avg_reel_comments": stats.get("avg_reel_comments",0),
        "reel_count": stats.get("reel_count",0), "feed_count": stats.get("feed_count",0),
        "upload_frequency": stats.get("upload_frequency",""), "active_hours": stats.get("active_hours",""),
        "avg_posting_interval": stats.get("avg_posting_interval",0), "last_post_date": stats.get("last_post_date",""),
        "reels_ratio": stats.get("reels_ratio",0), "sponsored_ratio": stats.get("sponsored_ratio",0),
        "top_posts_likes": json.dumps(stats.get("top_posts_likes",[]), ensure_ascii=False),
        "top_posts_comments": json.dumps(stats.get("top_posts_comments",[]), ensure_ascii=False),
        "top_reels_views": json.dumps(stats.get("top_reels_views",[]), ensure_ascii=False),
        "profile_pic_local": stats.get("profile_pic_local",""), "stats_updated_at": now,
    }
    if _USE_SUPABASE:
        _sb_patch(T_INF, f"?pk=eq.{pk}", payload)
    else:
        sets = ",".join(f"{k}=?" for k in payload)
        _sq_run(f"UPDATE {T_INF} SET {sets} WHERE pk=?", list(payload.values()) + [pk])


def upsert_post(data: dict):
    now = time.time()
    if _USE_SUPABASE:
        existing = _sb_get(T_POST, {"post_id": f"eq.{data['post_id']}", "select": "id"})
        if existing:
            _sb_patch(T_POST, f"?post_id=eq.{data['post_id']}",
                     {"likes": data.get("likes",0), "comments": data.get("comments",0),
                      "views": data.get("views",0), "crawled_at": now})
        else:
            _sb_post(T_POST, {**{k: data.get(k) for k in [
                "influencer_pk","post_id","post_url","post_type","likes","comments","views",
                "caption","hashtags_used","is_sponsored","thumbnail_url","thumbnail_local","taken_at"
            ]}, "crawled_at": now})
    else:
        conn = get_conn()
        try:
            ex = conn.execute(f"SELECT id FROM {T_POST} WHERE post_id=?", (data["post_id"],)).fetchone()
            if ex:
                conn.execute(f"UPDATE {T_POST} SET likes=?,comments=?,views=?,crawled_at=? WHERE post_id=?",
                           (data.get("likes",0),data.get("comments",0),data.get("views",0),now,data["post_id"]))
            else:
                conn.execute(f"""INSERT INTO {T_POST}
                    (influencer_pk,post_id,post_url,post_type,likes,comments,views,
                     caption,hashtags_used,is_sponsored,thumbnail_url,thumbnail_local,taken_at,crawled_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""", (
                    data.get("influencer_pk"),data.get("post_id"),data.get("post_url"),data.get("post_type"),
                    data.get("likes",0),data.get("comments",0),data.get("views",0),
                    data.get("caption",""),data.get("hashtags_used",""),data.get("is_sponsored",0),
                    data.get("thumbnail_url",""),data.get("thumbnail_local",""),data.get("taken_at"),now
                ))
            conn.commit()
        finally: conn.close()


def get_manual(pk: str) -> dict:
    if _USE_SUPABASE:
        r = _sb_get(T_MAN, {"pk": f"eq.{pk}"})
        return r[0] if r else {}
    return _sq_one(f"SELECT * FROM {T_MAN} WHERE pk=?", (pk,)) or {}


def save_manual(pk: str, data: dict):
    now = time.time()
    if _USE_SUPABASE:
        existing = _sb_get(T_MAN, {"pk": f"eq.{pk}", "select": "pk"})
        if existing:
            _sb_patch(T_MAN, f"?pk=eq.{pk}", {**data, "updated_at": now})
        else:
            _sb_post(T_MAN, {**data, "pk": pk, "updated_at": now})
    else:
        conn = get_conn()
        try:
            ex = conn.execute(f"SELECT pk FROM {T_MAN} WHERE pk=?", (pk,)).fetchone()
            if ex:
                sets = ",".join(f"{k}=?" for k in data)
                conn.execute(f"UPDATE {T_MAN} SET {sets},updated_at=? WHERE pk=?",
                           list(data.values()) + [now, pk])
            else:
                d = {**data, "pk": pk, "updated_at": now}
                cols = ",".join(d.keys())
                phs = ",".join(["?"]*len(d))
                conn.execute(f"INSERT INTO {T_MAN} ({cols}) VALUES ({phs})", list(d.values()))
            conn.commit()
        finally: conn.close()


def get_influencers(keyword="", min_f=None, max_f=None,
                    only_verified=False, exclude_private=False,
                    hashtag_filter="", main_category="",
                    can_live=False, only_approved=False,
                    has_pet=False, is_married=False, has_kids=False, has_car=False,
                    is_visual=False,
                    sort="follower_count", order="desc",
                    page=1, per_page=50):
    if _USE_SUPABASE:
        return _get_influencers_sb(keyword, min_f, max_f, only_verified, exclude_private,
                                    hashtag_filter, main_category, can_live, only_approved,
                                    has_pet, is_married, has_kids, has_car, is_visual,
                                    sort, order, page, per_page)
    # SQLite
    conditions, params = [], []
    if keyword:
        conditions.append("(i.username LIKE ? OR i.full_name LIKE ? OR i.biography LIKE ?)")
        params += [f"%{keyword}%"]*3
    if hashtag_filter:
        conditions.append("i.hashtags LIKE ?"); params.append(f"%{hashtag_filter}%")
    if min_f is not None: conditions.append("i.follower_count >= ?"); params.append(min_f)
    if max_f is not None: conditions.append("i.follower_count <= ?"); params.append(max_f)
    if only_verified: conditions.append("i.is_verified=1")
    if exclude_private: conditions.append("i.is_private=0")
    if main_category: conditions.append("m.main_category=?"); params.append(main_category)
    if can_live: conditions.append("m.can_live=1")
    if only_approved: conditions.append("m.is_approved=1")
    if has_pet: conditions.append("m.has_pet=1")
    if is_married: conditions.append("m.is_married=1")
    if has_kids: conditions.append("m.has_kids=1")
    if has_car: conditions.append("m.has_car=1")
    if is_visual: conditions.append("m.is_visual=1")
    where = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    valid_sorts = {"follower_count","engagement_rate","avg_reel_views","avg_likes",
                   "media_count","updated_at","stats_updated_at","username"}
    sort = sort if sort in valid_sorts else "follower_count"
    order_sql = f"ORDER BY i.{sort} {'DESC' if order=='desc' else 'ASC'}"
    offset = (page-1)*per_page
    join = f"LEFT JOIN {T_MAN} m ON i.pk=m.pk"
    conn = get_conn()
    try:
        total = conn.execute(f"SELECT COUNT(*) FROM {T_INF} i {join} {where}", params).fetchone()[0]
        rows = conn.execute(f"""SELECT i.*,
            COALESCE(m.can_live,0) as can_live, COALESCE(m.is_approved,0) as is_approved,
            COALESCE(m.main_category,'') as manual_category, COALESCE(m.quality_score,0) as quality_score,
            COALESCE(m.contact_name,'') as contact_name, COALESCE(m.collab_types,'') as collab_types,
            COALESCE(m.live_platforms,'') as live_platforms, COALESCE(m.feed_price,0) as feed_price,
            COALESCE(m.reel_price,0) as reel_price, COALESCE(m.has_pet,0) as has_pet,
            COALESCE(m.is_married,0) as is_married, COALESCE(m.has_kids,0) as has_kids,
            COALESCE(m.has_car,0) as has_car, COALESCE(m.is_visual,0) as is_visual,
            COALESCE(m.face_exposed,0) as face_exposed
            FROM {T_INF} i {join} {where} {order_sql} LIMIT ? OFFSET ?""",
            params + [per_page, offset]).fetchall()
        return total, [dict(r) for r in rows]
    finally: conn.close()


def _get_influencers_sb(keyword, min_f, max_f, only_verified, exclude_private,
                         hashtag_filter, main_category, can_live, only_approved,
                         has_pet, is_married, has_kids, has_car, is_visual,
                         sort, order, page, per_page):
    """Supabase PostgREST 필터링 - 조인이 없으므로 2단계 조회"""
    # Step 1: manual 필터로 pk 목록 가져오기
    man_params = {"select": "pk"}
    man_filters = []
    if can_live: man_params["can_live"] = "eq.1"
    if only_approved: man_params["is_approved"] = "eq.1"
    if main_category: man_params["main_category"] = f"eq.{main_category}"
    if has_pet: man_params["has_pet"] = "eq.1"
    if is_married: man_params["is_married"] = "eq.1"
    if has_kids: man_params["has_kids"] = "eq.1"
    if has_car: man_params["has_car"] = "eq.1"
    if is_visual: man_params["is_visual"] = "eq.1"

    need_manual_filter = any([can_live, only_approved, main_category, has_pet,
                               is_married, has_kids, has_car, is_visual])

    # Step 2: influencer 필터
    inf_params = {
        "select": "*",
        "order": f"{sort}.{'desc' if order=='desc' else 'asc'}",
        "limit": str(per_page),
        "offset": str((page-1)*per_page),
    }
    if keyword:
        inf_params["or"] = f"(username.ilike.*{keyword}*,full_name.ilike.*{keyword}*)"
    if hashtag_filter:
        inf_params["hashtags"] = f"ilike.*{hashtag_filter}*"
    if min_f is not None: inf_params["follower_count"] = f"gte.{min_f}"
    if max_f is not None:
        if "follower_count" in inf_params:
            inf_params["follower_count"] += f"&follower_count=lte.{max_f}"
        else:
            inf_params["follower_count"] = f"lte.{max_f}"
    if only_verified: inf_params["is_verified"] = "eq.1"
    if exclude_private: inf_params["is_private"] = "eq.0"

    if need_manual_filter:
        man_rows = _sb_get(T_MAN, man_params)
        pks = [r["pk"] for r in man_rows] if man_rows else []
        if not pks:
            return 0, []
        inf_params["pk"] = f"in.({','.join(pks)})"

    headers = _sb_headers()
    headers["Prefer"] = "count=exact"
    r = _req.get(_sb_url(T_INF), headers=headers, params=inf_params)
    rows = r.json() if isinstance(r.json(), list) else []
    total_str = r.headers.get("Content-Range","0/0").split("/")[-1]
    total = int(total_str) if total_str.isdigit() else len(rows)

    # manual 데이터 병합
    if rows:
        pks_in = [row["pk"] for row in rows]
        man_data = _sb_get(T_MAN, {"pk": f"in.({','.join(pks_in)})", "select": "*"})
        man_map = {m["pk"]: m for m in (man_data or [])}
        for row in rows:
            m = man_map.get(row["pk"], {})
            row["can_live"] = m.get("can_live", 0)
            row["is_approved"] = m.get("is_approved", 0)
            row["manual_category"] = m.get("main_category", "")
            row["quality_score"] = m.get("quality_score", 0)
            row["contact_name"] = m.get("contact_name", "")
            row["collab_types"] = m.get("collab_types", "")
            row["live_platforms"] = m.get("live_platforms", "")
            row["feed_price"] = m.get("feed_price", 0)
            row["reel_price"] = m.get("reel_price", 0)
            row["has_pet"] = m.get("has_pet", 0)
            row["is_married"] = m.get("is_married", 0)
            row["has_kids"] = m.get("has_kids", 0)
            row["has_car"] = m.get("has_car", 0)
            row["is_visual"] = m.get("is_visual", 0)
            row["face_exposed"] = m.get("face_exposed", 0)

    return total, rows


def get_influencer(pk: str) -> dict:
    if _USE_SUPABASE:
        r = _sb_get(T_INF, {"pk": f"eq.{pk}"})
        return r[0] if r else {}
    return _sq_one(f"SELECT * FROM {T_INF} WHERE pk=?", (pk,)) or {}


def get_influencer_posts(pk: str) -> list:
    if _USE_SUPABASE:
        return _sb_get(T_POST, {"influencer_pk": f"eq.{pk}", "order": "taken_at.desc"}) or []
    return _sq_all(f"SELECT * FROM {T_POST} WHERE influencer_pk=? ORDER BY taken_at DESC", (pk,))


def get_stats():
    if _USE_SUPABASE:
        def cnt(table, params=None):
            headers = _sb_headers()
            headers["Prefer"] = "count=exact"
            r = _req.get(_sb_url(table), headers=headers, params={**(params or {}), "select": "pk", "limit": "1"})
            s = r.headers.get("Content-Range","0/0").split("/")[-1]
            return int(s) if s.isdigit() else 0
        return {
            "total":      cnt(T_INF),
            "verified":   cnt(T_INF, {"is_verified": "eq.1"}),
            "business":   cnt(T_INF, {"is_business": "eq.1"}),
            "hashtags":   cnt(T_HASH),
            "with_stats": cnt(T_INF, {"stats_updated_at": "gt.0"}),
            "live_ok":    cnt(T_MAN, {"can_live": "eq.1"}),
            "approved":   cnt(T_MAN, {"is_approved": "eq.1"}),
            "has_url":    cnt(T_INF, {"external_url": "not.is.null"}),
            "linktree":   0,
            "has_tiktok": cnt(T_MAN, {"tiktok_url": "not.eq."}),
            "has_youtube":cnt(T_MAN, {"youtube_url": "not.eq."}),
            "has_kids":   cnt(T_MAN, {"has_kids": "eq.1"}),
            "has_pet":    cnt(T_MAN, {"has_pet": "eq.1"}),
            "is_married": cnt(T_MAN, {"is_married": "eq.1"}),
            "is_visual":  cnt(T_MAN, {"is_visual": "eq.1"}),
        }
    conn = get_conn()
    try:
        def cnt(sql):
            return conn.execute(sql).fetchone()[0]
        return {
            "total":      cnt(f"SELECT COUNT(*) FROM {T_INF}"),
            "verified":   cnt(f"SELECT COUNT(*) FROM {T_INF} WHERE is_verified=1"),
            "business":   cnt(f"SELECT COUNT(*) FROM {T_INF} WHERE is_business=1"),
            "hashtags":   cnt(f"SELECT COUNT(*) FROM {T_HASH}"),
            "with_stats": cnt(f"SELECT COUNT(*) FROM {T_INF} WHERE stats_updated_at>0"),
            "live_ok":    cnt(f"SELECT COUNT(*) FROM {T_MAN} WHERE can_live=1"),
            "approved":   cnt(f"SELECT COUNT(*) FROM {T_MAN} WHERE is_approved=1"),
            "has_url":    cnt(f"SELECT COUNT(*) FROM {T_INF} WHERE external_url IS NOT NULL AND external_url!=''"),
            "linktree":   cnt(f"SELECT COUNT(*) FROM {T_INF} WHERE external_url LIKE '%linktree%' OR external_url LIKE '%linktr.ee%'"),
            "has_tiktok": cnt(f"SELECT COUNT(*) FROM {T_MAN} WHERE tiktok_url!='' AND tiktok_url IS NOT NULL"),
            "has_youtube":cnt(f"SELECT COUNT(*) FROM {T_MAN} WHERE youtube_url!='' AND youtube_url IS NOT NULL"),
            "has_kids":   cnt(f"SELECT COUNT(*) FROM {T_MAN} WHERE has_kids=1"),
            "has_pet":    cnt(f"SELECT COUNT(*) FROM {T_MAN} WHERE has_pet=1"),
            "is_married": cnt(f"SELECT COUNT(*) FROM {T_MAN} WHERE is_married=1"),
            "is_visual":  cnt(f"SELECT COUNT(*) FROM {T_MAN} WHERE is_visual=1"),
        }
    finally: conn.close()


def get_public_stats():
    if _USE_SUPABASE:
        def cnt(table, params=None):
            headers = _sb_headers()
            headers["Prefer"] = "count=exact"
            r = _req.get(_sb_url(table), headers=headers, params={**(params or {}), "select": "pk", "limit": "1"})
            s = r.headers.get("Content-Range","0/0").split("/")[-1]
            return int(s) if s.isdigit() else 0
        r = _req.get(_sb_url(T_INF), headers=_sb_headers(),
                    params={"select": "follower_count"})
        tf = sum(row.get("follower_count",0) or 0 for row in (r.json() or []))
        return {"total": cnt(T_INF), "verified": cnt(T_INF, {"is_verified":"eq.1"}),
                "hashtags": cnt(T_HASH), "total_followers": tf}
    conn = get_conn()
    try:
        total = conn.execute(f"SELECT COUNT(*) FROM {T_INF}").fetchone()[0]
        verified = conn.execute(f"SELECT COUNT(*) FROM {T_INF} WHERE is_verified=1").fetchone()[0]
        htags = conn.execute(f"SELECT COUNT(*) FROM {T_HASH}").fetchone()[0]
        tf = conn.execute(f"SELECT SUM(follower_count) FROM {T_INF}").fetchone()[0] or 0
        return {"total": total, "verified": verified, "hashtags": htags, "total_followers": tf}
    finally: conn.close()


def get_public_influencers(page=1, per_page=30, sort="follower_count"):
    if _USE_SUPABASE:
        headers = _sb_headers()
        headers["Prefer"] = "count=exact"
        r = _req.get(_sb_url(T_INF), headers=headers, params={
            "select": "pk,username,full_name,follower_count,is_verified,is_business,category,profile_pic_local,engagement_rate,avg_reel_views,hashtags",
            "order": f"{sort}.desc",
            "limit": str(per_page),
            "offset": str((page-1)*per_page),
        })
        rows = r.json() if isinstance(r.json(), list) else []
        s = r.headers.get("Content-Range","0/0").split("/")[-1]
        total = int(s) if s.isdigit() else len(rows)
        return total, rows
    conn = get_conn()
    offset = (page-1)*per_page
    try:
        total = conn.execute(f"SELECT COUNT(*) FROM {T_INF}").fetchone()[0]
        rows = conn.execute(f"""SELECT pk,username,full_name,follower_count,is_verified,is_business,
            category,profile_pic_local,engagement_rate,avg_reel_views,hashtags
            FROM {T_INF} ORDER BY {sort} DESC LIMIT ? OFFSET ?""", [per_page, offset]).fetchall()
        return total, [dict(r) for r in rows]
    finally: conn.close()


def get_advertisers():
    if _USE_SUPABASE:
        return _sb_get(T_ADV, {"order": "created_at.desc"}) or []
    return _sq_all(f"SELECT * FROM {T_ADV} ORDER BY created_at DESC")


def get_refresh_status():
    if _USE_SUPABASE:
        r = _sb_get(T_RJOB, {"order": "started_at.desc", "limit": "1"})
        return r[0] if r else {}
    return _sq_one(f"SELECT * FROM {T_RJOB} ORDER BY started_at DESC LIMIT 1") or {}
