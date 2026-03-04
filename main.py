from fastapi import FastAPI, Request, Form, Cookie, Query, BackgroundTasks, HTTPException, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse, Response, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from typing import Optional, List
import uuid, time, json, os, bcrypt, logging
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

from database import (init_db, get_conn, get_influencers, get_influencer, get_influencer_posts,
                      get_stats, get_public_stats, get_public_influencers,
                      get_manual, save_manual, get_advertisers, get_refresh_status,
                      update_influencer_stats, get_advertiser_by_username,
                      add_advertiser as db_add_advertiser, delete_advertiser as db_delete_advertiser,
                      update_advertiser_plan,
                      get_hashtags, get_collect_jobs, add_hashtag as db_add_hashtag,
                      delete_hashtag as db_delete_hashtag, update_hashtag_status,
                      add_collect_job, update_collect_job,
                      get_accounts, upsert_account, delete_account, update_account_status,
                      reset_account_errors)

app = FastAPI()

# Supabase 모드(프로덕션)이면 /tmp 사용 (Vercel 읽기전용 FS)
_IS_PROD = bool(os.environ.get("SUPABASE_KEY"))
_LOCAL_DATA = os.path.join(os.path.dirname(__file__), "data")
DATA_DIR = "/tmp/data" if _IS_PROD else _LOCAL_DATA
try:
    os.makedirs(os.path.join(DATA_DIR, "profile_pics"), exist_ok=True)
    os.makedirs(os.path.join(DATA_DIR, "posts"), exist_ok=True)
except OSError:
    DATA_DIR = "/tmp/data"
    os.makedirs(os.path.join(DATA_DIR, "profile_pics"), exist_ok=True)
    os.makedirs(os.path.join(DATA_DIR, "posts"), exist_ok=True)

if not _IS_PROD and os.path.isdir(DATA_DIR):
    app.mount("/data", StaticFiles(directory=DATA_DIR), name="data")
_STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")
os.makedirs(_STATIC_DIR, exist_ok=True)
app.mount("/static", StaticFiles(directory=_STATIC_DIR), name="static")

templates = Jinja2Templates(directory=os.path.join(os.path.dirname(__file__), "templates"))
templates.env.filters["dt"]    = lambda t: datetime.fromtimestamp(float(t)).strftime("%m/%d %H:%M") if t else "-"
templates.env.filters["comma"] = lambda n: f"{int(n or 0):,}"
templates.env.filters["fmtn"]  = lambda n: (f"{int(n or 0)//10000}만" if int(n or 0) >= 10000 else f"{int(n or 0):,}") if n else "0"

def _safe_cd(fname: str) -> str:
    """Content-Disposition 헤더용 RFC 5987 인코딩"""
    from urllib.parse import quote
    ascii_name = fname.encode("ascii", "ignore").decode("ascii") or "download"
    encoded = quote(fname, safe="")
    return f"attachment; filename=\"{ascii_name}\"; filename*=UTF-8''{encoded}"

# ── 인증 (관리자) - JWT 기반 (Vercel 서버리스 호환) ──────────
from jose import jwt as _jwt
JWT_SECRET = os.getenv("SECRET_KEY", "instafinder-secret-key-2026-supers")
JWT_ALG = "HS256"
JWT_EXP_HOURS = 24 * 7  # 7일

ADMIN_PW_HASH = bcrypt.hashpw(
    os.getenv("ADMIN_PASSWORD", "admin").encode(), bcrypt.gensalt()
)
ADMIN_USER = os.getenv("ADMIN_USERNAME", "admin")

def _make_jwt(payload: dict, hours: int = JWT_EXP_HOURS) -> str:
    import datetime
    data = dict(payload)
    data["exp"] = datetime.datetime.utcnow() + datetime.timedelta(hours=hours)
    return _jwt.encode(data, JWT_SECRET, algorithm=JWT_ALG)

def _decode_jwt(token: str) -> dict | None:
    try:
        return _jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALG])
    except Exception:
        return None

# Fallback in-memory sessions for local dev (not used in prod)
sessions: dict = {}
adv_sessions: dict = {}

INSTA_CFG = {
    "username": os.getenv("INSTA_USERNAME", "jannat160304"),
    "password": os.getenv("INSTA_PASSWORD", "jug@575"),
    "totp":     os.getenv("INSTA_TOTP", "YQ754N2HTC7IDAT5BQIPNA5RHQA75JFY"),
    "proxy_host": os.getenv("INSTA_PROXY_HOST", ""),
    "proxy_port": os.getenv("INSTA_PROXY_PORT", ""),
    "proxy_user": os.getenv("INSTA_PROXY_USER", ""),
    "proxy_pass": os.getenv("INSTA_PROXY_PASS", ""),
}

# 구독 플랜 정의
PLANS = {
    "free":       {"name": "무료",       "per_hashtag": 100,   "daily_limit": 500,  "max_hashtags": 5,  "price_1m": 0,    "price_3m": 0},
    "starter":    {"name": "스타터",     "per_hashtag": 1000,  "daily_limit": 5000, "max_hashtags": 10, "price_1m": 10000, "price_3m": 27000},
    "pro":        {"name": "프로",       "per_hashtag": 5000,  "daily_limit": 20000,"max_hashtags": 30, "price_1m": 30000, "price_3m": 81000},
    "enterprise": {"name": "엔터프라이즈","per_hashtag": 10000, "daily_limit": 50000,"max_hashtags": 100,"price_1m": 50000, "price_3m": 135000},
}


def get_user(session_id: Optional[str] = None):
    """session_id는 실제로 JWT token (cookie name 유지)"""
    if not session_id:
        return None
    payload = _decode_jwt(session_id)
    if payload and payload.get("role") == "admin":
        return {"username": payload.get("username", "admin")}
    # Fallback: in-memory (local dev)
    return sessions.get(session_id)

def get_adv_user(session_id: Optional[str] = None):
    """adv_session_id는 JWT token"""
    if not session_id:
        return None
    payload = _decode_jwt(session_id)
    if payload and payload.get("role") == "advertiser":
        adv_id = payload.get("adv_id")
        if adv_id:
            # Supabase에서 최신 정보 가져오기
            from database import _USE_SUPABASE
            if _USE_SUPABASE:
                rows = __import__("database")._sb_get(
                    "insta_advertiser_accounts",
                    {"id": f"eq.{adv_id}", "limit": "1"}
                )
                return rows[0] if rows else None
        return payload
    return adv_sessions.get(session_id)

def require_admin(session_id: Optional[str]):
    user = get_user(session_id)
    if not user:
        raise HTTPException(status_code=303, headers={"Location": "/login"})
    return user


@app.on_event("startup")
def startup():
    init_db()


# ═══════════════════════════════════════════════════════
# 공개 메인 페이지 (로그인 없이)
# ═══════════════════════════════════════════════════════

@app.get("/", response_class=HTMLResponse)
def public_home(
    request: Request,
    page: int = Query(1, gt=0),
    sort: str = "follower_count",
    session_id: Optional[str] = Cookie(default=None)
):
    stats = get_public_stats()
    total, rows = get_public_influencers(page=page, per_page=30, sort=sort)
    total_pages = max(1, (total + 29) // 30)
    user = get_user(session_id)
    return templates.TemplateResponse("home.html", {
        "request": request, "stats": stats, "rows": rows,
        "total": total, "total_pages": total_pages, "page": page,
        "sort": sort, "user": user,
    })


# ═══════════════════════════════════════════════════════
# 관리자 로그인
# ═══════════════════════════════════════════════════════

@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.post("/login")
def login(request: Request, username: str = Form(...), password: str = Form(...)):
    if username == ADMIN_USER and bcrypt.checkpw(password.encode(), ADMIN_PW_HASH):
        token = _make_jwt({"role": "admin", "username": username})
        res = RedirectResponse("/influencers", status_code=302)
        res.set_cookie("session_id", token, httponly=True, max_age=604800)
        return res
    return templates.TemplateResponse("login.html", {
        "request": request, "error": "아이디 또는 비밀번호가 올바르지 않습니다."
    }, status_code=400)

@app.get("/logout")
def logout(session_id: Optional[str] = Cookie(default=None)):
    if session_id in sessions: del sessions[session_id]
    res = RedirectResponse("/login", 302)
    res.delete_cookie("session_id")
    return res


# ═══════════════════════════════════════════════════════
# 인플루언서 목록 (관리자 전용)
# ═══════════════════════════════════════════════════════

@app.get("/influencers", response_class=HTMLResponse)
def influencers(
    request: Request,
    q: str = "",
    hashtag: str = "",
    min_f: Optional[int] = None,
    max_f: Optional[int] = None,
    verified: int = 0,
    public_only: int = 0,
    main_category: str = "",
    can_live: int = 0,
    only_approved: int = 0,
    has_pet: int = 0,
    is_married: int = 0,
    has_kids: int = 0,
    has_car: int = 0,
    is_visual: int = 0,
    sort: str = "follower_count",
    order: str = "desc",
    page: int = Query(1, gt=0),
    per_page: int = Query(50, gt=0),
    session_id: Optional[str] = Cookie(default=None)
):
    user = get_user(session_id)
    if not user:
        return RedirectResponse(f"/login?next=/influencers", 302)

    total, rows = get_influencers(
        keyword=q, hashtag_filter=hashtag,
        min_f=min_f, max_f=max_f,
        only_verified=bool(verified), exclude_private=bool(public_only),
        main_category=main_category,
        can_live=bool(can_live), only_approved=bool(only_approved),
        has_pet=bool(has_pet), is_married=bool(is_married),
        has_kids=bool(has_kids), has_car=bool(has_car),
        is_visual=bool(is_visual),
        sort=sort, order=order, page=page, per_page=per_page
    )
    stats = get_stats()
    refresh = get_refresh_status()
    total_pages = max(1, (total + per_page - 1) // per_page)
    # 검색어가 있고 결과가 없으면 즉시 수집 제안
    instant_collect_target = ""
    if q and total == 0 and re.match(r'^[a-zA-Z0-9._]+$', q):
        instant_collect_target = q
    return templates.TemplateResponse("influencers.html", {
        "request": request, "user": user,
        "rows": rows, "total": total, "total_pages": total_pages,
        "page": page, "per_page": per_page,
        "q": q, "hashtag": hashtag,
        "min_f": min_f, "max_f": max_f,
        "verified": verified, "public_only": public_only,
        "main_category": main_category,
        "can_live": can_live, "only_approved": only_approved,
        "has_pet": has_pet, "is_married": is_married, "has_kids": has_kids, "has_car": has_car,
        "is_visual": is_visual,
        "sort": sort, "order": order,
        "stats": stats, "refresh": refresh,
        "instant_collect_target": instant_collect_target,
    })


# ═══════════════════════════════════════════════════════
# 즉시 수집 (계정명으로 실시간 크롤링)
# ═══════════════════════════════════════════════════════

@app.post("/influencers/instant-collect")
async def instant_collect(
    background_tasks: BackgroundTasks,
    username: str = Form(...),
    session_id: Optional[str] = Cookie(default=None)
):
    user = get_user(session_id)
    if not user:
        return JSONResponse({"error": "로그인 필요"}, 401)
    username = username.strip().lstrip("@")
    if not re.match(r'^[a-zA-Z0-9._]+$', username):
        return JSONResponse({"error": "올바르지 않은 계정명"}, 400)

    from crawler import crawl_single_user
    # 백그라운드로 실행
    background_tasks.add_task(_run_instant_collect, username)
    return JSONResponse({"ok": True, "message": f"@{username} 수집 시작"})


def _run_instant_collect(username: str):
    from crawler import crawl_single_user
    crawl_single_user(username)


# ═══════════════════════════════════════════════════════
# 인플루언서 상세
# ═══════════════════════════════════════════════════════

@app.get("/influencers/{pk}", response_class=HTMLResponse)
def influencer_detail(
    pk: str, request: Request,
    session_id: Optional[str] = Cookie(default=None)
):
    user = get_user(session_id)
    if not user:
        return RedirectResponse(f"/login", 302)
    inf = get_influencer(pk)
    if not inf:
        raise HTTPException(404, "인플루언서를 찾을 수 없습니다")
    manual = get_manual(pk)
    posts = get_influencer_posts(pk)

    # JSON 파싱
    for key in ["top_posts_likes", "top_posts_comments", "top_reels_views"]:
        try:
            inf[key] = json.loads(inf.get(key) or "[]")
        except:
            inf[key] = []

    # top_hashtags 파싱 (구형: ["뷰티",...] 또는 신형: [{"tag":"뷰티","count":5},...])
    try:
        raw = json.loads(inf.get("top_hashtags") or "[]")
        if raw and isinstance(raw[0], str):
            inf["top_hashtags"] = [{"tag": t, "count": 1} for t in raw]
        else:
            inf["top_hashtags"] = raw
    except:
        inf["top_hashtags"] = []

    # posts DB에 있으면 top posts도 posts 기반으로 재계산
    if posts:
        reels_p = [p for p in posts if p.get("post_type") == "reel"]
        feeds_p = [p for p in posts if p.get("post_type") != "reel"]
        reels_p.sort(key=lambda x: x.get("views", 0) or 0, reverse=True)
        feeds_p.sort(key=lambda x: x.get("likes", 0) or 0, reverse=True)
        if reels_p:
            inf["top_reels_views"] = [
                {"url": p["post_url"], "views": p.get("views", 0),
                 "thumbnail": p.get("thumbnail_url", "") or p.get("thumbnail_local", "")}
                for p in reels_p[:5]
            ]
        if feeds_p:
            inf["top_posts_likes"] = [
                {"url": p["post_url"], "likes": p.get("likes", 0),
                 "thumbnail": p.get("thumbnail_url", "") or p.get("thumbnail_local", "")}
                for p in feeds_p[:5]
            ]

    return templates.TemplateResponse("influencer_detail.html", {
        "request": request, "user": user,
        "inf": inf, "manual": manual, "posts": posts,
    })


# ═══════════════════════════════════════════════════════
# 수동 입력 편집
# ═══════════════════════════════════════════════════════

@app.get("/influencers/{pk}/edit", response_class=HTMLResponse)
def edit_page(pk: str, request: Request, session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id)
    if not user: return RedirectResponse("/login", 302)
    inf = get_influencer(pk)
    if not inf: raise HTTPException(404)
    manual = get_manual(pk)
    return templates.TemplateResponse("influencer_edit.html", {
        "request": request, "user": user, "inf": inf, "manual": manual,
    })

@app.post("/influencers/{pk}/edit")
def edit_save(
    pk: str,
    session_id: Optional[str] = Cookie(default=None),
    contact_name:  str = Form(default=""),
    contact_kakao: str = Form(default=""),
    contact_line:  str = Form(default=""),
    contact_email: str = Form(default=""),
    contact_phone: str = Form(default=""),
    can_live:      int = Form(default=0),
    live_platforms: str = Form(default=""),
    live_price:    int = Form(default=0),
    feed_price:    int = Form(default=0),
    reel_price:    int = Form(default=0),
    story_price:   int = Form(default=0),
    bundle_price:  int = Form(default=0),
    main_category: str = Form(default=""),
    sub_categories: str = Form(default=""),
    target_gender: str = Form(default=""),
    target_age:    str = Form(default=""),
    target_region: str = Form(default=""),
    collab_types:  str = Form(default=""),
    past_brands:   str = Form(default=""),
    quality_score: int = Form(default=0),
    notes:         str = Form(default=""),
    is_approved:   int = Form(default=0),
    has_pet:       int = Form(default=0),
    is_married:    int = Form(default=0),
    has_kids:      int = Form(default=0),
    has_car:       int = Form(default=0),
    pet_type:      str = Form(default=""),
    kids_age:      str = Form(default=""),
    is_brand:      int = Form(default=0),
    is_visual:     int = Form(default=0),
    face_exposed:  int = Form(default=0),
    tiktok_url:    str = Form(default=""),
    youtube_url:   str = Form(default=""),
    facebook_url:  str = Form(default=""),
    threads_url:   str = Form(default=""),
    tiktok_followers:   int = Form(default=0),
    youtube_subscribers: int = Form(default=0),
):
    user = get_user(session_id)
    if not user: return RedirectResponse("/login", 302)
    save_manual(pk, {
        "contact_name": contact_name, "contact_kakao": contact_kakao,
        "contact_line": contact_line, "contact_email": contact_email,
        "contact_phone": contact_phone,
        "can_live": can_live, "live_platforms": live_platforms, "live_price": live_price,
        "feed_price": feed_price, "reel_price": reel_price,
        "story_price": story_price, "bundle_price": bundle_price,
        "main_category": main_category, "sub_categories": sub_categories,
        "target_gender": target_gender, "target_age": target_age, "target_region": target_region,
        "collab_types": collab_types, "past_brands": past_brands,
        "quality_score": quality_score, "notes": notes, "is_approved": is_approved,
        "approved_at": time.time() if is_approved else 0,
        "has_pet": has_pet, "is_married": is_married, "has_kids": has_kids, "has_car": has_car,
        "pet_type": pet_type, "kids_age": kids_age,
        "is_brand": is_brand, "is_visual": is_visual, "face_exposed": face_exposed,
        "tiktok_url": tiktok_url, "youtube_url": youtube_url,
        "facebook_url": facebook_url, "threads_url": threads_url,
        "tiktok_followers": tiktok_followers, "youtube_subscribers": youtube_subscribers,
    })
    return RedirectResponse(f"/influencers/{pk}?saved=1", 302)


# ═══════════════════════════════════════════════════════
# 단일 인플루언서 상세 수집 (관리자)
# ═══════════════════════════════════════════════════════

@app.post("/influencers/{pk}/refresh")
def refresh_one(pk: str, background_tasks: BackgroundTasks,
                session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id)
    if not user: return JSONResponse({"error": "인증 필요"}, 403)
    inf = get_influencer(pk)
    if not inf: return JSONResponse({"error": "없음"}, 404)

    def do_refresh():
        from crawler import get_client, crawl_user_detail
        try:
            cl = get_client(INSTA_CFG["username"], INSTA_CFG["password"], INSTA_CFG["totp"], INSTA_CFG.get("proxy_host",""), INSTA_CFG.get("proxy_port",""), INSTA_CFG.get("proxy_user",""), INSTA_CFG.get("proxy_pass",""))
            crawl_user_detail(cl, pk, inf["username"], inf.get("follower_count", 0))
        except Exception as e:
            log.error(f"단일 갱신 실패: {e}")

    background_tasks.add_task(do_refresh)
    return JSONResponse({"ok": True})


# ═══════════════════════════════════════════════════════
# 내보내기 (PDF / PPT / Excel)
# ═══════════════════════════════════════════════════════

def _get_inf_with_manual(pk):
    inf = get_influencer(pk)
    manual = get_manual(pk)
    for key in ["top_posts_likes", "top_posts_comments", "top_reels_views"]:
        try: inf[key] = json.loads(inf.get(key) or "[]")
        except: inf[key] = []
    return inf, manual


@app.get("/influencers/{pk}/export/pdf")
def export_single_pdf(pk: str, tpl: str = "scorecard",
                      session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id) or get_adv_user(session_id)
    if not user: return RedirectResponse("/login", 302)
    from export_pdf import export_single_pdf as _pdf
    inf, manual = _get_inf_with_manual(pk)
    data = _pdf(inf, manual)
    tpl_label = {"scorecard": "스코어카드", "detail": "상세리포트"}.get(tpl, "스코어카드")
    fname = f"{inf.get('username', pk)}_{tpl_label}.pdf"
    return Response(data, media_type="application/pdf",
                    headers={"Content-Disposition": _safe_cd(fname)})


@app.get("/influencers/{pk}/export/ppt")
def export_single_ppt(pk: str, tpl: str = "scorecard",
                      session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id) or get_adv_user(session_id)
    if not user: return RedirectResponse("/login", 302)
    from export_ppt import export_single_ppt as _ppt
    inf, manual = _get_inf_with_manual(pk)
    data = _ppt(inf, manual)
    tpl_label = {"scorecard": "스코어카드", "proposal": "제안서"}.get(tpl, "스코어카드")
    fname = f"{inf.get('username', pk)}_{tpl_label}.pptx"
    return Response(data,
                    media_type="application/vnd.openxmlformats-officedocument.presentationml.presentation",
                    headers={"Content-Disposition": _safe_cd(fname)})


@app.get("/export/pdf")
def export_bulk_pdf(
    request: Request,
    mode: str = "scorecard",  # scorecard | list
    q: str = "", hashtag: str = "",
    min_f: str = "", max_f: str = "",
    verified: int = 0, public_only: int = 0,
    main_category: str = "", can_live: int = 0, only_approved: int = 0,
    session_id: Optional[str] = Cookie(default=None)
):
    user = get_user(session_id) or get_adv_user(session_id)
    if not user: return RedirectResponse("/login", 302)

    min_fi = int(min_f) if min_f and min_f.strip().isdigit() else None
    max_fi = int(max_f) if max_f and max_f.strip().isdigit() else None

    _, rows = get_influencers(
        keyword=q, hashtag_filter=hashtag, min_f=min_fi, max_f=max_fi,
        only_verified=bool(verified), exclude_private=bool(public_only),
        main_category=main_category, can_live=bool(can_live), only_approved=bool(only_approved),
        sort="follower_count", order="desc", page=1, per_page=500
    )
    pairs = [(_get_inf_with_manual(r["pk"])) for r in rows]

    if mode == "list":
        from export_pdf import export_list_pdf
        data = export_list_pdf(pairs)
        fname = f"influencer_list_{datetime.now().strftime('%Y%m%d')}.pdf"
    else:
        from export_pdf import export_multi_pdf
        data = export_multi_pdf(pairs)
        fname = f"influencer_scorecards_{datetime.now().strftime('%Y%m%d')}.pdf"

    return Response(data, media_type="application/pdf",
                    headers={"Content-Disposition": _safe_cd(fname)})


@app.get("/export/ppt")
def export_bulk_ppt(
    request: Request,
    mode: str = "scorecard",  # scorecard | list
    q: str = "", hashtag: str = "",
    min_f: str = "", max_f: str = "",
    verified: int = 0, public_only: int = 0,
    main_category: str = "", can_live: int = 0, only_approved: int = 0,
    session_id: Optional[str] = Cookie(default=None)
):
    user = get_user(session_id) or get_adv_user(session_id)
    if not user: return RedirectResponse("/login", 302)

    min_fi = int(min_f) if min_f and min_f.strip().isdigit() else None
    max_fi = int(max_f) if max_f and max_f.strip().isdigit() else None

    _, rows = get_influencers(
        keyword=q, hashtag_filter=hashtag, min_f=min_fi, max_f=max_fi,
        only_verified=bool(verified), exclude_private=bool(public_only),
        main_category=main_category, can_live=bool(can_live), only_approved=bool(only_approved),
        sort="follower_count", order="desc", page=1, per_page=300
    )
    pairs = [(_get_inf_with_manual(r["pk"])) for r in rows]

    if mode == "list":
        from export_ppt import export_list_ppt
        data = export_list_ppt(pairs)
    else:
        from export_ppt import export_multi_ppt
        data = export_multi_ppt(pairs)
    fname = f"influencer_{mode}_{datetime.now().strftime('%Y%m%d')}.pptx"
    return Response(data,
                    media_type="application/vnd.openxmlformats-officedocument.presentationml.presentation",
                    headers={"Content-Disposition": _safe_cd(fname)})


@app.get("/export")
def export_excel(
    q: str = "", hashtag: str = "",
    min_f: Optional[int] = None, max_f: Optional[int] = None,
    verified: int = 0, public_only: int = 0,
    main_category: str = "", can_live: int = 0, only_approved: int = 0,
    session_id: Optional[str] = Cookie(default=None)
):
    user = get_user(session_id) or get_adv_user(session_id)
    if not user: return RedirectResponse("/login", 302)

    import openpyxl
    from io import BytesIO

    _, rows = get_influencers(
        keyword=q, hashtag_filter=hashtag, min_f=min_f, max_f=max_f,
        only_verified=bool(verified), exclude_private=bool(public_only),
        main_category=main_category, can_live=bool(can_live), only_approved=bool(only_approved),
        sort="follower_count", order="desc", page=1, per_page=100000
    )

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "인플루언서"
    headers = ["username","full_name","팔로워","팔로우","게시물","인증","비공개",
               "카테고리","이메일","외부링크","해시태그",
               "참여율","평균좋아요","평균릴스뷰","릴스%","협찬%","업로드빈도",
               "마지막게시","라이브가능","피드단가","릴스단가","협업유형","품질점수","메모",
               "Instagram URL"]
    ws.append(headers)
    for r in rows:
        m = get_manual(r.get("pk", ""))
        ws.append([
            r.get("username"), r.get("full_name"),
            r.get("follower_count"), r.get("following_count"), r.get("media_count"),
            "O" if r.get("is_verified") else "",
            "O" if r.get("is_private") else "",
            r.get("category"), r.get("public_email"), r.get("external_url"), r.get("hashtags"),
            r.get("engagement_rate"), r.get("avg_likes"), r.get("avg_reel_views"),
            r.get("reels_ratio"), r.get("sponsored_ratio"), r.get("upload_frequency"),
            r.get("last_post_date"),
            "O" if m.get("can_live") else "",
            m.get("feed_price"), m.get("reel_price"),
            m.get("collab_types"), m.get("quality_score"), m.get("notes"),
            f"https://www.instagram.com/{r.get('username')}/",
        ])

    buf = BytesIO()
    wb.save(buf); buf.seek(0)
    fname = f"influencers_{datetime.now().strftime('%Y%m%d_%H%M')}.xlsx"
    return Response(buf.read(),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": _safe_cd(fname)})


# ═══════════════════════════════════════════════════════
# 전체 갱신 (관리자)
# ═══════════════════════════════════════════════════════

@app.post("/refresh/start")
def refresh_start(background_tasks: BackgroundTasks,
                  session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id)
    if not user: return JSONResponse({"error": "인증 필요"}, 403)
    from crawler import refresh_all, refresh_progress
    if refresh_progress.get("current", {}).get("running"):
        return JSONResponse({"error": "이미 실행 중"}, 400)
    background_tasks.add_task(refresh_all,
                              INSTA_CFG["username"], INSTA_CFG["password"], INSTA_CFG["totp"],
                              INSTA_CFG.get("proxy_host",""), INSTA_CFG.get("proxy_port",""),
                              INSTA_CFG.get("proxy_user",""), INSTA_CFG.get("proxy_pass",""))
    return JSONResponse({"ok": True})

@app.get("/refresh/status")
def refresh_status(session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id)
    if not user: return JSONResponse({"error": "인증 필요"}, 403)
    from crawler import refresh_progress
    return JSONResponse(refresh_progress.get("current", {}))


# ═══════════════════════════════════════════════════════
# 해시태그 관리
# ═══════════════════════════════════════════════════════

@app.get("/hashtags", response_class=HTMLResponse)
def hashtags_page(request: Request, session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id)
    if not user: return RedirectResponse("/login", 302)
    rows = get_hashtags()
    jobs = get_collect_jobs(30)
    return templates.TemplateResponse("hashtags.html", {
        "request": request, "user": user,
        "hashtags": [dict(r) if not isinstance(r, dict) else r for r in rows],
        "jobs": [dict(r) if not isinstance(r, dict) else r for r in jobs],
    })

@app.post("/hashtags/add")
def add_hashtag_route(name: str = Form(...), requested_count: int = Form(default=500),
                auto_collect: int = Form(default=1),
                session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id)
    if not user: return RedirectResponse("/login", 302)
    name = name.strip().lstrip("#")
    try:
        db_add_hashtag(name, requested_count, auto_collect)
    except Exception as e:
        log.error(f"해시태그 추가 실패: {e}")
    return RedirectResponse("/hashtags", 302)

@app.post("/hashtags/delete")
def delete_hashtag_route(name: str = Form(default=""), hashtag_id: int = Form(default=0),
                         session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id)
    if not user: return RedirectResponse("/login", 302)
    if hashtag_id:
        db_delete_hashtag(hashtag_id)
    elif name:
        # SQLite fallback: find by name
        from database import _USE_SUPABASE
        if not _USE_SUPABASE:
            conn = get_conn()
            conn.execute("DELETE FROM insta_hashtags WHERE name=?", (name,))
            conn.commit(); conn.close()
    return RedirectResponse("/hashtags", 302)


# ═══════════════════════════════════════════════════════
# 수집
# ═══════════════════════════════════════════════════════

@app.get("/collect", response_class=HTMLResponse)
def collect_page(request: Request, session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id)
    if not user: return RedirectResponse("/login", 302)
    htags = get_hashtags()
    return templates.TemplateResponse("collect.html", {
        "request": request, "user": user,
        "hashtags": [r.get("name","") if isinstance(r, dict) else r["name"] for r in htags],
    })

@app.post("/collect/start")
def collect_start(hashtag: str = Form(...), requested_count: int = Form(default=500),
                  background_tasks: BackgroundTasks = BackgroundTasks(),
                  session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id)
    if not user: return HTMLResponse("인증 필요", 403)
    hashtag = hashtag.strip().lstrip("#")
    job_id = str(uuid.uuid4())
    try:
        update_hashtag_status(hashtag, "running")
    except Exception:
        try: db_add_hashtag(hashtag)
        except: pass
    from crawler import crawl_hashtag
    background_tasks.add_task(crawl_hashtag, hashtag, requested_count,
                               INSTA_CFG["username"], INSTA_CFG["password"], INSTA_CFG["totp"], job_id,
                               INSTA_CFG.get("proxy_host",""), INSTA_CFG.get("proxy_port",""),
                               INSTA_CFG.get("proxy_user",""), INSTA_CFG.get("proxy_pass",""))
    return HTMLResponse(job_id)

@app.get("/collect/progress/{job_id}")
def collect_progress(job_id: str):
    from crawler import progress

    def stream():
        while True:
            p = progress.get(job_id, {})
            yield f"data: {json.dumps(p, ensure_ascii=False)}\n\n"
            if p.get("done"): break
            time.sleep(1)

    return StreamingResponse(stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


# ═══════════════════════════════════════════════════════
# 광고주 계정 관리 (관리자)
# ═══════════════════════════════════════════════════════

@app.get("/advertisers", response_class=HTMLResponse)
def advertisers_page(request: Request, session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id)
    if not user: return RedirectResponse("/login", 302)
    advs = get_advertisers()
    return templates.TemplateResponse("advertiser_list.html", {
        "request": request, "user": user, "advertisers": advs,
        "plans": PLANS, "now": time.time(),
    })

@app.post("/advertisers/add")
def add_advertiser_route(
    username: str = Form(...), password: str = Form(...),
    company_name: str = Form(default=""),
    hashtag_access: str = Form(default=""),
    min_followers: int = Form(default=0),
    only_approved: int = Form(default=1),
    session_id: Optional[str] = Cookie(default=None)
):
    user = get_user(session_id)
    if not user: return RedirectResponse("/login", 302)
    pw_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    try:
        db_add_advertiser(username.strip(), pw_hash, company_name, hashtag_access, min_followers, only_approved)
    except Exception as e:
        log.error(f"광고주 추가 실패: {e}")
    return RedirectResponse("/advertisers?added=1", 302)

@app.post("/advertisers/delete")
def delete_advertiser_route(adv_id: int = Form(...), session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id)
    if not user: return RedirectResponse("/login", 302)
    db_delete_advertiser(adv_id)
    return RedirectResponse("/advertisers", 302)

@app.post("/advertisers/{adv_id}/plan")
def set_advertiser_plan(
    adv_id: int,
    plan: str = Form(...),
    months: int = Form(default=1),
    hashtag_access: str = Form(default=""),
    min_followers: int = Form(default=0),
    only_approved: int = Form(default=1),
    session_id: Optional[str] = Cookie(default=None)
):
    user = get_user(session_id)
    if not user: return RedirectResponse("/login", 302)
    plan_info = PLANS.get(plan, PLANS["free"])
    expires = time.time() + months * 30 * 86400
    update_advertiser_plan(
        adv_id, plan, expires,
        plan_info["per_hashtag"],
        hashtag_access or None,
        min_followers or None,
        only_approved
    )
    return RedirectResponse("/advertisers?updated=1", 302)


# ═══════════════════════════════════════════════════════
# 광고주 로그인 / 대시보드
# ═══════════════════════════════════════════════════════

@app.get("/advertiser/login", response_class=HTMLResponse)
def adv_login_page(request: Request):
    return templates.TemplateResponse("advertiser_login.html", {"request": request})

@app.post("/advertiser/login")
def adv_login(request: Request, username: str = Form(...), password: str = Form(...)):
    row = get_advertiser_by_username(username.strip())
    if row and bcrypt.checkpw(password.encode(), (row.get("password_hash") or "").encode()):
        token = _make_jwt({"role": "advertiser", "username": username, "adv_id": row.get("id")})
        res = RedirectResponse("/advertiser", status_code=302)
        res.set_cookie("adv_session_id", token, httponly=True, max_age=604800)
        return res
    return templates.TemplateResponse("advertiser_login.html", {
        "request": request, "error": "아이디 또는 비밀번호가 올바르지 않습니다."
    }, status_code=400)

@app.get("/advertiser/logout")
def adv_logout(adv_session_id: Optional[str] = Cookie(default=None)):
    adv_sessions.pop(adv_session_id, None)
    res = RedirectResponse("/advertiser/login", 302)
    res.delete_cookie("adv_session_id")
    return res

@app.get("/advertiser", response_class=HTMLResponse)
def adv_dashboard(
    request: Request,
    q: str = "",
    hashtag: str = "",
    min_f: Optional[int] = None,
    max_f: Optional[int] = None,
    main_category: str = "",
    can_live: int = 0,
    sort: str = "follower_count",
    order: str = "desc",
    page: int = Query(1, gt=0),
    per_page: int = Query(30, gt=0),
    adv_session_id: Optional[str] = Cookie(default=None)
):
    adv = get_adv_user(adv_session_id)
    if not adv: return RedirectResponse("/advertiser/login", 302)

    # 광고주 접근 해시태그 필터링
    allowed_hashtags = adv.get("hashtag_access", "")
    effective_hashtag = hashtag
    if allowed_hashtags and not hashtag:
        effective_hashtag = allowed_hashtags.split(",")[0].strip()

    only_approved = bool(adv.get("only_approved", 1))
    min_f_eff = max(min_f or 0, adv.get("min_followers", 0)) or None

    total, rows = get_influencers(
        keyword=q, hashtag_filter=effective_hashtag,
        min_f=min_f_eff, max_f=max_f,
        only_approved=only_approved,
        main_category=main_category, can_live=bool(can_live),
        sort=sort, order=order, page=page, per_page=per_page
    )
    total_pages = max(1, (total + per_page - 1) // per_page)
    allowed_list = [h.strip() for h in allowed_hashtags.split(",") if h.strip()] if allowed_hashtags else []

    return templates.TemplateResponse("advertiser_dashboard.html", {
        "request": request, "adv": adv,
        "rows": rows, "total": total, "total_pages": total_pages,
        "page": page, "per_page": per_page,
        "q": q, "hashtag": effective_hashtag,
        "min_f": min_f, "max_f": max_f,
        "main_category": main_category, "can_live": can_live,
        "sort": sort, "order": order,
        "allowed_hashtags": allowed_list,
    })

@app.get("/advertiser/influencers/{pk}", response_class=HTMLResponse)
def adv_influencer_detail(pk: str, request: Request,
                           adv_session_id: Optional[str] = Cookie(default=None)):
    adv = get_adv_user(adv_session_id)
    if not adv: return RedirectResponse("/advertiser/login", 302)
    inf = get_influencer(pk)
    if not inf: raise HTTPException(404)
    manual = get_manual(pk)
    # 광고주에게는 연락처 숨김 (is_approved만 공개)
    manual_safe = {
        "can_live": manual.get("can_live"),
        "live_platforms": manual.get("live_platforms"),
        "feed_price": manual.get("feed_price"),
        "reel_price": manual.get("reel_price"),
        "story_price": manual.get("story_price"),
        "main_category": manual.get("main_category"),
        "sub_categories": manual.get("sub_categories"),
        "target_gender": manual.get("target_gender"),
        "target_age": manual.get("target_age"),
        "target_region": manual.get("target_region"),
        "collab_types": manual.get("collab_types"),
        "past_brands": manual.get("past_brands"),
        "quality_score": manual.get("quality_score"),
    }
    for key in ["top_posts_likes", "top_posts_comments", "top_reels_views"]:
        try: inf[key] = json.loads(inf.get(key) or "[]")
        except: inf[key] = []
    posts = get_influencer_posts(pk)
    return templates.TemplateResponse("influencer_detail.html", {
        "request": request, "adv": adv, "user": None,
        "inf": inf, "manual": manual_safe, "posts": posts,
        "is_advertiser_view": True,
    })


# ═══════════════════════════════════════════════════════
# ═══════════════════════════════════════════════════════
# 인스타그램 계정 관리
# ═══════════════════════════════════════════════════════

@app.get("/accounts", response_class=HTMLResponse)
def accounts_page(request: Request, session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id)
    if not user: return RedirectResponse("/login", 302)
    accs = get_accounts()
    return templates.TemplateResponse("accounts.html", {
        "request": request, "user": user,
        "accounts": [dict(a) if not isinstance(a, dict) else a for a in accs],
    })

@app.post("/accounts/add")
def account_add(
    username: str = Form(...), password: str = Form(...),
    totp_secret: str = Form(default=""),
    proxy_host: str = Form(default=""), proxy_port: str = Form(default=""),
    proxy_user: str = Form(default=""), proxy_pass: str = Form(default=""),
    session_id: Optional[str] = Cookie(default=None)
):
    user = get_user(session_id)
    if not user: return RedirectResponse("/login", 302)
    try:
        upsert_account(username.strip(), password, totp_secret.strip(),
                       proxy_host, proxy_port, proxy_user, proxy_pass)
    except Exception as e:
        log.error(f"계정 추가 실패: {e}")
    return RedirectResponse("/accounts?added=1", 302)

@app.post("/accounts/upload")
async def account_upload(
    file: UploadFile = File(default=None),
    session_id: Optional[str] = Cookie(default=None)
):
    """TXT/CSV/XLSX 파일로 다중 계정 업로드"""
    user = get_user(session_id)
    if not user: return JSONResponse({"error": "인증 필요"}, 403)

    if not file:
        return JSONResponse({"error": "파일 없음"}, 400)

    content = await file.read()
    filename = file.filename.lower()
    accounts_parsed = []

    try:
        if filename.endswith(".xlsx") or filename.endswith(".xls"):
            # Excel 파싱
            import openpyxl, io
            wb = openpyxl.load_workbook(io.BytesIO(content), data_only=True)
            ws = wb.active
            rows = list(ws.iter_rows(values_only=True))
            # 첫 행이 헤더인지 확인
            header = [str(c).lower().strip() if c else "" for c in rows[0]] if rows else []
            col_map = {}
            for i, h in enumerate(header):
                for field in ["username","id","아이디"]:
                    if field in h: col_map["username"] = i
                for field in ["password","pw","비밀번호"]:
                    if field in h: col_map["password"] = i
                for field in ["totp","2fa","otp","secret"]:
                    if field in h: col_map["totp_secret"] = i
                for field in ["proxy_host","proxy","프록시"]:
                    if field in h: col_map["proxy_host"] = i
                for field in ["proxy_port","port","포트"]:
                    if field in h: col_map["proxy_port"] = i
                for field in ["proxy_user","proxy_id"]:
                    if field in h: col_map["proxy_user"] = i
                for field in ["proxy_pass","proxy_pw","proxy_password"]:
                    if field in h: col_map["proxy_pass"] = i

            start_row = 1 if col_map else 0
            for row in rows[start_row:]:
                if not row or not row[col_map.get("username", 0)]:
                    continue
                def _get(field, default=""):
                    idx = col_map.get(field)
                    if idx is None: return default
                    return str(row[idx]).strip() if idx < len(row) and row[idx] else default
                accounts_parsed.append({
                    "username": _get("username"),
                    "password": _get("password"),
                    "totp_secret": _get("totp_secret").replace(" ", "").upper(),
                    "proxy_host": _get("proxy_host"),
                    "proxy_port": _get("proxy_port"),
                    "proxy_user": _get("proxy_user"),
                    "proxy_pass": _get("proxy_pass"),
                })

        else:
            # TXT/CSV 파싱
            # 지원 형식:
            # username:password
            # username:password:totp_secret
            # username:password:totp_secret:proxy_host:proxy_port
            # username:password:totp_secret:proxy_host:proxy_port:proxy_user:proxy_pass
            # 탭 또는 콤마 구분도 지원
            text = content.decode("utf-8-sig", errors="replace")
            for line in text.splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                # 구분자 감지
                if "\t" in line:
                    parts = line.split("\t")
                elif "," in line:
                    parts = line.split(",")
                else:
                    parts = line.split(":")
                parts = [p.strip() for p in parts]
                if len(parts) < 2:
                    continue
                # TOTP 시크릿은 스페이스 포함 가능 (e.g. "LMT7 GEDD OE5A ...")
                # username:password:TOTP(스페이스 포함) 형식 처리
                # 3번째 필드 이후가 proxy가 아닌 경우 TOTP로 합침
                # proxy는 보통 ip/domain 형식이라 구분 가능
                totp_raw = parts[2] if len(parts) > 2 else ""
                proxy_host, proxy_port, proxy_user, proxy_pass = "", "", "", ""

                if len(parts) > 3:
                    # 4번째 필드가 숫자(포트)거나 ip처럼 보이면 proxy_host:proxy_port
                    # 그 외엔 totp 시크릿이 스페이스 없이 단일 필드로 있는 경우
                    fourth = parts[3]
                    if fourth.isdigit() or (len(fourth) <= 5 and fourth.isdigit()):
                        # parts[2]=host, parts[3]=port 형태
                        proxy_host = parts[2]
                        proxy_port = parts[3]
                        totp_raw = ""
                        proxy_user = parts[4] if len(parts) > 4 else ""
                        proxy_pass = parts[5] if len(parts) > 5 else ""
                    else:
                        proxy_host = parts[3] if len(parts) > 3 else ""
                        proxy_port = parts[4] if len(parts) > 4 else ""
                        proxy_user = parts[5] if len(parts) > 5 else ""
                        proxy_pass = parts[6] if len(parts) > 6 else ""

                # TOTP 시크릿 스페이스 제거 (base32는 공백 없어야 함)
                totp_clean = totp_raw.replace(" ", "").upper()

                accounts_parsed.append({
                    "username": parts[0],
                    "password": parts[1],
                    "totp_secret": totp_clean,
                    "proxy_host": proxy_host,
                    "proxy_port": proxy_port,
                    "proxy_user": proxy_user,
                    "proxy_pass": proxy_pass,
                })

    except Exception as e:
        return JSONResponse({"error": f"파일 파싱 오류: {e}"}, 400)

    if not accounts_parsed:
        return JSONResponse({"error": "유효한 계정 데이터 없음"}, 400)

    added = 0
    errors = []
    for acc in accounts_parsed:
        if not acc["username"] or not acc["password"]:
            continue
        try:
            upsert_account(
                acc["username"], acc["password"], acc.get("totp_secret",""),
                acc.get("proxy_host",""), acc.get("proxy_port",""),
                acc.get("proxy_user",""), acc.get("proxy_pass","")
            )
            added += 1
        except Exception as e:
            errors.append(f"{acc['username']}: {e}")

    return JSONResponse({
        "ok": True, "added": added,
        "total": len(accounts_parsed),
        "errors": errors[:5]
    })

@app.post("/accounts/{account_id}/delete")
def account_delete(account_id: int, session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id)
    if not user: return RedirectResponse("/login", 302)
    delete_account(account_id)
    return RedirectResponse("/accounts", 302)

@app.post("/accounts/{account_id}/test")
async def account_test(account_id: int, session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id)
    if not user: return JSONResponse({"error": "인증 필요"}, 403)
    from crawler import login_test_account
    result = login_test_account(account_id)
    return JSONResponse(result)

@app.post("/accounts/reset-errors")
def account_reset_errors(session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id)
    if not user: return RedirectResponse("/login", 302)
    reset_account_errors()
    return RedirectResponse("/accounts?reset=1", 302)


# ═══════════════════════════════════════════════════════
# 설정
# ═══════════════════════════════════════════════════════

@app.get("/settings", response_class=HTMLResponse)
def settings_page(request: Request, session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id)
    if not user: return RedirectResponse("/login", 302)
    return templates.TemplateResponse("settings.html", {
        "request": request, "user": user,
        "insta_username": INSTA_CFG["username"],
        "proxy_host": INSTA_CFG.get("proxy_host",""),
        "proxy_port": INSTA_CFG.get("proxy_port",""),
        "proxy_user": INSTA_CFG.get("proxy_user",""),
        "plans": PLANS,
    })

@app.post("/settings/save")
def settings_save(
    insta_username: str = Form(...),
    insta_password: str = Form(...),
    insta_totp: str = Form(default=""),
    proxy_host: str = Form(default=""),
    proxy_port: str = Form(default=""),
    proxy_user: str = Form(default=""),
    proxy_pass: str = Form(default=""),
    admin_password: str = Form(default=""),
    session_id: Optional[str] = Cookie(default=None)
):
    global ADMIN_PW_HASH
    user = get_user(session_id)
    if not user: return RedirectResponse("/login", 302)
    INSTA_CFG["username"] = insta_username
    INSTA_CFG["password"] = insta_password
    INSTA_CFG["totp"] = insta_totp
    INSTA_CFG["proxy_host"] = proxy_host
    INSTA_CFG["proxy_port"] = proxy_port
    INSTA_CFG["proxy_user"] = proxy_user
    INSTA_CFG["proxy_pass"] = proxy_pass
    if admin_password:
        ADMIN_PW_HASH = bcrypt.hashpw(admin_password.encode(), bcrypt.gensalt())

    env_path = os.path.join(os.path.dirname(__file__), ".env")
    lines = open(env_path).readlines() if os.path.exists(env_path) else []
    keys = {
        "INSTA_USERNAME": insta_username, "INSTA_PASSWORD": insta_password,
        "INSTA_TOTP": insta_totp,
        "INSTA_PROXY_HOST": proxy_host, "INSTA_PROXY_PORT": proxy_port,
        "INSTA_PROXY_USER": proxy_user, "INSTA_PROXY_PASS": proxy_pass,
    }
    if admin_password:
        keys["ADMIN_PASSWORD"] = admin_password
    existing = {l.split("=")[0]: i for i, l in enumerate(lines) if "=" in l}
    for k, v in keys.items():
        if k in existing: lines[existing[k]] = f"{k}={v}\n"
        else: lines.append(f"{k}={v}\n")
    try:
        open(env_path, "w").writelines(lines)
    except OSError:
        pass  # Vercel 환경에서는 .env 쓰기 불가
    return RedirectResponse("/settings?saved=1", 302)

@app.post("/settings/test-account")
async def test_insta_account(session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id)
    if not user: raise HTTPException(403)
    try:
        import pyotp
        totp_code = ""
        if INSTA_CFG.get("totp"):
            totp_code = pyotp.TOTP(INSTA_CFG["totp"]).now()
        return JSONResponse({"ok": True, "totp_code": totp_code, "username": INSTA_CFG["username"]})
    except Exception as e:
        return JSONResponse({"ok": False, "error": str(e)})


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
