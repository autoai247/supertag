from fastapi import FastAPI, Request, Form, Cookie, Query, BackgroundTasks, HTTPException
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
                      update_influencer_stats)

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

# ── 인증 (관리자) ─────────────────────────────────────────
ADMIN_PW_HASH = bcrypt.hashpw(
    os.getenv("ADMIN_PASSWORD", "admin1234").encode(), bcrypt.gensalt()
)
ADMIN_USER = os.getenv("ADMIN_USERNAME", "admin")
sessions: dict = {}
adv_sessions: dict = {}  # 광고주 세션

INSTA_CFG = {
    "username": os.getenv("INSTA_USERNAME", "jannat160304"),
    "password": os.getenv("INSTA_PASSWORD", "jug@575"),
    "totp":     os.getenv("INSTA_TOTP", "YQ754N2HTC7IDAT5BQIPNA5RHQA75JFY"),
}


def get_user(session_id: Optional[str] = None):
    return sessions.get(session_id) if session_id else None

def get_adv_user(session_id: Optional[str] = None):
    return adv_sessions.get(session_id) if session_id else None

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
        sid = str(uuid.uuid4())
        sessions[sid] = {"username": username}
        res = RedirectResponse("/influencers", status_code=302)
        res.set_cookie("session_id", sid, httponly=True)
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
    })


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
            cl = get_client(INSTA_CFG["username"], INSTA_CFG["password"], INSTA_CFG["totp"])
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
                              INSTA_CFG["username"], INSTA_CFG["password"], INSTA_CFG["totp"])
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
    conn = get_conn()
    rows = conn.execute("SELECT * FROM hashtags ORDER BY created_at DESC").fetchall()
    jobs = conn.execute("SELECT * FROM collect_jobs ORDER BY started_at DESC LIMIT 30").fetchall()
    conn.close()
    return templates.TemplateResponse("hashtags.html", {
        "request": request, "user": user,
        "hashtags": [dict(r) for r in rows],
        "jobs": [dict(r) for r in jobs],
    })

@app.post("/hashtags/add")
def add_hashtag(name: str = Form(...), requested_count: int = Form(default=500),
                auto_collect: int = Form(default=1),
                session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id)
    if not user: return RedirectResponse("/login", 302)
    name = name.strip().lstrip("#")
    conn = get_conn()
    try:
        conn.execute("INSERT INTO hashtags (name, auto_collect, created_at) VALUES (?,?,?)",
                     (name, auto_collect, time.time()))
        conn.commit()
    except: pass
    conn.close()
    return RedirectResponse("/hashtags", 302)

@app.post("/hashtags/delete")
def delete_hashtag(name: str = Form(...), session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id)
    if not user: return RedirectResponse("/login", 302)
    conn = get_conn()
    conn.execute("DELETE FROM hashtags WHERE name=?", (name,))
    conn.commit(); conn.close()
    return RedirectResponse("/hashtags", 302)


# ═══════════════════════════════════════════════════════
# 수집
# ═══════════════════════════════════════════════════════

@app.get("/collect", response_class=HTMLResponse)
def collect_page(request: Request, session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id)
    if not user: return RedirectResponse("/login", 302)
    conn = get_conn()
    htags = conn.execute("SELECT name FROM hashtags ORDER BY name").fetchall()
    conn.close()
    return templates.TemplateResponse("collect.html", {
        "request": request, "user": user,
        "hashtags": [r["name"] for r in htags],
    })

@app.post("/collect/start")
def collect_start(hashtag: str = Form(...), requested_count: int = Form(default=500),
                  background_tasks: BackgroundTasks = BackgroundTasks(),
                  session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id)
    if not user: return HTMLResponse("인증 필요", 403)
    hashtag = hashtag.strip().lstrip("#")
    job_id = str(uuid.uuid4())
    conn = get_conn()
    try:
        conn.execute("INSERT INTO hashtags (name, status, created_at) VALUES (?,?,?)",
                     (hashtag, "running", time.time()))
    except:
        conn.execute("UPDATE hashtags SET status='running' WHERE name=?", (hashtag,))
    conn.commit(); conn.close()
    from crawler import crawl_hashtag
    background_tasks.add_task(crawl_hashtag, hashtag, requested_count,
                               INSTA_CFG["username"], INSTA_CFG["password"], INSTA_CFG["totp"], job_id)
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
    })

@app.post("/advertisers/add")
def add_advertiser(
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
    conn = get_conn()
    try:
        conn.execute("""
            INSERT INTO advertiser_accounts
                (username, password_hash, company_name, hashtag_access, min_followers, only_approved, created_at)
            VALUES (?,?,?,?,?,?,?)
        """, (username.strip(), pw_hash, company_name, hashtag_access, min_followers, only_approved, time.time()))
        conn.commit()
    except Exception as e:
        log.error(f"광고주 추가 실패: {e}")
    conn.close()
    return RedirectResponse("/advertisers?added=1", 302)

@app.post("/advertisers/delete")
def delete_advertiser(adv_id: int = Form(...), session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id)
    if not user: return RedirectResponse("/login", 302)
    conn = get_conn()
    conn.execute("DELETE FROM advertiser_accounts WHERE id=?", (adv_id,))
    conn.commit(); conn.close()
    return RedirectResponse("/advertisers", 302)


# ═══════════════════════════════════════════════════════
# 광고주 로그인 / 대시보드
# ═══════════════════════════════════════════════════════

@app.get("/advertiser/login", response_class=HTMLResponse)
def adv_login_page(request: Request):
    return templates.TemplateResponse("advertiser_login.html", {"request": request})

@app.post("/advertiser/login")
def adv_login(request: Request, username: str = Form(...), password: str = Form(...)):
    conn = get_conn()
    row = conn.execute("SELECT * FROM advertiser_accounts WHERE username=?", (username.strip(),)).fetchone()
    conn.close()
    if row and bcrypt.checkpw(password.encode(), row["password_hash"].encode()):
        sid = str(uuid.uuid4())
        adv_sessions[sid] = dict(row)
        res = RedirectResponse("/advertiser", status_code=302)
        res.set_cookie("adv_session_id", sid, httponly=True)
        return res
    return templates.TemplateResponse("advertiser_login.html", {
        "request": request, "error": "아이디 또는 비밀번호가 올바르지 않습니다."
    }, status_code=400)

@app.get("/advertiser/logout")
def adv_logout(adv_session_id: Optional[str] = Cookie(default=None)):
    if adv_session_id in adv_sessions: del adv_sessions[adv_session_id]
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
# 설정
# ═══════════════════════════════════════════════════════

@app.get("/settings", response_class=HTMLResponse)
def settings_page(request: Request, session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id)
    if not user: return RedirectResponse("/login", 302)
    return templates.TemplateResponse("settings.html", {
        "request": request, "user": user,
        "insta_username": INSTA_CFG["username"],
    })

@app.post("/settings/save")
def settings_save(
    insta_username: str = Form(...),
    insta_password: str = Form(...),
    insta_totp: str = Form(default=""),
    admin_password: str = Form(default=""),
    session_id: Optional[str] = Cookie(default=None)
):
    global ADMIN_PW_HASH
    user = get_user(session_id)
    if not user: return RedirectResponse("/login", 302)
    INSTA_CFG["username"] = insta_username
    INSTA_CFG["password"] = insta_password
    INSTA_CFG["totp"] = insta_totp
    if admin_password:
        ADMIN_PW_HASH = bcrypt.hashpw(admin_password.encode(), bcrypt.gensalt())

    env_path = os.path.join(os.path.dirname(__file__), ".env")
    lines = open(env_path).readlines() if os.path.exists(env_path) else []
    keys = {"INSTA_USERNAME": insta_username, "INSTA_PASSWORD": insta_password, "INSTA_TOTP": insta_totp}
    if admin_password:
        keys["ADMIN_PASSWORD"] = admin_password
    existing = {l.split("=")[0]: i for i, l in enumerate(lines) if "=" in l}
    for k, v in keys.items():
        if k in existing: lines[existing[k]] = f"{k}={v}\n"
        else: lines.append(f"{k}={v}\n")
    open(env_path, "w").writelines(lines)
    return RedirectResponse("/settings?saved=1", 302)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
