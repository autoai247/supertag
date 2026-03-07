from fastapi import FastAPI, Request, Form, Cookie, Query, BackgroundTasks, HTTPException, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse, Response, JSONResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List
import uuid, time, json, os, re, bcrypt, logging, collections, hashlib, secrets, hmac
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

from database import (init_db, get_conn, get_influencers, get_influencer, get_influencer_posts, get_influencer_reels,
                      get_stats, get_public_stats, get_public_influencers,
                      get_manual, save_manual, get_advertisers, get_refresh_status,
                      update_influencer_stats, get_advertiser_by_username,
                      add_advertiser as db_add_advertiser, delete_advertiser as db_delete_advertiser,
                      update_advertiser_plan,
                      get_hashtags, get_collect_jobs, add_hashtag as db_add_hashtag,
                      delete_hashtag as db_delete_hashtag, update_hashtag_status,
                      add_collect_job, update_collect_job,
                      get_influencer_by_username, update_influencer_profile,
                      get_favorites, get_favorite_pks, toggle_favorite,
                      get_campaigns, create_campaign, get_campaign, get_campaign_influencers,
                      add_to_campaign, remove_from_campaign, delete_campaign,
                      add_cron_log, get_cron_logs, get_auto_hashtags)

# ── 해시태그 활동 유형 분석 헬퍼 ──
_SELLER_KW = {
    "공동구매","공구","단독공구","공구오픈","공구마감","공구진행","공구알림",
    "마감임박","품절임박","재오픈","재입고","선착순","추가생산","예약구매","한정수량",
    "오픈예정","마감","품절","솔드아웃","추가주문",
    "스마트스토어","쇼핑몰","내쇼핑몰","구매대행","셀러",
    "링크인바이오","프로필링크","네이버쇼핑","카카오쇼핑",
    "라이브커머스","네이버라이브","카카오라이브","라이브쇼핑","쇼핑라이브",
}
_AD_KW = {
    "협찬","체험단","광고","유료광고","제공","제공받음","후원","지원받음",
    "ad","sponsored","partnership","콜라보","콜라보레이션",
    "ppl","브랜드협찬","브랜드체험","무료체험","서포터즈","앰배서더",
    "솔직후기","내돈내산","리뷰","사용후기","실사용","내돈내구매",
}

def _analyze_activity(htags: list) -> dict:
    """해시태그 사용 빈도 가중합으로 공구셀러 vs 광고수신 vs 순수콘텐츠 비중 계산"""
    def _match(tag: str, kw_set) -> bool:
        t = tag.lower().replace(" ", "").replace("#", "")
        return any(kw in t for kw in kw_set)

    seller_score = sum(h["count"] for h in htags if _match(h["tag"], _SELLER_KW))
    ad_score     = sum(h["count"] for h in htags if _match(h["tag"], _AD_KW))
    total_tagged = seller_score + ad_score
    content_score = max(0, sum(h["count"] for h in htags) - total_tagged)
    grand = seller_score + ad_score + content_score or 1

    return {
        "seller_pct":  round(seller_score  / grand * 100),
        "ad_pct":      round(ad_score      / grand * 100),
        "content_pct": round(content_score / grand * 100),
        "seller_tags": [h["tag"] for h in htags if _match(h["tag"], _SELLER_KW)][:8],
        "ad_tags":     [h["tag"] for h in htags if _match(h["tag"], _AD_KW)][:8],
        "primary": (
            "seller"  if seller_score > ad_score * 1.5 and seller_score > 0
            else "ad" if ad_score > seller_score * 1.5 and ad_score > 0
            else "mixed"  if total_tagged > 0
            else "content"
        ),
        "has_data": len(htags) > 0,
    }


app = FastAPI()

# ─── CORS: 크롬 확장 프로그램만 허용, 와일드카드(*) 제거 ───
_CORS_ORIGINS = [o.strip() for o in os.getenv("CORS_ORIGINS", "").split(",") if o.strip()]
app.add_middleware(CORSMiddleware,
    allow_origins=_CORS_ORIGINS or [],          # 기본: 외부 origin 차단 (same-origin만 허용)
    allow_origin_regex=r"^chrome-extension://.*$",  # 크롬 확장은 패턴으로 허용
    allow_methods=["GET","POST"],
    allow_headers=["X-Api-Key", "Content-Type"],
    allow_credentials=False,                    # credentials 불필요 시 false
)

# ─── 봇 차단 / Rate Limiting ───────────────────────────────────────
_BOT_UA = [
    "bot","crawler","spider","scraper","wget","curl","python-requests",
    "httpx","aiohttp","scrapy","mechanize","selenium","playwright",
    "headlesschrome","phantomjs","slurp","baiduspider",
    "facebookexternalhit","twitterbot",
    "gptbot","chatgpt","claudebot","anthropic","ccbot","semrush",
    "ahrefsbot","mj12bot","dotbot","petalbot","bytespider",
    "zoominfobot","dataforseobot","blexbot","megaindex",
    "go-http-client","java/","libwww-perl","httpclient",
]
# 정상 검색엔진 봇 (홈페이지만 허용, 데이터 페이지는 차단)
_SEARCH_ENGINE_BOTS = ["googlebot", "bingbot", "yeti", "naverbot", "daumoa", "yandexbot"]

# IP당 분당 요청 기록 (최근 60초 타임스탬프 큐)
_rate_store: dict = collections.defaultdict(collections.deque)
_rate_store_cleanup_ts: float = 0.0  # 마지막 정리 시간
_honeypot_ips: set = set()  # 허니팟 걸린 IP 영구 차단
_RATE_LIMIT = 40  # 분당 최대 요청 수 (비인증 사용자)
_RATE_LIMIT_AUTH = 120  # 분당 최대 요청 수 (인증된 사용자)
_WHITELIST_PATHS = {"/static", "/data", "/robots.txt", "/favicon.ico"}
# 데이터 페이지 경로 (검색엔진 봇 차단 대상)
_DATA_PATHS = ["/influencers", "/api/", "/advertiser", "/export", "/collect",
               "/hashtags", "/settings", "/refresh", "/target-extract"]

def _get_client_ip(request: Request) -> str:
    """클라이언트 IP 추출 (프록시 지원)"""
    forwarded = request.headers.get("x-forwarded-for", "")
    if forwarded:
        return forwarded.split(",")[0].strip()
    return request.client.host if request.client else ""

@app.middleware("http")
async def bot_protection(request: Request, call_next):
    global _rate_store_cleanup_ts
    path = request.url.path
    ip = _get_client_ip(request)
    ua = request.headers.get("user-agent", "").lower()

    # 허니팟에 걸린 IP 영구 차단
    if ip in _honeypot_ips:
        return Response("Forbidden", status_code=403)

    # 정적파일은 통과
    if any(path.startswith(p) for p in _WHITELIST_PATHS):
        return await call_next(request)

    # User-Agent가 비어있으면 차단 (정상 브라우저는 항상 UA를 보냄)
    if not ua or len(ua) < 10:
        log.warning(f"빈/짧은 UA 차단: {ip} UA='{ua}'")
        return Response("Forbidden", status_code=403)

    # 정상 검색엔진 봇: 홈페이지(/)만 허용, 데이터 페이지는 차단
    is_search_bot = any(sb in ua for sb in _SEARCH_ENGINE_BOTS)
    if is_search_bot:
        if path == "/" or path == "":
            return await call_next(request)
        else:
            log.info(f"검색엔진 봇 데이터 페이지 차단: {ip} UA={ua[:40]} path={path}")
            return Response("Forbidden", status_code=403)

    # User-Agent 봇 차단
    if any(b in ua for b in _BOT_UA):
        log.warning(f"봇 차단: {ip} UA={ua[:60]}")
        return Response("Forbidden", status_code=403)

    # 데이터센터/헤드리스 브라우저 힌트 차단
    if any(h in ua for h in _DATACENTER_UA_HINTS):
        log.warning(f"헤드리스 브라우저 차단: {ip} UA={ua[:60]}")
        return Response("Forbidden", status_code=403)

    # 클라이언트 JS 봇 탐지 쿠키 확인 (navigator.webdriver 등)
    if request.cookies.get("_bot") == "1":
        log.warning(f"JS 봇 탐지 쿠키 발견: {ip}")
        _honeypot_ips.add(ip)
        return Response("Forbidden", status_code=403)

    # TLS Fingerprinting (Vercel 환경에서만 작동)
    if not _check_tls_fingerprint(request):
        log.warning(f"TLS fingerprint 차단: {ip} JA4={request.headers.get('x-vercel-ja4-digest','')[:20]}")
        return Response("Forbidden", status_code=403)

    # Rate limiting
    now = time.time()
    has_session = bool(request.cookies.get("session_id") or request.cookies.get("adv_session_id"))
    limit = _RATE_LIMIT_AUTH if has_session else _RATE_LIMIT
    dq = _rate_store[ip]
    # 60초 이전 기록 제거
    while dq and dq[0] < now - 60:
        dq.popleft()
    if len(dq) >= limit:
        log.warning(f"Rate limit 초과: {ip} ({len(dq)}req/min, limit={limit})")
        return JSONResponse({"error": "Too many requests"}, status_code=429,
            headers={"Retry-After": "60", "X-RateLimit-Limit": str(limit)})
    dq.append(now)

    # 5분마다 오래된 IP 레코드 정리 (메모리 누수 방지)
    if now - _rate_store_cleanup_ts > 300:
        _rate_store_cleanup_ts = now
        stale = [k for k, v in _rate_store.items() if not v or v[-1] < now - 120]
        for k in stale:
            del _rate_store[k]

    response = await call_next(request)

    # 보안 헤더 추가
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    # 데이터 페이지는 검색엔진 인덱싱 차단
    if any(path.startswith(dp) for dp in _DATA_PATHS):
        response.headers["X-Robots-Tag"] = "noindex, nofollow, noarchive"
        response.headers["Cache-Control"] = "private, no-store, no-cache, must-revalidate"

    return response


@app.get("/trap/data-feed")  # 허니팟: robots.txt에 Disallow되어 있지만 봇은 방문함
async def honeypot(request: Request):
    ip = _get_client_ip(request)
    _honeypot_ips.add(ip)
    log.warning(f"허니팟 감지! IP 영구 차단: {ip}")
    return Response("Not Found", status_code=404)

@app.get("/trap/admin-panel")  # 추가 허니팟
async def honeypot2(request: Request):
    ip = _get_client_ip(request)
    _honeypot_ips.add(ip)
    log.warning(f"허니팟2 감지! IP 영구 차단: {ip}")
    return Response("Not Found", status_code=404)


# ─── JS Challenge 토큰 (헤드리스 브라우저/스크래퍼 차단) ──────────
_JS_CHALLENGE_SECRET = os.getenv("JS_CHALLENGE_SECRET", secrets.token_hex(16))

def _generate_challenge_token(ip: str) -> str:
    """IP + 시간(10분 단위) 기반 HMAC 토큰 생성"""
    time_slot = str(int(time.time()) // 600)  # 10분마다 갱신
    msg = f"{ip}:{time_slot}".encode()
    return hmac.new(_JS_CHALLENGE_SECRET.encode(), msg, hashlib.sha256).hexdigest()[:32]

def _verify_challenge_token(ip: str, token: str) -> bool:
    """JS Challenge 토큰 검증 (현재 + 이전 10분 슬롯 허용)"""
    if not token:
        return False
    now_slot = int(time.time()) // 600
    for slot in [now_slot, now_slot - 1]:
        msg = f"{ip}:{slot}".encode()
        expected = hmac.new(_JS_CHALLENGE_SECRET.encode(), msg, hashlib.sha256).hexdigest()[:32]
        if hmac.compare_digest(token, expected):
            return True
    return False

@app.get("/api/challenge-token")
async def get_challenge_token(request: Request):
    """JS에서 호출하여 challenge 토큰을 받아가는 엔드포인트"""
    ip = _get_client_ip(request)
    return JSONResponse({"token": _generate_challenge_token(ip)})


# ─── 로그인 Brute-force 방지 ──────────────────────────────────────
_login_attempts: dict = collections.defaultdict(list)  # IP -> [timestamp, ...]
_LOGIN_MAX_ATTEMPTS = 5       # 15분 내 최대 시도
_LOGIN_LOCKOUT_SECS = 900     # 15분 잠금

def _check_login_allowed(ip: str) -> bool:
    """로그인 시도 허용 여부 확인"""
    now = time.time()
    attempts = _login_attempts[ip]
    # 15분 이전 기록 제거
    _login_attempts[ip] = [t for t in attempts if t > now - _LOGIN_LOCKOUT_SECS]
    return len(_login_attempts[ip]) < _LOGIN_MAX_ATTEMPTS

def _record_login_attempt(ip: str):
    """실패한 로그인 시도 기록"""
    _login_attempts[ip].append(time.time())


# ─── 데이터센터 IP 차단 (간이 ASN 체크) ──────────────────────────
_DATACENTER_UA_HINTS = [
    "headless", "phantomjs", "electron", "puppeteer",
    "splash", "nightmare", "casperjs", "slimerjs",
]


# ─── TLS Fingerprinting (Vercel JA4) ─────────────────────────────
# Vercel은 모든 요청에 x-vercel-ja4-digest 헤더를 자동 주입
# 알려진 봇/스크래퍼 TLS fingerprint를 차단
_KNOWN_BOT_JA4 = set()  # 탐지 시 동적으로 추가
_KNOWN_BROWSER_JA4_PREFIXES = [
    "t13d",  # TLS 1.3 (Chrome, Firefox, Safari, Edge)
    "t12d",  # TLS 1.2 (구형 브라우저)
]

def _check_tls_fingerprint(request: Request) -> bool:
    """Vercel JA4 fingerprint로 비브라우저 클라이언트 탐지.
    Vercel 환경이 아니면 (로컬 개발) 통과."""
    ja4 = request.headers.get("x-vercel-ja4-digest", "")
    if not ja4:
        return True  # 로컬 개발 환경 또는 JA4 미지원 → 통과
    if ja4 in _KNOWN_BOT_JA4:
        return False
    # TLS 1.3 브라우저 fingerprint 패턴 확인
    # JA4 형식: t13d1516h2_8daaf6152771_... (TLS버전_ciphers_extensions)
    if any(ja4.startswith(p) for p in _KNOWN_BROWSER_JA4_PREFIXES):
        return True
    # 알 수 없는 JA4 → 로그 기록 후 통과 (false positive 방지, 모니터링용)
    log.info(f"미확인 JA4 fingerprint: {ja4[:30]}")
    return True


# ─── 워터마킹 (데이터 유출 추적) ─────────────────────────────────
# Zero-Width 유니코드 문자로 사용자 ID를 텍스트에 인코딩
_ZW_CHARS = ['\u200b', '\u200c', '\u200d', '\ufeff']  # ZWS, ZWNJ, ZWJ, BOM

def _encode_watermark(user_id: str) -> str:
    """사용자 ID를 보이지 않는 Zero-Width 문자열로 인코딩"""
    # user_id의 각 문자를 2비트씩 4개의 ZW 문자로 매핑
    bits = ''.join(format(ord(c), '08b') for c in user_id[:8])  # 최대 8자
    result = []
    for i in range(0, len(bits), 2):
        idx = int(bits[i:i+2], 2)  # 0~3
        result.append(_ZW_CHARS[idx])
    return ''.join(result)

def _watermark_text(text: str, user_id: str) -> str:
    """텍스트에 보이지 않는 워터마크 삽입"""
    if not text or not user_id:
        return text
    wm = _encode_watermark(user_id)
    # 텍스트 중간에 워터마크 삽입 (첫 번째 공백 뒤)
    idx = text.find(' ')
    if idx > 0:
        return text[:idx] + wm + text[idx:]
    return text + wm

def _watermark_number(value: int, user_id: str) -> int:
    """숫자에 미세한 워터마크 (±0.1% 이내 변동, 사용자별 고유)"""
    if not value or value < 100:
        return value
    # user_id 기반 결정론적 오프셋 (-0.1% ~ +0.1%)
    h = int(hashlib.md5(f"{user_id}:{value}".encode()).hexdigest()[:4], 16)
    offset_pct = (h % 200 - 100) / 100000  # -0.001 ~ +0.001
    return int(value * (1 + offset_pct))


# ─── Proof-of-Work (PoW) ─────────────────────────────────────────
# 클라이언트가 SHA-256 해시를 찾아야 데이터 접근 가능
_POW_DIFFICULTY = 4  # 해시 앞 4자리가 '0000'이어야 함 (평균 ~65,000번 시도, ~200ms)
_POW_SECRET = os.getenv("POW_SECRET", secrets.token_hex(8))
_pow_used_nonces: dict = {}  # nonce -> expiry timestamp (재사용 방지)
_pow_cleanup_ts: float = 0.0

def _generate_pow_challenge() -> dict:
    """PoW 챌린지 생성"""
    challenge_id = secrets.token_hex(8)
    timestamp = int(time.time())
    return {
        "challenge": challenge_id,
        "timestamp": timestamp,
        "difficulty": _POW_DIFFICULTY,
    }

def _verify_pow(challenge: str, timestamp: int, nonce: str) -> bool:
    """PoW 솔루션 검증"""
    global _pow_cleanup_ts
    now = time.time()
    # 타임스탬프 유효성 (5분 이내)
    if abs(now - timestamp) > 300:
        return False
    # nonce 재사용 방지
    nonce_key = f"{challenge}:{nonce}"
    if nonce_key in _pow_used_nonces:
        return False
    # 해시 검증
    data = f"{challenge}:{timestamp}:{nonce}:{_POW_SECRET}".encode()
    h = hashlib.sha256(data).hexdigest()
    if not h.startswith('0' * _POW_DIFFICULTY):
        return False
    # nonce 등록 (5분 TTL)
    _pow_used_nonces[nonce_key] = now + 300
    # 10분마다 만료된 nonce 정리
    if now - _pow_cleanup_ts > 600:
        _pow_cleanup_ts = now
        expired = [k for k, v in _pow_used_nonces.items() if v < now]
        for k in expired:
            del _pow_used_nonces[k]
    return True

@app.get("/api/pow-challenge")
async def pow_challenge(request: Request):
    """PoW 챌린지 발급"""
    return JSONResponse(_generate_pow_challenge())

@app.post("/api/pow-verify")
async def pow_verify(request: Request):
    """PoW 솔루션 검증 → 성공 시 데이터 접근 토큰 발급"""
    try:
        body = await request.json()
        challenge = body.get("challenge", "")
        timestamp = body.get("timestamp", 0)
        nonce = body.get("nonce", "")
        if _verify_pow(challenge, timestamp, nonce):
            ip = _get_client_ip(request)
            token = _generate_challenge_token(ip)
            return JSONResponse({"ok": True, "token": token})
        return JSONResponse({"ok": False, "error": "Invalid solution"}, status_code=400)
    except Exception:
        return JSONResponse({"ok": False, "error": "Bad request"}, status_code=400)


@app.get("/robots.txt")
async def robots():
    content = """# 정상 검색엔진: 홈페이지만 허용
User-agent: Googlebot
Allow: /$
Disallow: /

User-agent: Yeti
Allow: /$
Disallow: /

User-agent: Bingbot
Allow: /$
Disallow: /

User-agent: Naverbot
Allow: /$
Disallow: /

User-agent: DaumOa
Allow: /$
Disallow: /

User-agent: Yandexbot
Allow: /$
Disallow: /

# AI/스크래핑 봇 전면 차단
User-agent: GPTBot
Disallow: /
User-agent: ChatGPT-User
Disallow: /
User-agent: CCBot
Disallow: /
User-agent: anthropic-ai
Disallow: /
User-agent: ClaudeBot
Disallow: /
User-agent: Bytespider
Disallow: /
User-agent: SemrushBot
Disallow: /
User-agent: AhrefsBot
Disallow: /
User-agent: MJ12bot
Disallow: /
User-agent: DataForSeoBot
Disallow: /
User-agent: PetalBot
Disallow: /

# 기타 모든 봇 전면 차단
User-agent: *
Disallow: /
Disallow: /api/
Disallow: /influencers/
Disallow: /advertiser/
Disallow: /export/
Disallow: /collect/
Disallow: /trap/
Disallow: /settings/
"""
    return Response(content, media_type="text/plain")

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
templates.env.filters["dt"]    = lambda t: (datetime.fromtimestamp(float(t), tz=timezone(timedelta(hours=9)))).strftime("%m/%d %H:%M") if t else "-"
templates.env.filters["comma"] = lambda n: f"{int(n or 0):,}"
def _fmtn(n):
    v = int(n or 0)
    if v <= 0: return "0"
    if v >= 1_000_000: return f"{v/1_000_000:.1f}".rstrip('0').rstrip('.') + "M"
    if v >= 1_000: return f"{v/1_000:.1f}".rstrip('0').rstrip('.') + "K"
    return str(v)
templates.env.filters["fmtn"] = _fmtn
def _fromjson(s):
    try: return json.loads(s) if isinstance(s, str) else s
    except: return {}
templates.env.filters["fromjson"] = _fromjson

def _pic(inf, size=128):
    """프로필 사진 URL: profile_pic_local > profile_pic_url > fallback"""
    _sb_url = os.environ.get("SUPABASE_URL", "https://ysqnixgdpltguatvjjcb.supabase.co")
    _SB_STORAGE = f"{_sb_url}/storage/v1/object/public/profile-pics/"
    if isinstance(inf, dict):
        local = inf.get("profile_pic_local") or ""
        cdn = inf.get("profile_pic_url") or ""
        name = inf.get("username") or inf.get("full_name") or "U"
    else:
        local = cdn = ""
        name = str(inf) if inf else "U"
    # profile_pic_local이 상대 경로면 Supabase Storage URL로 변환
    if local:
        if local.startswith("http"):
            return local
        return _SB_STORAGE + local
    if cdn:
        return cdn
    from urllib.parse import quote
    return f"https://ui-avatars.com/api/?name={quote(name[:2])}&background=6366f1&color=fff&size={size}"
templates.env.filters["pic"] = _pic

def _safe_cd(fname: str) -> str:
    """Content-Disposition 헤더용 RFC 5987 인코딩"""
    from urllib.parse import quote
    ascii_name = fname.encode("ascii", "ignore").decode("ascii") or "download"
    encoded = quote(fname, safe="")
    return f"attachment; filename=\"{ascii_name}\"; filename*=UTF-8''{encoded}"

# ── 인증 (관리자) - JWT 기반 (Vercel 서버리스 호환) ──────────
from jose import jwt as _jwt
JWT_SECRET = os.getenv("SECRET_KEY", "supertag-secret-key-2026-supers")
JWT_ALG = "HS256"
JWT_EXP_HOURS = 24 * 7  # 7일

ADMIN_PW_HASH = bcrypt.hashpw(
    os.getenv("ADMIN_PASSWORD", "admin").encode(), bcrypt.gensalt()
)
ADMIN_USER = os.getenv("ADMIN_USERNAME", "admin")
EXTENSION_API_KEY = os.getenv("EXTENSION_API_KEY", "supertag-ext-key")

def _check_ext_key(request: Request) -> bool:
    return request.headers.get("X-Api-Key") == EXTENSION_API_KEY

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
    session_id: Optional[str] = Cookie(default=None)
):
    stats = get_public_stats()
    user = get_user(session_id)
    return templates.TemplateResponse("home.html", {
        "request": request, "stats": stats, "user": user,
    })


@app.get("/api/public")
def api_public(
    request: Request,
    page: int = Query(1, gt=0),
    sort: str = "follower_count",
    q: str = "",
    min_f: int = 0,
    max_f: int = 0,
    category: str = "",
    hashtag: str = "",
    public_only: bool = False,
    no_biz: bool = False,       # 비즈니스 계정 제외
    biz_only: bool = False,     # 비즈니스 계정만
    verified_only: bool = False, # 인증 계정만
    session_id: Optional[str] = Cookie(default=None),
    adv_session_id: Optional[str] = Cookie(default=None),
):
    """공개 인플루언서 목록 JSON API - JS 렌더링용
    비인증 사용자: 제한된 필드만 반환, 페이지 제한, JS Challenge 필수
    인증 사용자: 전체 필드 반환
    """
    is_authorized = bool(get_user(session_id) or get_adv_user(adv_session_id))
    # 비인증 사용자: JS Challenge 토큰 검증 (헤드리스 크롤러 차단)
    if not is_authorized:
        challenge = request.headers.get("X-Challenge-Token", "")
        ip = _get_client_ip(request)
        if not _verify_challenge_token(ip, challenge):
            return JSONResponse({"error": "Challenge token required", "total": 0, "rows": []}, status_code=403)
    per_page = 30 if is_authorized else 10
    # 비인증 사용자는 최대 3페이지까지만 허용 (데이터 대량 수집 방지)
    if not is_authorized and page > 3:
        return JSONResponse({"error": "로그인이 필요합니다", "total": 0, "rows": []}, status_code=401)
    total, rows = get_public_influencers(page=page, per_page=per_page, sort=sort,
                                         q=q, min_f=min_f, max_f=max_f,
                                         category=category, hashtag=hashtag,
                                         public_only=public_only, no_biz=no_biz,
                                         biz_only=biz_only, verified_only=verified_only)
    total_pages = max(1, (total + per_page - 1) // per_page)
    def _g(r, k, d=0):
        return r[k] if isinstance(r, dict) else getattr(r, k, d)
    # 워터마킹용 사용자 ID 추출
    _wm_id = ""
    if is_authorized:
        u = get_user(session_id)
        a = get_adv_user(adv_session_id)
        _wm_id = (u.get("username","") if u else "") or (a.get("username","") if a else "") or _get_client_ip(request)

    result = []
    for r in rows:
        if is_authorized:
            # 인증된 사용자: 전체 데이터 (워터마크 포함)
            fc = int(_g(r,"follower_count",0))
            result.append({
                "pk": _g(r,"pk",""),
                "username": _g(r,"username",""),
                "full_name": _watermark_text(str(_g(r,"full_name","")), _wm_id),
                "biography": _watermark_text(str(_g(r,"biography","")), _wm_id),
                "follower_count": _watermark_number(fc, _wm_id),
                "engagement_rate": _g(r,"engagement_rate",0),
                "avg_likes": _g(r,"avg_likes",0),
                "avg_comments": _g(r,"avg_comments",0),
                "avg_reel_views": _g(r,"avg_reel_views",0),
                "avg_reel_likes": _g(r,"avg_reel_likes",0),
                "avg_reel_comments": _g(r,"avg_reel_comments",0),
                "avg_feed_likes": _g(r,"avg_feed_likes",0),
                "avg_feed_comments": _g(r,"avg_feed_comments",0),
                "is_verified": _g(r,"is_verified",False),
                "is_business": _g(r,"is_business",False),
                "is_private": bool(_g(r,"is_private",False)),
                "category": _g(r,"category",""),
                "hashtags": _g(r,"hashtags",""),
                "profile_pic_url": _g(r,"profile_pic_url",""),
                "profile_pic_local": _g(r,"profile_pic_local",""),
            })
        else:
            # 비인증 사용자: 제한된 필드만 (상세 통계 숨김)
            result.append({
                "pk": _g(r,"pk",""),
                "username": _g(r,"username",""),
                "full_name": _g(r,"full_name",""),
                "follower_count": _g(r,"follower_count",0),
                "is_verified": _g(r,"is_verified",False),
                "is_business": _g(r,"is_business",False),
                "category": _g(r,"category",""),
                "profile_pic_url": _g(r,"profile_pic_url",""),
                "profile_pic_local": _g(r,"profile_pic_local",""),
            })
    return {"total": total, "total_pages": total_pages, "page": page, "rows": result}


# ═══════════════════════════════════════════════════════
# 관리자 로그인
# ═══════════════════════════════════════════════════════

@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

@app.post("/login")
def login(request: Request, username: str = Form(...), password: str = Form(...)):
    ip = _get_client_ip(request)
    if not _check_login_allowed(ip):
        log.warning(f"로그인 brute-force 차단: {ip}")
        return templates.TemplateResponse("login.html", {
            "request": request, "error": "너무 많은 로그인 시도입니다. 15분 후 다시 시도해주세요."
        }, status_code=429)
    if username == ADMIN_USER and bcrypt.checkpw(password.encode(), ADMIN_PW_HASH):
        _login_attempts.pop(ip, None)  # 성공 시 기록 초기화
        token = _make_jwt({"role": "admin", "username": username})
        res = RedirectResponse("/influencers", status_code=302)
        res.set_cookie("session_id", token, httponly=True, max_age=604800,
                        samesite="lax", secure=_IS_PROD)
        return res
    _record_login_attempt(ip)
    remaining = _LOGIN_MAX_ATTEMPTS - len(_login_attempts.get(ip, []))
    return templates.TemplateResponse("login.html", {
        "request": request, "error": f"아이디 또는 비밀번호가 올바르지 않습니다. (남은 시도: {remaining}회)"
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
    min_f: Optional[str] = None,
    max_f: Optional[str] = None,
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

    _min_f = int(min_f) if min_f and min_f.isdigit() else None
    _max_f = int(max_f) if max_f and max_f.isdigit() else None

    total, rows = get_influencers(
        keyword=q, hashtag_filter=hashtag,
        min_f=_min_f, max_f=_max_f,
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

    # ── 해시태그 활동 유형 분석 ──
    activity_analysis = _analyze_activity(inf.get("top_hashtags", []))

    # posts DB에 있으면 top posts도 posts 기반으로 재계산
    if posts:
        reels_p = [p for p in posts if p.get("post_type") == "reel"]
        feeds_p = [p for p in posts if p.get("post_type") != "reel"]
        all_p = list(posts)
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
        # 댓글순 TOP 5
        comments_sorted = sorted(all_p, key=lambda x: x.get("comments", 0) or 0, reverse=True)
        inf["top_posts_comments"] = [
            {"url": p["post_url"], "comments": p.get("comments", 0), "likes": p.get("likes", 0),
             "post_type": p.get("post_type", ""), "views": p.get("views", 0),
             "thumbnail": p.get("thumbnail_url", "") or p.get("thumbnail_local", "")}
            for p in comments_sorted[:5]
        ]

    recent_reels = get_influencer_reels(pk, sort="recent", limit=12)
    popular_reels = get_influencer_reels(pk, sort="popular", limit=12)

    return templates.TemplateResponse("influencer_detail.html", {
        "request": request, "user": user,
        "inf": inf, "manual": manual, "posts": posts,
        "activity": activity_analysis,
        "recent_reels": recent_reels,
        "popular_reels": popular_reels,
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
    agency:        str = Form(default=""),
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
        "agency": agency,
    })
    return RedirectResponse(f"/influencers/{pk}?saved=1", 302)


# ═══════════════════════════════════════════════════════
# 단일 인플루언서 상세 수집 (관리자)
# ═══════════════════════════════════════════════════════

@app.post("/influencers/{pk}/mark-brand")
def mark_brand(pk: str, session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id)
    if not user: return JSONResponse({"error": "인증 필요"}, 403)
    from database import save_manual
    save_manual(pk, {"is_brand": 1})
    return JSONResponse({"ok": True})


@app.post("/influencers/{pk}/ban")
def ban_one(pk: str, reason: str = Form(default="스팸/광고 계정"),
            session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id)
    if not user: return JSONResponse({"error": "인증 필요"}, 403)
    from database import ban_influencer
    ban_influencer(pk, reason)
    return JSONResponse({"ok": True})


@app.post("/influencers/{pk}/delete")
def delete_influencer_route(pk: str, session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id)
    if not user: return JSONResponse({"error": "인증 필요"}, 403)
    from database import delete_influencer
    delete_influencer(pk)
    return JSONResponse({"ok": True})


@app.post("/influencers/{pk}/hide")
def hide_one(pk: str, session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id)
    if not user: return JSONResponse({"error": "인증 필요"}, 403)
    from database import hide_influencer
    hide_influencer(pk)
    return JSONResponse({"ok": True})


@app.post("/influencers/{pk}/unhide")
def unhide_one(pk: str, session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id)
    if not user: return JSONResponse({"error": "인증 필요"}, 403)
    from database import unhide_influencer
    unhide_influencer(pk)
    return JSONResponse({"ok": True})


@app.post("/influencers/{pk}/unban")
def unban_one(pk: str, session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id)
    if not user: return JSONResponse({"error": "인증 필요"}, 403)
    from database import unban_influencer
    unban_influencer(pk)
    return JSONResponse({"ok": True})


@app.get("/banned", response_class=HTMLResponse)
def banned_page(request: Request, session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id)
    if not user: return RedirectResponse("/login", 302)
    from database import get_banned_list
    banned = get_banned_list()
    return templates.TemplateResponse("banned.html", {
        "request": request, "user": user, "banned": banned,
    })

@app.get("/hidden", response_class=HTMLResponse)
def hidden_page(request: Request, session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id)
    if not user: return RedirectResponse("/login", 302)
    from database import get_hidden_list
    hidden = get_hidden_list()
    return templates.TemplateResponse("hidden.html", {
        "request": request, "user": user, "hidden": hidden,
    })

@app.get("/api/collect-job/{job_id}/users")
def collect_job_users(job_id: int, session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id)
    if not user: return JSONResponse({"error": "인증 필요"}, 403)
    from database import get_collect_job_users
    users = get_collect_job_users(job_id)
    for u in users:
        u["profile_pic_resolved"] = _pic(u)
    return JSONResponse(users)


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
    min_f: Optional[str] = None, max_f: Optional[str] = None,
    verified: int = 0, public_only: int = 0,
    main_category: str = "", can_live: int = 0, only_approved: int = 0,
    session_id: Optional[str] = Cookie(default=None)
):
    user = get_user(session_id) or get_adv_user(session_id)
    if not user: return RedirectResponse("/login", 302)

    import openpyxl
    from io import BytesIO

    _min_f = int(min_f) if min_f and min_f.isdigit() else None
    _max_f = int(max_f) if max_f and max_f.isdigit() else None
    _, rows = get_influencers(
        keyword=q, hashtag_filter=hashtag, min_f=_min_f, max_f=_max_f,
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
# 게시물 수집 (전체 갱신)
# ═══════════════════════════════════════════════════════

@app.get("/collect/posts", response_class=HTMLResponse)
def collect_posts_page(request: Request, session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id)
    if not user: return RedirectResponse("/login", 302)
    return templates.TemplateResponse("collect_posts.html", {
        "request": request, "user": user,
    })

@app.get("/target-extract", response_class=HTMLResponse)
def target_extract_page(request: Request, session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id)
    if not user: return RedirectResponse("/login", 302)
    return templates.TemplateResponse("target_extract.html", {"request": request, "user": user})


@app.get("/api/target-extract/stream")
def target_extract_stream(
    type: str = "followers", target: str = "",
    max_count: int = 500, save_to_db: str = "1",
    session_id: Optional[str] = Cookie(default=None),
):
    user = get_user(session_id)
    if not user: return JSONResponse({"error": "인증 필요"}, 403)
    if not target: return JSONResponse({"error": "대상을 입력해주세요"}, 400)

    import requests as req
    token = os.getenv("HIKERAPI_TOKEN", "")
    save = save_to_db == "1"

    def stream():
        extracted = 0
        new_cnt = 0
        dup_cnt = 0
        cursor = None
        p = {"extracted": 0, "new_cnt": 0, "dup_cnt": 0, "max_count": max_count,
             "done": False, "users": [], "status": "", "label": ""}

        try:
            # 팔로워/팔로잉: username → user_id 변환
            if type in ("followers", "following"):
                label = "팔로워" if type == "followers" else "팔로잉"
                p["status"] = f"@{target} 계정 정보 조회 중"
                p["label"] = f"@{target} {label} 추출 중..."
                yield f"data: {json.dumps(p, ensure_ascii=False)}\n\n"

                # username → user_id
                info_r = req.get("https://api.hikerapi.com/v1/user/by/username",
                                 params={"username": target, "access_key": token}, timeout=15)
                info = info_r.json()
                user_id = str(info.get("pk", ""))
                if not user_id:
                    p.update({"done": True, "error": True, "status": f"@{target} 계정을 찾을 수 없습니다"})
                    yield f"data: {json.dumps(p, ensure_ascii=False)}\n\n"
                    return

                endpoint = f"https://api.hikerapi.com/v1/user/{type}/chunk"

                while extracted < max_count:
                    params = {"user_id": user_id, "access_key": token}
                    if cursor:
                        params["max_id"] = cursor

                    r = req.get(endpoint, params=params, timeout=15)
                    data = r.json()

                    if not isinstance(data, list) or len(data) < 2:
                        break
                    users_list = data[0] if isinstance(data[0], list) else []
                    cursor = data[1] if len(data) > 1 else None

                    if not users_list:
                        break

                    page_users = []
                    from database import upsert_influencer, get_influencer
                    for u in users_list:
                        if not isinstance(u, dict):
                            continue
                        if extracted >= max_count:
                            break
                        pk = str(u.get("pk", ""))
                        uname = u.get("username", "")
                        fname = u.get("full_name", "")
                        pic = u.get("profile_pic_url", "")
                        is_private = u.get("is_private", False)

                        existing = get_influencer(pk)
                        is_new = not existing
                        pic_local = ""

                        if save and not is_private:
                            try:
                                from database import upload_profile_pic
                                if pic:
                                    stored = upload_profile_pic(pk, pic)
                                    if stored: pic_local = stored
                                inf_data = {"pk": pk, "username": uname, "full_name": fname,
                                            "profile_pic_url": pic}
                                if pic_local: inf_data["profile_pic_local"] = pic_local
                                upsert_influencer(inf_data)
                            except Exception:
                                pass

                        if is_new: new_cnt += 1
                        else: dup_cnt += 1
                        extracted += 1

                        page_users.append({"username": uname, "full_name": fname,
                                          "pic": _pic({"profile_pic_local": pic_local, "profile_pic_url": pic, "username": uname}), "is_new": is_new})

                    p.update({"extracted": extracted, "new_cnt": new_cnt, "dup_cnt": dup_cnt,
                              "users": page_users,
                              "status": f"@{target} {label} 추출 중 — {extracted}명",
                              "label": f"@{target} {label} 추출 중..."})
                    yield f"data: {json.dumps(p, ensure_ascii=False)}\n\n"

                    if not cursor:
                        break
                    time.sleep(0.5)

            # 댓글 작성자
            elif type == "commenters":
                code = target.split("/p/")[-1].split("/")[0] if "/p/" in target else target.split("/reel/")[-1].split("/")[0] if "/reel/" in target else target
                p["status"] = f"게시물 정보 조회 중"
                p["label"] = "댓글 작성자 추출 중..."
                yield f"data: {json.dumps(p, ensure_ascii=False)}\n\n"

                # code → media_id
                media_r = req.get("https://api.hikerapi.com/v1/media/by/code",
                                  params={"code": code, "access_key": token}, timeout=15)
                media = media_r.json()
                media_id = str(media.get("pk", ""))
                if not media_id:
                    p.update({"done": True, "error": True, "status": "게시물을 찾을 수 없습니다"})
                    yield f"data: {json.dumps(p, ensure_ascii=False)}\n\n"
                    return

                endpoint = "https://api.hikerapi.com/v1/media/comments/chunk"
                seen_pks = set()

                while extracted < max_count:
                    params = {"id": media_id, "access_key": token}
                    if cursor:
                        params["max_id"] = cursor

                    r = req.get(endpoint, params=params, timeout=15)
                    data = r.json()

                    comments = []
                    next_cursor = None
                    if isinstance(data, list) and len(data) >= 2:
                        comments = data[0] if isinstance(data[0], list) else []
                        next_cursor = data[1] if len(data) > 1 else None
                    elif isinstance(data, dict):
                        comments = data.get("comments", [])
                        next_cursor = data.get("next_min_id")

                    if not comments:
                        break

                    page_users = []
                    from database import upsert_influencer, get_influencer
                    for c in comments:
                        if not isinstance(c, dict): continue
                        u = c.get("user", {})
                        if not isinstance(u, dict): continue
                        pk = str(u.get("pk", ""))
                        if not pk or pk in seen_pks: continue
                        seen_pks.add(pk)
                        if extracted >= max_count: break

                        uname = u.get("username", "")
                        fname = u.get("full_name", "")
                        pic = u.get("profile_pic_url", "")

                        existing = get_influencer(pk)
                        is_new = not existing

                        if save:
                            try:
                                upsert_influencer({"pk": pk, "username": uname,
                                                   "full_name": fname, "profile_pic_url": pic})
                            except Exception: pass

                        if is_new: new_cnt += 1
                        else: dup_cnt += 1
                        extracted += 1
                        page_users.append({"username": uname, "full_name": fname,
                                          "pic": _pic({"profile_pic_url": pic, "username": uname}), "is_new": is_new})

                    cursor = next_cursor
                    p.update({"extracted": extracted, "new_cnt": new_cnt, "dup_cnt": dup_cnt,
                              "users": page_users,
                              "status": f"댓글 작성자 추출 중 — {extracted}명"})
                    yield f"data: {json.dumps(p, ensure_ascii=False)}\n\n"

                    if not cursor: break
                    time.sleep(0.5)

            # 계정 전체 게시물 댓글 작성자
            elif type == "all_commenters":
                p["status"] = f"@{target} 계정 정보 조회 중"
                p["label"] = f"@{target} 전체 댓글 작성자 추출 중..."
                yield f"data: {json.dumps(p, ensure_ascii=False)}\n\n"

                info_r = req.get("https://api.hikerapi.com/v1/user/by/username",
                                 params={"username": target, "access_key": token}, timeout=15)
                info = info_r.json()
                user_id = str(info.get("pk", ""))
                if not user_id:
                    p.update({"done": True, "error": True, "status": f"@{target} 계정을 찾을 수 없습니다"})
                    yield f"data: {json.dumps(p, ensure_ascii=False)}\n\n"
                    return

                # 게시물 목록 가져오기
                medias_r = req.get("https://api.hikerapi.com/v1/user/medias/chunk",
                                   params={"user_id": user_id, "access_key": token}, timeout=15)
                medias_data = medias_r.json()
                media_items = []
                if isinstance(medias_data, list) and len(medias_data) >= 1:
                    media_items = medias_data[0] if isinstance(medias_data[0], list) else medias_data

                if not media_items:
                    p.update({"done": True, "error": True, "status": "게시물을 찾을 수 없습니다"})
                    yield f"data: {json.dumps(p, ensure_ascii=False)}\n\n"
                    return

                seen_pks = set()
                from database import upsert_influencer, get_influencer

                for mi, media in enumerate(media_items):
                    if extracted >= max_count:
                        break
                    if not isinstance(media, dict):
                        continue
                    media_id = str(media.get("pk", ""))
                    if not media_id:
                        continue

                    p["status"] = f"게시물 {mi+1}/{len(media_items)} 댓글 추출 중 — {extracted}명"
                    p["users"] = []
                    yield f"data: {json.dumps(p, ensure_ascii=False)}\n\n"

                    comment_cursor = None
                    while extracted < max_count:
                        cparams = {"id": media_id, "access_key": token}
                        if comment_cursor:
                            cparams["max_id"] = comment_cursor

                        cr = req.get("https://api.hikerapi.com/v1/media/comments/chunk",
                                     params=cparams, timeout=15)
                        cdata = cr.json()

                        comments = []
                        next_c = None
                        if isinstance(cdata, list) and len(cdata) >= 2:
                            comments = cdata[0] if isinstance(cdata[0], list) else []
                            next_c = cdata[1] if len(cdata) > 1 else None
                        elif isinstance(cdata, dict):
                            comments = cdata.get("comments", [])
                            next_c = cdata.get("next_min_id")

                        if not comments:
                            break

                        page_users = []
                        for c in comments:
                            if not isinstance(c, dict): continue
                            u = c.get("user", {})
                            if not isinstance(u, dict): continue
                            pk = str(u.get("pk", ""))
                            if not pk or pk in seen_pks: continue
                            seen_pks.add(pk)
                            if extracted >= max_count: break

                            uname = u.get("username", "")
                            fname = u.get("full_name", "")
                            pic = u.get("profile_pic_url", "")

                            existing = get_influencer(pk)
                            is_new = not existing

                            if save:
                                try:
                                    upsert_influencer({"pk": pk, "username": uname,
                                                       "full_name": fname, "profile_pic_url": pic})
                                except Exception: pass

                            if is_new: new_cnt += 1
                            else: dup_cnt += 1
                            extracted += 1
                            page_users.append({"username": uname, "full_name": fname,
                                              "pic": _pic({"profile_pic_url": pic, "username": uname}), "is_new": is_new})

                        comment_cursor = next_c
                        if page_users:
                            p.update({"extracted": extracted, "new_cnt": new_cnt, "dup_cnt": dup_cnt,
                                      "users": page_users,
                                      "status": f"게시물 {mi+1}/{len(media_items)} — {extracted}명"})
                            yield f"data: {json.dumps(p, ensure_ascii=False)}\n\n"

                        if not comment_cursor: break
                        time.sleep(0.3)

                    time.sleep(0.3)

            # 좋아요 누른 사람
            elif type == "likers":
                code = target.split("/p/")[-1].split("/")[0] if "/p/" in target else target.split("/reel/")[-1].split("/")[0] if "/reel/" in target else target
                p["status"] = "게시물 정보 조회 중"
                p["label"] = "좋아요 누른 사람 추출 중..."
                yield f"data: {json.dumps(p, ensure_ascii=False)}\n\n"

                media_r = req.get("https://api.hikerapi.com/v1/media/by/code",
                                  params={"code": code, "access_key": token}, timeout=15)
                media = media_r.json()
                media_id = str(media.get("pk", ""))
                if not media_id:
                    p.update({"done": True, "error": True, "status": "게시물을 찾을 수 없습니다"})
                    yield f"data: {json.dumps(p, ensure_ascii=False)}\n\n"
                    return

                likers_r = req.get("https://api.hikerapi.com/v1/media/likers",
                                   params={"id": media_id, "access_key": token}, timeout=15)
                likers_data = likers_r.json()
                users_list = likers_data if isinstance(likers_data, list) else likers_data.get("users", [])

                page_users = []
                from database import upsert_influencer, get_influencer
                for u in users_list:
                    if not isinstance(u, dict): continue
                    if extracted >= max_count: break
                    pk = str(u.get("pk", ""))
                    uname = u.get("username", "")
                    fname = u.get("full_name", "")
                    pic = u.get("profile_pic_url", "")

                    existing = get_influencer(pk)
                    is_new = not existing

                    if save:
                        try:
                            upsert_influencer({"pk": pk, "username": uname,
                                               "full_name": fname, "profile_pic_url": pic})
                        except Exception: pass

                    if is_new: new_cnt += 1
                    else: dup_cnt += 1
                    extracted += 1
                    page_users.append({"username": uname, "full_name": fname,
                                      "pic": _pic({"profile_pic_url": pic, "username": uname}), "is_new": is_new})

                p.update({"extracted": extracted, "new_cnt": new_cnt, "dup_cnt": dup_cnt,
                          "users": page_users,
                          "status": f"좋아요 추출 완료 — {extracted}명"})
                yield f"data: {json.dumps(p, ensure_ascii=False)}\n\n"

            # 완료
            p.update({"done": True, "users": [],
                      "status": f"완료 — 추출 {extracted}명 (신규 {new_cnt} / 중복 {dup_cnt})"})
            yield f"data: {json.dumps(p, ensure_ascii=False)}\n\n"

        except Exception as e:
            log.error(f"타겟 추출 에러: {e}")
            p.update({"done": True, "error": True, "users": [],
                      "status": f"오류: {str(e)[:100]}"})
            yield f"data: {json.dumps(p, ensure_ascii=False)}\n\n"

    return StreamingResponse(stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


@app.get("/api/hashtag/search")
def hashtag_search_api(q: str = "", session_id: Optional[str] = Cookie(default=None)):
    """해시태그 검색 → 연관 해시태그 + 게시물 수"""
    user = get_user(session_id)
    if not user: return JSONResponse({"error": "인증 필요"}, 403)
    if not q: return JSONResponse([])
    import requests
    token = os.getenv("HIKERAPI_TOKEN", "")
    try:
        r = requests.get("https://api.hikerapi.com/v1/search/hashtags",
                         params={"query": q, "access_key": token}, timeout=10)
        data = r.json()
        if isinstance(data, list):
            return JSONResponse(data[:20])
    except Exception:
        pass
    return JSONResponse([])


@app.get("/refresh", response_class=HTMLResponse)
def refresh_page(request: Request, session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id)
    if not user: return RedirectResponse("/login", 302)
    cutoff = time.time() - 86400 * 30
    from database import get_banned_pks
    banned_pks = get_banned_pks()
    all_infs = get_influencers(per_page=99999, page=1)
    items = all_infs.get("items", [])
    stale = [r for r in items
             if (not r.get("stats_updated_at") or r["stats_updated_at"] < cutoff)
             and str(r.get("pk","")) not in banned_pks]
    return templates.TemplateResponse("refresh.html", {
        "request": request, "user": user,
        "total_count": len(items), "stale_count": len(stale),
    })


@app.post("/refresh/start")
def refresh_start(session_id: Optional[str] = Cookie(default=None)):
    """게시물 수집 시작 → SSE 스트림으로 전환"""
    user = get_user(session_id)
    if not user: return JSONResponse({"error": "인증 필요"}, 403)
    return JSONResponse({"ok": True})

@app.get("/refresh/stream")
def refresh_stream(session_id: Optional[str] = Cookie(default=None)):
    """SSE 스트림 안에서 직접 게시물 갱신 실행 (서버리스 호환)"""
    user = get_user(session_id)
    if not user: return JSONResponse({"error": "인증 필요"}, 403)

    from crawler import crawl_user_detail

    def stream():
        try:
            cutoff = time.time() - 86400 * 30  # 30일 이상 지난 것만 갱신
            from database import get_banned_pks
            banned_pks = get_banned_pks()
            all_infs = get_influencers(per_page=99999, page=1)
            rows = [r for r in all_infs.get("items", [])
                    if (not r.get("stats_updated_at") or r["stats_updated_at"] < cutoff)
                    and str(r.get("pk","")) not in banned_pks]

            total = len(rows)
            success = fail = 0

            for i, row in enumerate(rows):
                pk = row.get("pk") or row.get("id")
                uname = row.get("username", "")
                followers = row.get("follower_count", 0)

                try:
                    ok = crawl_user_detail(None, str(pk), uname, followers or 0)
                    if ok:
                        success += 1
                    else:
                        fail += 1
                except Exception:
                    fail += 1

                p = {"running": True, "total": total, "done": i + 1,
                     "success": success, "fail": fail, "current_username": uname}
                yield f"data: {json.dumps(p, ensure_ascii=False)}\n\n"
                time.sleep(0.5)

            p = {"running": False, "total": total, "done": total,
                 "success": success, "fail": fail}
            yield f"data: {json.dumps(p, ensure_ascii=False)}\n\n"

        except Exception as e:
            log.error(f"게시물 갱신 에러: {e}")
            yield f"data: {json.dumps({'running': False, 'error': str(e)}, ensure_ascii=False)}\n\n"

    return StreamingResponse(stream(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

@app.get("/refresh/status")
def refresh_status(session_id: Optional[str] = Cookie(default=None)):
    """하위호환용 — SSE 방식으로 전환됨"""
    user = get_user(session_id)
    if not user: return JSONResponse({"error": "인증 필요"}, 403)
    return JSONResponse({})


# ═══════════════════════════════════════════════════════
# 자동화 (Cron)
# ═══════════════════════════════════════════════════════

@app.get("/api/cron/auto")
def cron_auto(request: Request):
    """Vercel Cron 또는 외부 cron이 호출하는 자동화 엔드포인트.
    1) auto_collect 해시태그 1개 → 신규 계정 10명 수집
    2) 미갱신 인플루언서 5명 → 게시물 갱신
    """
    # 인증: Vercel CRON_SECRET 또는 커스텀 헤더
    cron_secret = os.getenv("CRON_SECRET", "")
    auth = request.headers.get("authorization", "")
    if cron_secret and auth != f"Bearer {cron_secret}":
        raise HTTPException(401, "Unauthorized")
    if not cron_secret:
        raise HTTPException(403, "CRON_SECRET 미설정")

    from crawler import cron_collect_batch, cron_refresh_batch

    results = {"collect": None, "refresh": None}

    # ① 계정 수집: auto_collect 해시태그 중 가장 오래된 것
    auto_tags = get_auto_hashtags()
    if auto_tags:
        tag = auto_tags[0]
        tag_name = tag.get("name", "")
        try:
            r = cron_collect_batch(tag_name, target_users=10, search_type="recent")
            results["collect"] = {"hashtag": tag_name, **r}
            add_cron_log("collect", "error" if r.get("error") else "ok",
                         hashtag=tag_name, details=r)
        except Exception as e:
            results["collect"] = {"hashtag": tag_name, "error": str(e)}
            add_cron_log("collect", "error", hashtag=tag_name, details={"error": str(e)})

    # ② 게시물 갱신
    try:
        r = cron_refresh_batch(batch_size=5, stale_hours=720)
        results["refresh"] = r
        add_cron_log("refresh", "ok", details=r)
    except Exception as e:
        results["refresh"] = {"error": str(e)}
        add_cron_log("refresh", "error", details={"error": str(e)})

    return JSONResponse(results)


@app.get("/api/hiker-balance")
def hiker_balance(session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id)
    if not user: return JSONResponse({"error": "인증 필요"}, 403)
    token = os.getenv("HIKERAPI_TOKEN", "").strip()
    if not token:
        return JSONResponse({"error": "HIKERAPI_TOKEN 미설정"}, 400)
    try:
        import requests as _r
        r = _r.get("https://api.hikerapi.com/sys/balance",
                    headers={"x-access-key": token}, timeout=10)
        return JSONResponse(r.json())
    except Exception as e:
        return JSONResponse({"error": str(e)}, 500)


@app.post("/api/cron/manual-run")
def cron_manual_run(session_id: Optional[str] = Cookie(default=None)):
    """관리자가 대시보드에서 수동으로 cron 1회 실행"""
    user = get_user(session_id)
    if not user: return JSONResponse({"error": "인증 필요"}, 403)

    from crawler import cron_collect_batch, cron_refresh_batch

    results = {"collect": None, "refresh": None}

    auto_tags = get_auto_hashtags()
    if auto_tags:
        tag = auto_tags[0]
        tag_name = tag.get("name", "")
        try:
            r = cron_collect_batch(tag_name, target_users=10, search_type="recent")
            results["collect"] = {"hashtag": tag_name, **r}
            add_cron_log("collect", "error" if r.get("error") else "ok",
                         hashtag=tag_name, details=r)
        except Exception as e:
            results["collect"] = {"hashtag": tag_name, "error": str(e)}
            add_cron_log("collect", "error", hashtag=tag_name, details={"error": str(e)})

    try:
        r = cron_refresh_batch(batch_size=5, stale_hours=720)
        results["refresh"] = r
        add_cron_log("refresh", "ok", details=r)
    except Exception as e:
        results["refresh"] = {"error": str(e)}
        add_cron_log("refresh", "error", details={"error": str(e)})

    return JSONResponse(results)


@app.get("/automation", response_class=HTMLResponse)
def automation_page(request: Request, session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id)
    if not user: return RedirectResponse("/login", 302)
    stats = get_stats()
    auto_tags = get_auto_hashtags()
    cron_logs = get_cron_logs(30)
    return templates.TemplateResponse("automation.html", {
        "request": request, "user": user,
        "stats": stats, "auto_tags": auto_tags, "cron_logs": cron_logs,
        "cron_secret_set": bool(os.getenv("CRON_SECRET", "")),
    })


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
    from database import get_collect_jobs
    htags = get_hashtags()
    jobs = get_collect_jobs(limit=20)
    return templates.TemplateResponse("collect.html", {
        "request": request, "user": user,
        "hashtags": [r.get("name","") if isinstance(r, dict) else r["name"] for r in htags],
        "jobs": jobs,
    })

@app.post("/collect/start")
def collect_start(hashtag: str = Form(...), requested_count: int = Form(default=500),
                  target_users: int = Form(default=0),
                  search_type: str = Form(default="recent"),
                  session_id: Optional[str] = Cookie(default=None)):
    user = get_user(session_id)
    if not user: return HTMLResponse("인증 필요", 403)
    hashtag = hashtag.strip().lstrip("#")
    job_id = str(uuid.uuid4())
    return HTMLResponse(job_id)

@app.get("/collect/progress/{job_id}")
def collect_progress(job_id: str,
                     hashtag: str = Query(default=""),
                     target_users: int = Query(default=30),
                     search_type: str = Query(default="recent"),
                     resume_from: str = Query(default=""),
                     resume_new: int = Query(default=0),
                     resume_updated: int = Query(default=0),
                     resume_posts: int = Query(default=0),
                     resume_page: int = Query(default=0),
                     session_id: Optional[str] = Cookie(default=None)):
    """SSE 스트림 안에서 직접 수집 실행 (서버리스 호환). resume_from으로 이어서 수집 가능."""
    user = get_user(session_id)
    if not user:
        return JSONResponse({"error": "인증 필요"}, 403)
    if not re.match(r'^[a-f0-9\-]{36}$', job_id):
        return JSONResponse({"error": "잘못된 job_id"}, 400)

    from database import upsert_influencer, add_collect_job, update_collect_job, get_existing_pks, get_banned_pks

    def stream():
        if not hashtag:
            yield f"data: {json.dumps({'done': True, 'error': '해시태그 없음'}, ensure_ascii=False)}\n\n"
            return

        try:
            update_hashtag_status(hashtag, "running")
        except Exception:
            try: db_add_hashtag(hashtag)
            except: pass

        job_db_id = None
        try:
            job_db_id = add_collect_job(hashtag, "running", target_users, search_type)
        except Exception:
            pass

        p = {"hashtag": hashtag, "posts": resume_posts, "new": resume_new,
             "updated": resume_updated, "done": False, "error": None,
             "status": "게시물 검색 중", "target": target_users}
        yield f"data: {json.dumps(p, ensure_ascii=False)}\n\n"

        BATCH_PAGES = 8  # 배치당 페이지 수 (Vercel 타임아웃 전에 완료)

        try:
            from crawler import _hiker_hashtag_medias_page

            existing_pks = get_existing_pks()
            banned_pks = get_banned_pks()

            seen_pks = set()
            collected_pk_list = []
            total_medias = resume_posts  # 검색한 게시물 총 수
            new_cnt = resume_new
            updated_cnt = resume_updated
            max_id = resume_from or None
            endpoint = "top" if search_type == "top" else "recent"
            page_num = resume_page
            batch_done = 0
            last_next_id = None
            no_data = False

            while new_cnt < target_users and batch_done < BATCH_PAGES:
                page_num += 1
                batch_done += 1
                _page_start = time.time()

                try:
                    items, next_id = _hiker_hashtag_medias_page(hashtag, endpoint, max_id)
                except Exception as api_err:
                    # API 에러 시 1회 재시도
                    time.sleep(2)
                    try:
                        items, next_id = _hiker_hashtag_medias_page(hashtag, endpoint, max_id)
                    except Exception:
                        raise api_err

                if not items and not max_id:
                    raise Exception("HikerAPI 해시태그 조회 실패 — HIKERAPI_TOKEN을 확인하세요")
                if not items:
                    time.sleep(1)
                    items, next_id = _hiker_hashtag_medias_page(hashtag, endpoint, max_id)
                    if not items:
                        no_data = True
                        break

                total_medias += len(items)
                page_users = []
                for m in items:
                    if not isinstance(m, dict):
                        continue
                    user_data = m.get("user") or {}
                    if not isinstance(user_data, dict):
                        continue
                    upk = user_data.get("pk")
                    if not upk:
                        continue
                    pk_str = str(upk)
                    if pk_str in seen_pks:
                        continue
                    seen_pks.add(pk_str)
                    if pk_str in banned_pks:
                        continue
                    uname = user_data.get("username", "")
                    fname = user_data.get("full_name", "")
                    pic = str(user_data.get("profile_pic_url", "") or "")
                    is_new = pk_str not in existing_pks

                    # 프로필 사진 Supabase Storage 업로드
                    pic_local = ""
                    if pic:
                        try:
                            from database import upload_profile_pic
                            stored = upload_profile_pic(pk_str, pic)
                            if stored:
                                pic_local = stored
                        except Exception:
                            pass

                    try:
                        inf_data = {
                            "pk": pk_str, "username": uname,
                            "full_name": fname, "profile_pic_url": pic,
                            "hashtag": hashtag,
                        }
                        if pic_local:
                            inf_data["profile_pic_local"] = pic_local
                        upsert_influencer(inf_data)
                        collected_pk_list.append(pk_str)
                        if is_new:
                            new_cnt += 1
                            existing_pks.add(pk_str)
                        else:
                            updated_cnt += 1
                    except Exception:
                        pass

                    page_users.append({
                        "username": uname, "full_name": fname,
                        "pic": _pic({"profile_pic_local": pic_local, "profile_pic_url": pic, "username": uname}),
                        "is_new": is_new,
                    })

                p.update({"posts": total_medias, "new": new_cnt, "updated": updated_cnt,
                          "status": f"페이지 {page_num} — 신규 {new_cnt}명 / 중복 {updated_cnt}명 / 목표 {target_users}명",
                          "page": page_num, "page_items": len(items),
                          "has_next": bool(next_id),
                          "next_id": next_id or "",
                          "users": page_users})
                yield f"data: {json.dumps(p, ensure_ascii=False)}\n\n"

                if not next_id:
                    no_data = True
                    break
                max_id = next_id
                last_next_id = next_id

                base_wait = min(0.3 + page_num * 0.05, 2.0)
                elapsed = time.time() - _page_start
                if elapsed > 5:
                    base_wait = max(base_wait, elapsed * 0.5)
                time.sleep(base_wait)

            # 배치 결과 저장
            try:
                update_collect_job(job_db_id, status="done",
                    collected_posts=total_medias, new_users=new_cnt,
                    updated_users=updated_cnt, finished_at=time.time(),
                    collected_pks=json.dumps(collected_pk_list))
                update_hashtag_status(hashtag, "idle")
            except Exception:
                pass

            # 목표 달성 or 더 이상 데이터 없음 → 진짜 완료
            # 아직 목표 미달 + 다음 페이지 있음 → continue 이벤트 (클라이언트가 자동 이어서 요청)
            if new_cnt >= target_users or no_data:
                reason = "목표 달성" if new_cnt >= target_users else f"해시태그 끝 ({page_num}페이지)"
                p.update({"done": True, "new": new_cnt, "updated": updated_cnt,
                          "status": f"완료 — 신규 {new_cnt}명 / 중복 {updated_cnt}명 ({reason})",
                          "page": page_num})
            else:
                p.update({"done": False, "has_more": True, "new": new_cnt, "updated": updated_cnt,
                          "next_id": last_next_id or "", "page": page_num,
                          "posts": total_medias,
                          "status": f"배치 {page_num}페이지 완료 — 신규 {new_cnt}명 / 중복 {updated_cnt}명 / 목표 {target_users}명 (자동 계속)"})
            yield f"data: {json.dumps(p, ensure_ascii=False)}\n\n"

        except Exception as e:
            log.error(f"[{hashtag}] 수집 에러: {e}")
            try:
                if job_db_id:
                    update_collect_job(job_db_id, status="error", error_msg=str(e)[:200], finished_at=time.time())
                update_hashtag_status(hashtag, "error")
            except Exception:
                pass
            p.update({"done": True, "error": str(e), "status": "수집 실패"})
            yield f"data: {json.dumps(p, ensure_ascii=False)}\n\n"

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
    ip = _get_client_ip(request)
    if not _check_login_allowed(ip):
        log.warning(f"광고주 로그인 brute-force 차단: {ip}")
        return templates.TemplateResponse("advertiser_login.html", {
            "request": request, "error": "너무 많은 로그인 시도입니다. 15분 후 다시 시도해주세요."
        }, status_code=429)
    row = get_advertiser_by_username(username.strip())
    if row and bcrypt.checkpw(password.encode(), (row.get("password_hash") or "").encode()):
        _login_attempts.pop(ip, None)
        token = _make_jwt({"role": "advertiser", "username": username, "adv_id": row.get("id")})
        res = RedirectResponse("/advertiser", status_code=302)
        res.set_cookie("adv_session_id", token, httponly=True, max_age=604800,
                        samesite="lax", secure=_IS_PROD)
        return res
    _record_login_attempt(ip)
    remaining = _LOGIN_MAX_ATTEMPTS - len(_login_attempts.get(ip, []))
    return templates.TemplateResponse("advertiser_login.html", {
        "request": request, "error": f"아이디 또는 비밀번호가 올바르지 않습니다. (남은 시도: {remaining}회)"
    }, status_code=400)

@app.get("/advertiser/logout")
def adv_logout(adv_session_id: Optional[str] = Cookie(default=None)):
    adv_sessions.pop(adv_session_id, None)
    res = RedirectResponse("/advertiser/login", 302)
    res.delete_cookie("adv_session_id")
    return res

# ─── 찜 API ───────────────────────────────────────────────────────────
@app.post("/advertiser/favorites/toggle")
def adv_toggle_favorite(
    influencer_pk: str = Form(...),
    adv_session_id: Optional[str] = Cookie(default=None)
):
    adv = get_adv_user(adv_session_id)
    if not adv: return JSONResponse({"error": "로그인 필요"}, 401)
    added = toggle_favorite(adv["id"], influencer_pk)
    return JSONResponse({"ok": True, "favorited": added})

@app.get("/advertiser/favorites", response_class=HTMLResponse)
def adv_favorites_page(request: Request, adv_session_id: Optional[str] = Cookie(default=None)):
    adv = get_adv_user(adv_session_id)
    if not adv: return RedirectResponse("/advertiser/login", 302)
    favs = get_favorites(adv["id"])
    pks = [f["influencer_pk"] for f in favs]
    rows = [get_influencer(pk) for pk in pks if pk]
    rows = [r for r in rows if r]
    campaigns = get_campaigns(adv["id"])
    return templates.TemplateResponse("advertiser_favorites.html", {
        "request": request, "adv": adv, "rows": rows,
        "fav_pks": set(pks), "campaigns": campaigns,
    })


# ─── 캠페인 API ────────────────────────────────────────────────────────
@app.post("/advertiser/campaigns/create")
def adv_create_campaign(
    name: str = Form(...),
    description: str = Form(default=""),
    budget: int = Form(default=0),
    adv_session_id: Optional[str] = Cookie(default=None)
):
    adv = get_adv_user(adv_session_id)
    if not adv: return JSONResponse({"error": "로그인 필요"}, 401)
    camp_id = create_campaign(adv["id"], name, description, budget)
    return JSONResponse({"ok": True, "id": camp_id})

@app.get("/advertiser/campaigns", response_class=HTMLResponse)
def adv_campaigns_page(request: Request, adv_session_id: Optional[str] = Cookie(default=None)):
    adv = get_adv_user(adv_session_id)
    if not adv: return RedirectResponse("/advertiser/login", 302)
    camps = get_campaigns(adv["id"])
    return templates.TemplateResponse("advertiser_campaigns.html", {
        "request": request, "adv": adv, "campaigns": camps,
    })

@app.get("/advertiser/campaigns/{camp_id}", response_class=HTMLResponse)
def adv_campaign_detail(camp_id: int, request: Request, adv_session_id: Optional[str] = Cookie(default=None)):
    adv = get_adv_user(adv_session_id)
    if not adv: return RedirectResponse("/advertiser/login", 302)
    camp = get_campaign(camp_id)
    if not camp or camp.get("advertiser_id") != adv["id"]: raise HTTPException(403)
    cinfs = get_campaign_influencers(camp_id)
    # 인플루언서 상세 정보 조인
    rows = []
    total_price = 0
    for ci in cinfs:
        inf = get_influencer(ci["influencer_pk"])
        if inf:
            manual = get_manual(ci["influencer_pk"])
            rows.append({**ci, "inf": inf, "manual": manual})
            total_price += ci.get("price", 0) or 0
    return templates.TemplateResponse("advertiser_campaign_detail.html", {
        "request": request, "adv": adv, "camp": camp,
        "rows": rows, "total_price": total_price,
    })

@app.post("/advertiser/campaigns/{camp_id}/add")
def adv_add_to_campaign(
    camp_id: int,
    influencer_pk: str = Form(...),
    content_type: str = Form(default="feed"),
    price: int = Form(default=0),
    note: str = Form(default=""),
    adv_session_id: Optional[str] = Cookie(default=None)
):
    adv = get_adv_user(adv_session_id)
    if not adv: return JSONResponse({"error": "로그인 필요"}, 401)
    camp = get_campaign(camp_id)
    if not camp or camp.get("advertiser_id") != adv["id"]: return JSONResponse({"error": "권한 없음"}, 403)
    added = add_to_campaign(camp_id, influencer_pk, content_type, price, note)
    return JSONResponse({"ok": True, "added": added})

@app.post("/advertiser/campaigns/{camp_id}/remove")
def adv_remove_from_campaign(
    camp_id: int,
    influencer_pk: str = Form(...),
    adv_session_id: Optional[str] = Cookie(default=None)
):
    adv = get_adv_user(adv_session_id)
    if not adv: return JSONResponse({"error": "로그인 필요"}, 401)
    remove_from_campaign(camp_id, influencer_pk)
    return JSONResponse({"ok": True})

@app.post("/advertiser/campaigns/{camp_id}/delete")
def adv_delete_campaign(camp_id: int, adv_session_id: Optional[str] = Cookie(default=None)):
    adv = get_adv_user(adv_session_id)
    if not adv: return JSONResponse({"error": "로그인 필요"}, 401)
    delete_campaign(camp_id)
    return RedirectResponse("/advertiser/campaigns", 302)


@app.get("/advertiser", response_class=HTMLResponse)
def adv_dashboard(
    request: Request,
    q: str = "",
    hashtag: str = "",
    min_f: Optional[str] = None,
    max_f: Optional[str] = None,
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

    _min_f = int(min_f) if min_f and min_f.isdigit() else None
    _max_f = int(max_f) if max_f and max_f.isdigit() else None

    # 광고주 접근 해시태그 필터링
    allowed_hashtags = adv.get("hashtag_access", "")
    effective_hashtag = hashtag
    if allowed_hashtags and not hashtag:
        effective_hashtag = allowed_hashtags.split(",")[0].strip()

    only_approved = bool(adv.get("only_approved", 1))
    min_f_eff = max(_min_f or 0, adv.get("min_followers", 0)) or None

    total, rows = get_influencers(
        keyword=q, hashtag_filter=effective_hashtag,
        min_f=min_f_eff, max_f=_max_f,
        only_approved=only_approved,
        main_category=main_category, can_live=bool(can_live),
        sort=sort, order=order, page=page, per_page=per_page
    )
    total_pages = max(1, (total + per_page - 1) // per_page)
    allowed_list = [h.strip() for h in allowed_hashtags.split(",") if h.strip()] if allowed_hashtags else []
    adv_id = adv.get("id", 0)
    fav_pks = get_favorite_pks(adv_id)
    campaigns = get_campaigns(adv_id)

    return templates.TemplateResponse("advertiser_dashboard.html", {
        "request": request, "adv": adv,
        "rows": rows, "total": total, "total_pages": total_pages,
        "page": page, "per_page": per_page,
        "q": q, "hashtag": effective_hashtag,
        "min_f": min_f, "max_f": max_f,
        "main_category": main_category, "can_live": can_live,
        "sort": sort, "order": order,
        "allowed_hashtags": allowed_list,
        "fav_pks": fav_pks,
        "campaigns": campaigns,
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
    try:
        raw = json.loads(inf.get("top_hashtags") or "[]")
        inf["top_hashtags"] = [{"tag": t, "count": 1} for t in raw] if raw and isinstance(raw[0], str) else raw
    except:
        inf["top_hashtags"] = []
    posts = get_influencer_posts(pk)
    recent_reels = get_influencer_reels(pk, sort="recent", limit=12)
    popular_reels = get_influencer_reels(pk, sort="popular", limit=12)
    # 활동 유형 분석 (광고주 뷰에서도 동일하게 표시)
    activity = _analyze_activity(inf.get("top_hashtags", []))
    return templates.TemplateResponse("influencer_detail.html", {
        "request": request, "adv": adv, "user": None,
        "inf": inf, "manual": manual_safe, "posts": posts,
        "is_advertiser_view": True,
        "activity": activity,
        "recent_reels": recent_reels,
        "popular_reels": popular_reels,
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
        "extension_api_key": EXTENSION_API_KEY,
        "plans": PLANS,
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
    keys = {
        "INSTA_USERNAME": insta_username, "INSTA_PASSWORD": insta_password,
        "INSTA_TOTP": insta_totp,
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
