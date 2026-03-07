import time, logging, pyotp, os, json, re
from collections import Counter
from datetime import datetime
import requests as req_lib
from database import (upsert_influencer, update_influencer_stats, upsert_post,
                      update_hashtag_status, update_collect_job)

log = logging.getLogger(__name__)

progress: dict = {}   # job_id → {status, hashtag, posts, new, updated, total, done}
refresh_progress: dict = {}  # single key "current" → {done, total, current_user, ...}

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
PROFILE_PIC_DIR = os.path.join(DATA_DIR, "profile_pics")
POSTS_DIR = os.path.join(DATA_DIR, "posts")
try:
    os.makedirs(PROFILE_PIC_DIR, exist_ok=True)
    os.makedirs(POSTS_DIR, exist_ok=True)
except OSError:
    DATA_DIR = "/tmp/data"
    PROFILE_PIC_DIR = "/tmp/data/profile_pics"
    POSTS_DIR = "/tmp/data/posts"
    os.makedirs(PROFILE_PIC_DIR, exist_ok=True)
    os.makedirs(POSTS_DIR, exist_ok=True)

# ─── HikerAPI (SaaS) ───────────────────────────────────────────
_hiker_client = None

def _get_hiker():
    """HikerAPI 클라이언트 싱글톤. 토큰 없으면 None 반환."""
    global _hiker_client
    if _hiker_client is not None:
        return _hiker_client
    token = os.environ.get("HIKERAPI_TOKEN", "").strip()
    if not token:
        return None
    try:
        from hikerapi import Client as HikerClient
        _hiker_client = HikerClient(token=token, timeout=30)
        log.info("HikerAPI 클라이언트 초기화 완료")
        return _hiker_client
    except Exception as e:
        log.warning(f"HikerAPI 초기화 실패: {e}")
        return None


def _hiker_user_info(username: str) -> dict | None:
    """HikerAPI로 유저 프로필 조회. 실패 시 None."""
    hk = _get_hiker()
    if not hk:
        return None
    try:
        data = hk.user_by_username_v1(username)
        if isinstance(data, dict) and data.get("pk"):
            return data
        log.warning(f"[HikerAPI] 유저 조회 응답 비정상: {username}")
    except Exception as e:
        log.warning(f"[HikerAPI] 유저 조회 실패 {username}: {e}")
    return None


def _hiker_user_info_by_id(user_id: str) -> dict | None:
    """HikerAPI로 pk 기반 유저 프로필 조회."""
    hk = _get_hiker()
    if not hk:
        return None
    try:
        data = hk.user_by_id_v1(str(user_id))
        if isinstance(data, dict) and data.get("pk"):
            return data
    except Exception as e:
        log.warning(f"[HikerAPI] 유저 ID 조회 실패 {user_id}: {e}")
    return None


def _hiker_user_medias(user_id: str, amount: int = 50) -> list | None:
    """HikerAPI로 유저 게시물 조회 (페이징). 실패 시 None."""
    hk = _get_hiker()
    if not hk:
        return None
    try:
        all_medias = []
        end_cursor = None
        while len(all_medias) < amount:
            resp = hk.user_medias_chunk_v1(str(user_id), end_cursor=end_cursor)
            items = []
            npid = None
            # 응답 형태: [items_list, next_page_id] 또는 dict
            if isinstance(resp, list) and len(resp) == 2 and isinstance(resp[0], list):
                items = resp[0]
                npid = resp[1]
            elif isinstance(resp, dict):
                if "response" in resp:
                    items = resp["response"].get("items", [])
                elif "items" in resp:
                    items = resp["items"]
                npid = resp.get("next_page_id") or resp.get("next_max_id")
            if not items:
                break
            all_medias.extend(items)
            end_cursor = npid
            if not end_cursor:
                break
        return all_medias[:amount]
    except Exception as e:
        log.warning(f"[HikerAPI] 게시물 조회 실패 {user_id}: {e}")
    return None


def _hiker_hashtag_medias(hashtag: str, amount: int = 100, search_type: str = "recent") -> list | None:
    """HikerAPI로 해시태그 게시물 조회. 실패 시 None."""
    hk = _get_hiker()
    if not hk:
        return None
    try:
        if search_type == "top":
            medias = hk.hashtag_medias_top(hashtag, count=amount)
        else:
            medias = hk.hashtag_medias_recent(hashtag, count=amount)
        # 응답: list of dicts (paging은 라이브러리가 처리)
        if isinstance(medias, list):
            # [items, next_page_id] 형태일 수도 있음
            if len(medias) == 2 and isinstance(medias[0], list):
                return medias[0][:amount]
            return medias[:amount]
    except Exception as e:
        log.warning(f"[HikerAPI] 해시태그 조회 실패 {hashtag}: {e}")
    return None


def _media_get(m, key, default=0):
    """instagrapi 객체 또는 dict에서 값 추출 헬퍼."""
    if isinstance(m, dict):
        return m.get(key, default)
    return getattr(m, key, default)


def _media_get_str(m, key, default=""):
    val = _media_get(m, key, default)
    return str(val) if val else default

SPONSOR_KEYWORDS = re.compile(
    r'#(ad|advertisement|sponsored|협찬|광고|제공|유료광고|ppㅣ|ppl|협찬제품|제품협찬|협업|파트너십|홍보)',
    re.IGNORECASE
)

# ─── 계정 풀 (다중 계정 관리) ────────────────────────────────
# account_id → instagrapi Client 인스턴스 캐시
_client_pool: dict = {}
_current_account_id: int = None


def _extract_sessionid_playwright(username: str, password: str, totp_secret: str = "") -> str | None:
    """Playwright로 브라우저 자동 로그인 → sessionid 쿠키 추출.
    instagrapi 로그인이 모두 실패했을 때 최후 수단으로 사용.
    """
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log.warning("playwright 미설치 — pip install playwright && playwright install chromium")
        return None

    log.info(f"[Playwright] 브라우저 자동 로그인 시도: {username}")
    sessionid = None
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                           "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
                locale="en-US",
            )
            page = context.new_page()
            page.goto("https://www.instagram.com/accounts/login/", timeout=30000)
            time.sleep(3)

            # 로그인 폼 입력
            page.fill('input[name="email"]', username)
            page.fill('input[name="pass"]', password)
            page.get_by_role("button", name="Log In", exact=True).click()
            time.sleep(8)

            # 2FA 코드 자동 입력
            if totp_secret:
                inputs = page.query_selector_all("input")
                for inp in inputs:
                    name = (inp.get_attribute("name") or "").lower()
                    aria = (inp.get_attribute("aria-label") or "").lower()
                    if any(k in name + aria for k in ["code", "verif", "security"]):
                        code = pyotp.TOTP(totp_secret).now()
                        inp.fill(code)
                        log.info(f"[Playwright] 2FA 코드 입력: {code}")
                        try:
                            page.get_by_role("button", name="Confirm", exact=True).click(timeout=5000)
                        except Exception:
                            page.query_selector('button[type="button"]')
                        time.sleep(5)
                        break

            # "Save info" 등 팝업 닫기
            try:
                page.get_by_role("button", name="Not Now").click(timeout=3000)
            except Exception:
                pass

            # sessionid 추출
            for c in context.cookies("https://www.instagram.com"):
                if c["name"] == "sessionid":
                    sessionid = c["value"]
                    break

            browser.close()

        if sessionid:
            log.info(f"[Playwright] sessionid 추출 성공: {username}")
        else:
            log.warning(f"[Playwright] sessionid 추출 실패: {username}")
    except Exception as e:
        log.error(f"[Playwright] 오류 ({username}): {e}")

    return sessionid


def _make_client(acc: dict):
    """계정 딕셔너리로 instagrapi Client 생성 및 로그인.
    우선순위: ① sessionid 쿠키 → ② 저장된 세션 → ③ 아이디/비밀번호 → ④ Playwright 자동 추출
    """
    from instagrapi import Client
    cl = Client()
    cl.delay_range = [1, 3]

    # 프록시 설정 (모든 방법에 공통 적용)
    proxy_host = acc.get("proxy_host", "")
    proxy_port = acc.get("proxy_port", "")
    if proxy_host and proxy_port:
        proxy_user = acc.get("proxy_user", "")
        proxy_pass = acc.get("proxy_pass", "")
        auth = f"{proxy_user}:{proxy_pass}@" if proxy_user and proxy_pass else ""
        cl.set_proxy(f"http://{auth}{proxy_host}:{proxy_port}")
        log.info(f"프록시 설정: {proxy_host}:{proxy_port}")

    # device UUID 영속화 (인스타가 같은 기기로 인식)
    session_data = acc.get("session_data", "")
    saved_uuids = None
    if session_data:
        try:
            saved_settings = json.loads(session_data)
            saved_uuids = {k: saved_settings[k] for k in
                          ("uuid", "phone_id", "device_id", "android_device_id")
                          if k in saved_settings}
        except Exception:
            pass

    def _apply_uuids(client):
        if saved_uuids:
            client.set_settings({"uuids": saved_uuids})

    # ① sessionid 쿠키로 로그인 (IP 차단 우회 - 최우선)
    sessionid = (acc.get("sessionid_cookie") or "").strip()
    if sessionid:
        try:
            _apply_uuids(cl)
            cl.login_by_sessionid(sessionid)
            log.info(f"세션ID 로그인 성공: {acc['username']}")
            return cl
        except Exception as e:
            log.warning(f"세션ID 만료 또는 오류 ({acc['username']}): {e}")

    # ② 저장된 instagrapi 세션 복원
    if session_data:
        try:
            cl.set_settings(json.loads(session_data))
            cl.get_timeline_feed()
            log.info(f"세션 복원 성공: {acc['username']}")
            return cl
        except Exception:
            log.info(f"세션 만료, 재로그인: {acc['username']}")
            cl = Client()
            cl.delay_range = [1, 3]
            if proxy_host and proxy_port:
                auth = f"{proxy_user}:{proxy_pass}@" if proxy_user and proxy_pass else ""
                cl.set_proxy(f"http://{auth}{proxy_host}:{proxy_port}")

    # ③ 아이디/비밀번호 로그인
    totp_secret = acc.get("totp_secret", "")
    try:
        _apply_uuids(cl)
        totp_code = pyotp.TOTP(totp_secret).now() if totp_secret else None
        cl.login(acc["username"], acc["password"], verification_code=totp_code)
        log.info(f"로그인 성공: {acc['username']}" + (" (2FA)" if totp_code else ""))
        return cl
    except Exception as e:
        log.warning(f"비밀번호 로그인 실패 ({acc['username']}): {e}")

    # ④ Playwright 브라우저 자동 로그인 → sessionid 추출 → DB 저장
    new_sessionid = _extract_sessionid_playwright(
        acc["username"], acc["password"], totp_secret
    )
    if new_sessionid:
        try:
            from database import update_account_sessionid
            acc_id = acc.get("id") or acc.get("pk")
            update_account_sessionid(acc_id, new_sessionid)
        except Exception:
            pass
        cl2 = Client()
        cl2.delay_range = [1, 3]
        if proxy_host and proxy_port:
            auth = f"{proxy_user}:{proxy_pass}@" if proxy_user and proxy_pass else ""
            cl2.set_proxy(f"http://{auth}{proxy_host}:{proxy_port}")
        _apply_uuids(cl2)
        cl2.login_by_sessionid(new_sessionid)
        log.info(f"Playwright sessionid 로그인 성공: {acc['username']}")
        return cl2

    raise Exception(f"모든 로그인 방법 실패: {acc['username']}")


def get_client_from_pool():
    """
    계정 풀에서 다음 사용 가능한 계정으로 Client 반환.
    라운드로빈 방식, 실패 시 다음 계정으로 자동 전환.
    """
    from database import get_active_accounts, update_account_status

    accounts = get_active_accounts()
    if not accounts:
        raise RuntimeError("사용 가능한 인스타그램 계정이 없습니다. 설정 > 계정 관리에서 계정을 추가하세요.")

    # 이미 로그인된 클라이언트 우선 사용 (가장 오래된 것)
    for acc in accounts:
        acc_id = acc.get("id") or acc.get("pk")
        if acc_id in _client_pool:
            try:
                _client_pool[acc_id].get_timeline_feed()
                update_account_status(acc_id, "active")
                log.info(f"계정 재사용: {acc['username']}")
                return _client_pool[acc_id], acc_id
            except Exception:
                del _client_pool[acc_id]

    # 새로 로그인
    last_error = None
    for acc in accounts:
        acc_id = acc.get("id") or acc.get("pk")
        try:
            cl = _make_client(acc)
            _client_pool[acc_id] = cl
            # 세션 저장
            try:
                session_json = json.dumps(cl.get_settings())
                update_account_status(acc_id, "active", session_data=session_json)
            except Exception:
                update_account_status(acc_id, "active")
            return cl, acc_id
        except Exception as e:
            last_error = str(e)
            err_msg = str(e)[:200]
            # 밴/차단 감지
            if any(w in err_msg.lower() for w in ["challenge", "banned", "blocked", "checkpoint"]):
                update_account_status(acc_id, "banned", last_error=err_msg)
                log.warning(f"계정 차단됨: {acc['username']} - {err_msg[:80]}")
            else:
                update_account_status(acc_id, "error", last_error=err_msg)
                log.error(f"로그인 실패: {acc['username']} - {err_msg[:80]}")

    raise RuntimeError(f"모든 계정 로그인 실패. 마지막 오류: {last_error}")


def get_client(username=None, password=None, totp_secret=None,
               proxy_host="", proxy_port="", proxy_user="", proxy_pass=""):
    """
    하위 호환 래퍼. username 없으면 계정 풀 사용.
    """
    if username:
        # 단일 계정 레거시 모드
        global _client_pool
        cache_key = f"legacy_{username}"
        if cache_key in _client_pool:
            try:
                _client_pool[cache_key].get_timeline_feed()
                return _client_pool[cache_key]
            except Exception:
                del _client_pool[cache_key]

        from instagrapi import Client
        cl = Client()
        cl.delay_range = [1, 3]
        if proxy_host and proxy_port:
            auth = f"{proxy_user}:{proxy_pass}@" if proxy_user else ""
            cl.set_proxy(f"http://{auth}{proxy_host}:{proxy_port}")
        code = pyotp.TOTP(totp_secret).now() if totp_secret else None
        cl.login(username, password, verification_code=code)
        _client_pool[cache_key] = cl
        log.info(f"레거시 로그인 성공: {username}")
        return cl

    # 계정 풀 모드
    cl, acc_id = get_client_from_pool()
    return cl


def download_image(url: str, save_path: str) -> bool:
    """이미지 다운로드. 성공시 True"""
    try:
        if not url or url.startswith("http") is False:
            return False
        r = req_lib.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code == 200 and len(r.content) > 1000:
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            with open(save_path, "wb") as f:
                f.write(r.content)
            return True
    except Exception as e:
        log.debug(f"이미지 다운로드 실패 {url}: {e}")
    return False


def calc_stats(pk: str, username: str, medias: list, follower_count: int) -> dict:
    """게시물 목록에서 통계 계산 (instagrapi 객체 및 HikerAPI dict 모두 지원)"""
    reels, feeds = [], []
    all_hours = []
    all_intervals = []
    sponsored_cnt = 0
    prev_date = None

    for m in medias:
        media_type = _media_get(m, "media_type", 1)
        product_type = _media_get(m, "product_type", "")
        is_reel = (media_type == 2 and product_type == "clips") or (media_type == 2 and (_media_get(m, "video_duration", 0) or 0) > 0)
        likes = _media_get(m, "like_count", 0) or 0
        comments = _media_get(m, "comment_count", 0) or 0
        views = _media_get(m, "view_count", 0) or _media_get(m, "play_count", 0) or 0
        # caption: instagrapi는 caption_text, HikerAPI는 caption.text
        caption = _media_get_str(m, "caption_text", "")
        if not caption and isinstance(m, dict):
            cap_obj = m.get("caption")
            if isinstance(cap_obj, dict):
                caption = cap_obj.get("text", "")
            elif isinstance(cap_obj, str):
                caption = cap_obj
        taken_at = _media_get(m, "taken_at", None)
        taken_at_ts = _media_get(m, "taken_at_ts", None)

        if SPONSOR_KEYWORDS.search(caption):
            sponsored_cnt += 1

        if taken_at or taken_at_ts:
            if taken_at_ts and isinstance(taken_at_ts, (int, float)):
                dt = datetime.fromtimestamp(float(taken_at_ts))
            elif isinstance(taken_at, datetime):
                dt = taken_at
            elif isinstance(taken_at, str):
                try:
                    dt = datetime.fromisoformat(taken_at.replace("Z", "+00:00"))
                except ValueError:
                    dt = None
            elif isinstance(taken_at, (int, float)):
                dt = datetime.fromtimestamp(float(taken_at))
            else:
                dt = None
            if dt:
                # timezone-aware → naive로 변환 (비교용)
                if hasattr(dt, 'tzinfo') and dt.tzinfo:
                    dt = dt.replace(tzinfo=None)
                all_hours.append(dt.hour)
                if prev_date:
                    diff = abs((dt - prev_date).days)
                    if diff < 365:
                        all_intervals.append(diff)
                prev_date = dt

        entry = {"likes": likes, "comments": comments, "views": views}
        if is_reel:
            reels.append(entry)
        else:
            feeds.append(entry)

    total = len(reels) + len(feeds)
    avg_reel_views = round(sum(r["views"] for r in reels) / len(reels), 1) if reels else 0
    avg_feed_likes = round(sum(f["likes"] for f in feeds) / len(feeds), 1) if feeds else 0
    all_likes = [m["likes"] for m in reels + feeds]
    all_comments = [m["comments"] for m in reels + feeds]
    avg_likes = round(sum(all_likes) / len(all_likes), 1) if all_likes else 0
    avg_comments = round(sum(all_comments) / len(all_comments), 1) if all_comments else 0
    engagement_rate = round((avg_likes + avg_comments) / follower_count * 100, 2) if follower_count else 0

    # 업로드 빈도
    avg_interval = round(sum(all_intervals) / len(all_intervals), 1) if all_intervals else 0
    if avg_interval <= 1:   upload_freq = "매일"
    elif avg_interval <= 3: upload_freq = "2~3일마다"
    elif avg_interval <= 7: upload_freq = "주 1회"
    else:                   upload_freq = "월 1~2회"

    # 활성 시간대
    top_hours = [f"{h:02d}:00" for h, _ in Counter(all_hours).most_common(3)]
    active_hours = ", ".join(top_hours)

    # Top3
    all_posts_data = reels + feeds
    top_likes = sorted(all_posts_data, key=lambda x: x["likes"], reverse=True)[:3]
    top_comments = sorted(all_posts_data, key=lambda x: x["comments"], reverse=True)[:3]
    top_reels = sorted(reels, key=lambda x: x["views"], reverse=True)[:3]

    # 마지막 게시일
    last_post_date = ""
    if medias:
        ta = _media_get(medias[0], "taken_at", None)
        ta_ts = _media_get(medias[0], "taken_at_ts", None)
        dt = None
        if ta_ts and isinstance(ta_ts, (int, float)):
            dt = datetime.fromtimestamp(float(ta_ts))
        elif isinstance(ta, datetime):
            dt = ta
        elif isinstance(ta, str) and ta:
            try:
                dt = datetime.fromisoformat(ta.replace("Z", "+00:00"))
            except ValueError:
                pass
        elif isinstance(ta, (int, float)):
            dt = datetime.fromtimestamp(float(ta))
        if dt:
            last_post_date = dt.strftime("%Y-%m-%d")

    return {
        "avg_likes": avg_likes,
        "avg_comments": avg_comments,
        "engagement_rate": engagement_rate,
        "avg_reel_views": avg_reel_views,
        "avg_feed_likes": avg_feed_likes,
        "reel_count": len(reels),
        "feed_count": len(feeds),
        "upload_frequency": upload_freq,
        "active_hours": active_hours,
        "avg_posting_interval": avg_interval,
        "last_post_date": last_post_date,
        "reels_ratio": round(len(reels) / total * 100, 1) if total else 0,
        "sponsored_ratio": round(sponsored_cnt / total * 100, 1) if total else 0,
        "top_posts_likes": [{"likes": x["likes"], "comments": x["comments"]} for x in top_likes],
        "top_posts_comments": [{"likes": x["likes"], "comments": x["comments"]} for x in top_comments],
        "top_reels_views": [{"views": x["views"], "likes": x["likes"]} for x in top_reels],
    }


def _extract_media_fields(m, pk: str):
    """게시물(instagrapi 객체 또는 dict)에서 DB 저장용 필드 추출."""
    media_type = _media_get(m, "media_type", 1)
    product_type = _media_get(m, "product_type", "")
    is_reel = (media_type == 2 and product_type == "clips")
    post_type = "reel" if is_reel else ("video" if media_type == 2 else ("carousel" if media_type == 8 else "photo"))

    # taken_at: int/float(timestamp), datetime, 또는 ISO문자열 모두 처리
    taken_at = _media_get(m, "taken_at", None)
    # HikerAPI는 taken_at_ts (unix timestamp)도 제공
    taken_at_ts = _media_get(m, "taken_at_ts", None)
    if taken_at_ts and isinstance(taken_at_ts, (int, float)):
        taken_ts = float(taken_at_ts)
    elif isinstance(taken_at, (int, float)):
        taken_ts = float(taken_at)
    elif hasattr(taken_at, "timestamp"):
        taken_ts = taken_at.timestamp()
    elif isinstance(taken_at, str) and taken_at:
        try:
            taken_ts = datetime.fromisoformat(taken_at.replace("Z", "+00:00")).timestamp()
        except ValueError:
            taken_ts = 0.0
    else:
        taken_ts = 0.0

    # caption 추출
    caption_text = _media_get_str(m, "caption_text", "")
    if not caption_text and isinstance(m, dict):
        cap_obj = m.get("caption")
        if isinstance(cap_obj, dict):
            caption_text = cap_obj.get("text", "")
        elif isinstance(cap_obj, str):
            caption_text = cap_obj

    hashtags_in_post = ",".join(re.findall(r'#(\w+)', caption_text))

    # thumbnail
    thumbnail_url = ""
    if isinstance(m, dict):
        thumbnail_url = m.get("thumbnail_url", "") or ""
        if not thumbnail_url:
            img = m.get("image_versions2", {})
            candidates = img.get("candidates", [])
            if candidates:
                thumbnail_url = candidates[0].get("url", "")
    else:
        try:
            if m.thumbnail_url:
                thumbnail_url = str(m.thumbnail_url)
            elif m.resources:
                thumbnail_url = str(m.resources[0].thumbnail_url or "")
        except:
            pass

    # 썸네일 로컬 저장
    post_dir = os.path.join(POSTS_DIR, pk)
    thumb_local = ""
    media_pk = str(_media_get(m, "pk", ""))
    if thumbnail_url and media_pk:
        thumb_path = os.path.join(post_dir, f"{media_pk}.jpg")
        if download_image(thumbnail_url, thumb_path):
            thumb_local = f"posts/{pk}/{media_pk}.jpg"

    is_sponsored = 1 if SPONSOR_KEYWORDS.search(caption_text) else 0
    code = _media_get_str(m, "code", "")

    return {
        "influencer_pk": pk,
        "post_id": media_pk,
        "post_url": f"https://www.instagram.com/p/{code}/" if code else "",
        "post_type": post_type,
        "likes": _media_get(m, "like_count", 0) or 0,
        "comments": _media_get(m, "comment_count", 0) or 0,
        "views": _media_get(m, "view_count", 0) or _media_get(m, "play_count", 0) or 0,
        "caption": caption_text[:500],
        "hashtags_used": hashtags_in_post,
        "is_sponsored": is_sponsored,
        "thumbnail_url": thumbnail_url,
        "thumbnail_local": thumb_local,
        "taken_at": taken_ts,
    }


def _extract_top_posts(medias: list):
    """Top 게시물 URL 추출 (instagrapi 객체/dict 모두 지원)."""
    def _thumb(m):
        if isinstance(m, dict):
            url = m.get("thumbnail_url", "")
            if not url:
                img = m.get("image_versions2", {})
                cands = img.get("candidates", [])
                url = cands[0].get("url", "") if cands else ""
            return url
        try:
            return str(m.thumbnail_url) if m.thumbnail_url else ""
        except:
            return ""

    sorted_by_likes = sorted(medias, key=lambda m: _media_get(m, "like_count", 0) or 0, reverse=True)
    top_likes = []
    for m in sorted_by_likes[:3]:
        code = _media_get_str(m, "code", "")
        top_likes.append({
            "url": f"https://www.instagram.com/p/{code}/" if code else "",
            "likes": _media_get(m, "like_count", 0) or 0,
            "thumbnail": _thumb(m),
            "post_id": str(_media_get(m, "pk", "")),
        })

    sorted_by_comments = sorted(medias, key=lambda m: _media_get(m, "comment_count", 0) or 0, reverse=True)
    top_comments = []
    for m in sorted_by_comments[:3]:
        code = _media_get_str(m, "code", "")
        top_comments.append({
            "url": f"https://www.instagram.com/p/{code}/" if code else "",
            "comments": _media_get(m, "comment_count", 0) or 0,
            "thumbnail": _thumb(m),
            "post_id": str(_media_get(m, "pk", "")),
        })

    reel_medias = [m for m in medias
                   if _media_get(m, "media_type", 1) == 2 and _media_get(m, "product_type", "") == "clips"]
    sorted_reels = sorted(reel_medias, key=lambda m: _media_get(m, "view_count", 0) or 0, reverse=True)
    top_reels = []
    for m in sorted_reels[:3]:
        code = _media_get_str(m, "code", "")
        top_reels.append({
            "url": f"https://www.instagram.com/p/{code}/" if code else "",
            "views": _media_get(m, "view_count", 0) or _media_get(m, "play_count", 0) or 0,
            "thumbnail": _thumb(m),
            "post_id": str(_media_get(m, "pk", "")),
        })

    return top_likes, top_comments, top_reels


def _update_profile_from_info(u_info, pk: str, username: str):
    """유저 프로필 정보를 DB에 갱신 (instagrapi 객체/dict 모두 지원). 프로필 사진 로컬경로 반환."""
    pic_local = ""
    try:
        if isinstance(u_info, dict):
            fc = u_info.get("follower_count", 0)
            foc = u_info.get("following_count", 0)
            mc = u_info.get("media_count", 0)
            bio = u_info.get("biography", "") or ""
            fn = u_info.get("full_name", "") or ""
            is_biz = 1 if u_info.get("is_business") else 0
            cat = u_info.get("category", "") or ""
            pic_url = u_info.get("profile_pic_url", "") or u_info.get("profile_pic_url_hd", "") or ""
        else:
            fc = getattr(u_info, "follower_count", 0)
            foc = getattr(u_info, "following_count", 0)
            mc = getattr(u_info, "media_count", 0)
            bio = str(getattr(u_info, "biography", "") or "")
            fn = str(getattr(u_info, "full_name", "") or "")
            is_biz = 1 if getattr(u_info, "is_business", False) else 0
            cat = str(getattr(u_info, "category", "") or "")
            pic_url = str(u_info.profile_pic_url) if u_info.profile_pic_url else ""

        profile_updates = {}
        if fc: profile_updates["follower_count"] = fc
        if foc: profile_updates["following_count"] = foc
        if mc: profile_updates["media_count"] = mc
        if bio: profile_updates["bio"] = bio
        if fn: profile_updates["full_name"] = fn
        profile_updates["is_business"] = is_biz
        if cat: profile_updates["category"] = cat

        if is_biz:
            from database import save_manual, get_manual
            m = get_manual(pk)
            if not m.get("is_brand"):
                save_manual(pk, {**m, "is_brand": 1})

        if profile_updates:
            from database import update_influencer_profile
            update_influencer_profile(pk, profile_updates)

        if pic_url:
            pic_path = os.path.join(PROFILE_PIC_DIR, f"{username}.jpg")
            if download_image(pic_url, pic_path):
                pic_local = f"profile_pics/{username}.jpg"
    except Exception as e:
        log.debug(f"프로필 갱신 오류 {username}: {e}")

    return pic_local


def crawl_user_detail(cl, pk: str, username: str, follower_count: int) -> bool:
    """단일 인플루언서 게시물 수집 + 통계 계산 + 사진 저장.
    HikerAPI 우선, 실패 시 instagrapi(cl) 폴백.
    cl이 None이면 HikerAPI 전용 모드.
    """
    try:
        medias = None
        u_info = None

        # ① HikerAPI 시도
        hiker_medias = _hiker_user_medias(pk, amount=50)
        if hiker_medias:
            medias = hiker_medias
            u_info = _hiker_user_info_by_id(pk)
            log.info(f"[{username}] HikerAPI로 게시물 {len(medias)}개 조회")

        # ② instagrapi 폴백
        if not medias and cl:
            medias = cl.user_medias(int(pk), amount=50)
            try:
                u_info = cl.user_info(int(pk))
            except Exception:
                pass

        if not medias:
            return False

        # 게시물 DB 저장
        for m in medias:
            post_data = _extract_media_fields(m, pk)
            upsert_post(post_data)

        # 통계 계산
        stats = calc_stats(pk, username, medias, follower_count)

        # 프로필 정보 갱신
        pic_local = ""
        if u_info:
            pic_local = _update_profile_from_info(u_info, pk, username)
        elif cl:
            try:
                u_info = cl.user_info(int(pk))
                pic_local = _update_profile_from_info(u_info, pk, username)
            except Exception as e:
                log.debug(f"프로필 갱신 오류 {username}: {e}")

        stats["profile_pic_local"] = pic_local

        # Top 게시물 URL
        try:
            top_likes, top_comments, top_reels = _extract_top_posts(medias)
            stats["top_posts_likes"] = top_likes
            stats["top_posts_comments"] = top_comments
            stats["top_reels_views"] = top_reels
        except:
            pass

        update_influencer_stats(pk, stats)
        log.info(f"[{username}] 상세 수집 완료 - 게시물 {len(medias)}개")
        return True

    except Exception as e:
        log.error(f"[{username}] 상세 수집 실패: {e}")
        return False


def crawl_single_user(target_username: str) -> dict:
    """
    특정 계정명을 즉시 수집하여 DB에 저장하고 pk 반환.
    HikerAPI 우선, 실패 시 instagrapi 계정 풀 폴백.
    """
    from database import upsert_influencer, get_influencer_by_username

    existing = get_influencer_by_username(target_username)

    # ① HikerAPI 시도
    hiker_info = _hiker_user_info(target_username)
    if hiker_info:
        pk = str(hiker_info["pk"])
        uname = hiker_info.get("username", target_username)
        upsert_influencer({
            "pk": pk,
            "username": uname,
            "full_name": str(hiker_info.get("full_name", "") or ""),
            "follower_count": hiker_info.get("follower_count", 0) or 0,
            "following_count": hiker_info.get("following_count", 0) or 0,
            "media_count": hiker_info.get("media_count", 0) or 0,
            "bio": str(hiker_info.get("biography", "") or ""),
            "is_private": 1 if hiker_info.get("is_private") else 0,
            "is_verified": 1 if hiker_info.get("is_verified") else 0,
            "hashtag": "__direct__",
        })
        # 상세 수집 (cl=None → HikerAPI 전용)
        crawl_user_detail(None, pk, uname, hiker_info.get("follower_count", 0) or 0)
        log.info(f"[HikerAPI] 즉시 수집 완료: @{target_username} (pk={pk})")
        return {"ok": True, "pk": pk, "username": uname}

    # ② instagrapi 폴백
    log.info(f"[HikerAPI 실패/미설정] instagrapi 폴백: @{target_username}")
    used_acc_id = None
    try:
        cl, used_acc_id = get_client_from_pool()
    except Exception as e:
        log.error(f"계정 풀 로그인 실패: {e}")
        return {"error": str(e)}

    try:
        u = cl.user_info_by_username(target_username)
        pk = str(u.pk)

        upsert_influencer({
            "pk": pk,
            "username": u.username,
            "full_name": str(u.full_name or ""),
            "follower_count": u.follower_count or 0,
            "following_count": u.following_count or 0,
            "media_count": u.media_count or 0,
            "bio": str(u.biography or ""),
            "is_private": 1 if u.is_private else 0,
            "is_verified": 1 if u.is_verified else 0,
            "hashtag": "__direct__",
        })

        crawl_user_detail(cl, pk, u.username, u.follower_count or 0)

        if used_acc_id:
            from database import update_account_status
            update_account_status(used_acc_id, "idle")

        log.info(f"즉시 수집 완료: @{target_username} (pk={pk})")
        return {"ok": True, "pk": pk, "username": u.username}

    except Exception as e:
        log.error(f"즉시 수집 실패 @{target_username}: {e}")
        if used_acc_id:
            from database import update_account_status
            update_account_status(used_acc_id, "idle")
        return {"error": str(e)}


def crawl_hashtag(hashtag: str, requested_count: int,
                  username: str = None, password: str = None, totp_secret: str = None,
                  job_id: str = None,
                  proxy_host: str = "", proxy_port: str = "",
                  target_users: int = 0, search_type: str = "recent",
                  proxy_user: str = "", proxy_pass: str = ""):
    """해시태그 크롤링 - HikerAPI 우선, instagrapi 폴백"""
    import time as _time
    from database import add_collect_job, update_collect_job

    if not job_id:
        import uuid
        job_id = str(uuid.uuid4())

    # target_users 모드: 목표 인원 달성까지 게시물 검색 (최대 게시물 10000개)
    user_mode = target_users > 0
    if user_mode:
        requested_count = 10000  # 충분히 큰 값으로 설정

    progress[job_id] = {
        "status": "수집 준비 중", "hashtag": hashtag,
        "posts": 0, "new": 0, "updated": 0,
        "requested": requested_count, "target_users": target_users,
        "done": False, "error": None
    }

    try:
        job_db_id = add_collect_job(hashtag, "running", requested_count)
    except Exception:
        job_db_id = None

    try:
        all_pks = set()
        collected_posts = 0
        use_hiker = False

        # ① HikerAPI로 해시태그 게시물 수집
        hiker_medias = _hiker_hashtag_medias(hashtag, amount=requested_count, search_type=search_type)
        if hiker_medias:
            use_hiker = True
            progress[job_id]["status"] = "게시물 수집 중 (HikerAPI)"
            for m in hiker_medias:
                user_data = m.get("user", {})
                pk = user_data.get("pk")
                if pk:
                    all_pks.add(str(pk))
                collected_posts += 1
                if user_mode and len(all_pks) >= target_users:
                    break
            progress[job_id].update({"posts": collected_posts,
                                      "status": f"게시물 수집 완료 ({collected_posts}개, {len(all_pks)}명)"})
            log.info(f"[{hashtag}] HikerAPI 게시물 {collected_posts}개 수집, 유저 {len(all_pks)}명")

        # ② instagrapi 폴백
        if not use_hiker:
            progress[job_id]["status"] = "로그인 중"
            if username:
                cl = get_client(username, password, totp_secret, proxy_host, proxy_port, proxy_user, proxy_pass)
                used_acc_id = None
            else:
                cl, used_acc_id = get_client_from_pool()
            progress[job_id]["status"] = "게시물 수집 중"

            next_page = None
            while collected_posts < requested_count:
                try:
                    resp = cl.private_request(
                        f"tags/{hashtag}/sections/",
                        data={"max_id": next_page or "", "page": 1, "surface": "grid", "count": 18, "_uuid": cl.uuid}
                    )
                except Exception as e:
                    log.warning(f"fallback to hashtag_medias: {e}")
                    try:
                        if search_type == "top":
                            medias = cl.hashtag_medias_top(hashtag, amount=min(requested_count - collected_posts, 50))
                        else:
                            medias = cl.hashtag_medias_recent(hashtag, amount=min(requested_count - collected_posts, 50))
                        for m in medias:
                            all_pks.add(str(m.user.pk))
                        collected_posts += len(medias)
                        break
                    except Exception as e2:
                        raise Exception(f"게시물 수집 실패: {e2}")

                sections = resp.get("sections", [])
                page_pks = set()
                page_posts = 0
                for sec in sections:
                    lc = sec.get("layout_content", {})
                    for item in lc.get("fill_items", []):
                        pk = item.get("media", {}).get("user", {}).get("pk")
                        if pk: page_pks.add(str(pk)); page_posts += 1
                    for item in lc.get("one_by_two_item", {}).get("clips", {}).get("items", []):
                        pk = item.get("media", {}).get("user", {}).get("pk")
                        if pk: page_pks.add(str(pk)); page_posts += 1
                    for item in lc.get("medias", []):
                        pk = item.get("media", {}).get("user", {}).get("pk")
                        if pk: page_pks.add(str(pk)); page_posts += 1

                all_pks.update(page_pks)
                collected_posts += page_posts
                next_page = resp.get("next_max_id")
                if user_mode:
                    progress[job_id].update({"posts": collected_posts,
                        "status": f"게시물 수집 중 — {len(all_pks)}명 발견 (목표 {target_users}명)"})
                    if len(all_pks) >= target_users:
                        break
                else:
                    progress[job_id].update({"posts": collected_posts,
                        "status": f"게시물 수집 중 ({collected_posts}/{requested_count})"})
                if not next_page or page_posts == 0:
                    break
                time.sleep(1)

        # 유저 상세 정보 조회
        total_pks = len(all_pks)
        progress[job_id]["status"] = f"유저 정보 조회 중 (0/{total_pks})"
        new_cnt = updated_cnt = 0

        for i, pk in enumerate(all_pks):
            try:
                # HikerAPI로 유저 정보 조회
                u_data = _hiker_user_info_by_id(pk)
                if u_data:
                    data = {
                        "pk": str(u_data["pk"]),
                        "username": u_data.get("username", ""),
                        "full_name": u_data.get("full_name", ""),
                        "biography": u_data.get("biography", ""),
                        "follower_count": u_data.get("follower_count", 0),
                        "following_count": u_data.get("following_count", 0),
                        "media_count": u_data.get("media_count", 0),
                        "is_private": u_data.get("is_private", False),
                        "is_verified": u_data.get("is_verified", False),
                        "is_business": u_data.get("is_business", False),
                        "category": u_data.get("category", ""),
                        "public_email": u_data.get("public_email", "") or "",
                        "public_phone": u_data.get("public_phone_number", "") or "",
                        "external_url": str(u_data.get("external_url", "") or ""),
                        "profile_pic_url": str(u_data.get("profile_pic_url", "") or ""),
                        "hashtag": hashtag,
                    }
                elif not use_hiker:
                    # instagrapi 폴백
                    u = cl.user_info(pk)
                    data = {
                        "pk": str(u.pk),
                        "username": u.username,
                        "full_name": u.full_name,
                        "biography": u.biography,
                        "follower_count": u.follower_count,
                        "following_count": u.following_count,
                        "media_count": u.media_count,
                        "is_private": u.is_private,
                        "is_verified": u.is_verified,
                        "is_business": u.is_business,
                        "category": u.category,
                        "public_email": getattr(u, "public_email", None) or "",
                        "public_phone": getattr(u, "public_phone_number", None) or "",
                        "external_url": str(u.external_url) if u.external_url else "",
                        "profile_pic_url": str(u.profile_pic_url) if u.profile_pic_url else "",
                        "hashtag": hashtag,
                    }
                else:
                    log.warning(f"유저 {pk} 정보 조회 실패 (HikerAPI)")
                    continue

                result = upsert_influencer(data)
                if result == "new": new_cnt += 1
                else: updated_cnt += 1
                progress[job_id].update({"new": new_cnt, "updated": updated_cnt,
                                          "status": f"유저 정보 조회 중 ({i+1}/{total_pks})"})
                time.sleep(0.3)
            except Exception as e:
                log.warning(f"유저 {pk} 조회 실패: {e}")
                time.sleep(1)

        try:
            update_collect_job(job_db_id,
                status="done", collected_posts=collected_posts,
                new_users=new_cnt, updated_users=updated_cnt, finished_at=_time.time())
            update_hashtag_status(hashtag, "idle")
        except Exception:
            pass

        progress[job_id].update({"status": "완료", "done": True, "new": new_cnt, "updated": updated_cnt})
        log.info(f"[{hashtag}] 완료: 게시물 {collected_posts}, 신규 {new_cnt}, 업데이트 {updated_cnt}")

    except Exception as e:
        log.error(f"[{hashtag}] 크롤링 에러: {e}")
        try:
            if job_db_id:
                update_collect_job(job_db_id, status="error", error_msg=str(e)[:200], finished_at=_time.time())
            update_hashtag_status(hashtag, "error")
        except Exception:
            pass
        progress[job_id].update({"status": "에러", "error": str(e), "done": True})


def refresh_all(username: str = None, password: str = None, totp_secret: str = None,
                proxy_host: str = "", proxy_port: str = "",
                proxy_user: str = "", proxy_pass: str = "",
                requests_per_account: int = 10):
    """
    전체 인플루언서 상세 갱신.
    HikerAPI 우선 → instagrapi 폴백 (계정 풀 라운드로빈)
    """
    from database import get_influencers, update_collect_job

    refresh_progress["current"] = {"done": 0, "total": 0, "current_user": "", "running": True,
                                   "error": None, "current_account": ""}

    try:
        cutoff = time.time() - 86400
        all_infs = get_influencers(per_page=99999, page=1)
        rows = [r for r in all_infs.get("items", [])
                if not r.get("stats_updated_at") or r["stats_updated_at"] < cutoff]

        total = len(rows)
        refresh_progress["current"]["total"] = total

        use_hiker = _get_hiker() is not None
        if use_hiker:
            refresh_progress["current"]["current_account"] = "HikerAPI"
            log.info("전체 갱신: HikerAPI 모드")

        request_count = 0
        cl = None
        used_acc_id = None

        for i, row in enumerate(rows):
            pk = row.get("pk") or row.get("id")
            uname = row.get("username", "")
            followers = row.get("follower_count", 0)
            refresh_progress["current"]["current_user"] = uname

            # HikerAPI 사용 시 instagrapi 로그인 불필요
            if not use_hiker:
                if cl is None or request_count >= requests_per_account:
                    try:
                        if username:
                            cl = get_client(username, password, totp_secret,
                                            proxy_host, proxy_port, proxy_user, proxy_pass)
                            used_acc_id = None
                        else:
                            cl, used_acc_id = get_client_from_pool()
                        request_count = 0
                        acc_name = username or (cl.account_info().username if cl else "?")
                        refresh_progress["current"]["current_account"] = acc_name
                        log.info(f"계정 전환: {acc_name}")
                    except Exception as e:
                        log.error(f"계정 로그인 실패: {e}")
                        refresh_progress["current"]["error"] = str(e)
                        break

            log.info(f"갱신 중 [{i+1}/{total}]: @{uname}")
            try:
                crawl_user_detail(cl, str(pk), uname, followers or 0)
                request_count += 1
            except Exception as e:
                log.warning(f"갱신 실패 {uname}: {e}")
                if "rate" in str(e).lower() or "wait" in str(e).lower():
                    cl = None

            refresh_progress["current"]["done"] = i + 1
            time.sleep(1 if use_hiker else 2)

        refresh_progress["current"]["running"] = False
        log.info(f"전체 갱신 완료: {total}개")

    except Exception as e:
        log.error(f"전체 갱신 에러: {e}")
        refresh_progress["current"]["running"] = False
        refresh_progress["current"]["error"] = str(e)


def login_test_account(account_id: int):
    """특정 계정 로그인 테스트"""
    from database import get_accounts, update_account_status
    accounts = get_accounts()
    acc = next((a for a in accounts if (a.get("id") or a.get("pk")) == account_id), None)
    if not acc:
        return {"ok": False, "error": "계정을 찾을 수 없음"}
    try:
        acc_id = acc.get("id") or acc.get("pk")
        # 캐시 제거 후 재로그인
        _client_pool.pop(acc_id, None)
        cl = _make_client(acc)
        _client_pool[acc_id] = cl
        # 세션 저장
        try:
            session_json = json.dumps(cl.get_settings())
            update_account_status(acc_id, "active", session_data=session_json)
        except Exception:
            update_account_status(acc_id, "active")
        totp_code = ""
        if acc.get("totp_secret"):
            totp_code = pyotp.TOTP(acc["totp_secret"]).now()
        return {"ok": True, "username": acc["username"], "totp_code": totp_code}
    except Exception as e:
        acc_id = acc.get("id") or acc.get("pk")
        update_account_status(acc_id, "error", last_error=str(e)[:200])
        return {"ok": False, "error": str(e)[:200]}
