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

SPONSOR_KEYWORDS = re.compile(
    r'#(ad|advertisement|sponsored|협찬|광고|제공|유료광고|ppㅣ|ppl|협찬제품|제품협찬|협업|파트너십|홍보)',
    re.IGNORECASE
)

# ─── 계정 풀 (다중 계정 관리) ────────────────────────────────
# account_id → instagrapi Client 인스턴스 캐시
_client_pool: dict = {}
_current_account_id: int = None


def _make_client(acc: dict):
    """계정 딕셔너리로 instagrapi Client 생성 및 로그인"""
    from instagrapi import Client
    cl = Client()
    cl.delay_range = [1, 3]

    # 저장된 세션 복원 시도
    session_data = acc.get("session_data", "")
    if session_data:
        try:
            cl.set_settings(json.loads(session_data))
            cl.get_timeline_feed()
            log.info(f"세션 복원 성공: {acc['username']}")
            return cl
        except Exception:
            log.info(f"세션 만료, 재로그인: {acc['username']}")

    # 프록시 설정
    proxy_host = acc.get("proxy_host", "")
    proxy_port = acc.get("proxy_port", "")
    if proxy_host and proxy_port:
        proxy_user = acc.get("proxy_user", "")
        proxy_pass = acc.get("proxy_pass", "")
        if proxy_user and proxy_pass:
            proxy_url = f"http://{proxy_user}:{proxy_pass}@{proxy_host}:{proxy_port}"
        else:
            proxy_url = f"http://{proxy_host}:{proxy_port}"
        cl.set_proxy(proxy_url)
        log.info(f"프록시 설정: {proxy_host}:{proxy_port}")

    # 2FA TOTP 자동 생성
    totp_secret = acc.get("totp_secret", "")
    totp_code = pyotp.TOTP(totp_secret).now() if totp_secret else None

    cl.login(acc["username"], acc["password"], verification_code=totp_code)
    log.info(f"로그인 성공: {acc['username']}" + (" (2FA)" if totp_code else ""))
    return cl


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
    """게시물 목록에서 통계 계산"""
    reels, feeds = [], []
    all_hours = []
    all_intervals = []
    sponsored_cnt = 0
    prev_date = None

    for m in medias:
        media_type = getattr(m, "media_type", 1)
        product_type = getattr(m, "product_type", "")
        is_reel = (media_type == 2 and product_type == "clips") or (media_type == 2 and hasattr(m, "video_duration"))
        likes = getattr(m, "like_count", 0) or 0
        comments = getattr(m, "comment_count", 0) or 0
        views = getattr(m, "view_count", 0) or getattr(m, "play_count", 0) or 0
        caption = str(getattr(m, "caption_text", "") or "")
        taken_at = getattr(m, "taken_at", None)

        if SPONSOR_KEYWORDS.search(caption):
            sponsored_cnt += 1

        if taken_at:
            dt = taken_at if isinstance(taken_at, datetime) else datetime.fromtimestamp(float(taken_at))
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
        ta = getattr(medias[0], "taken_at", None)
        if ta:
            dt = ta if isinstance(ta, datetime) else datetime.fromtimestamp(float(ta))
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


def crawl_user_detail(cl, pk: str, username: str, follower_count: int) -> bool:
    """단일 인플루언서 게시물 수집 + 통계 계산 + 사진 저장"""
    try:
        # 게시물 수집 (최근 50개)
        medias = cl.user_medias(int(pk), amount=50)
        if not medias:
            return False

        # 게시물 DB 저장
        for m in medias:
            media_type = getattr(m, "media_type", 1)
            product_type = getattr(m, "product_type", "")
            is_reel = (media_type == 2 and product_type == "clips")
            post_type = "reel" if is_reel else ("video" if media_type == 2 else ("carousel" if media_type == 8 else "photo"))

            taken_at = getattr(m, "taken_at", None)
            taken_ts = taken_at.timestamp() if hasattr(taken_at, "timestamp") else float(taken_at or 0)

            caption_text = str(getattr(m, "caption_text", "") or "")
            hashtags_in_post = ",".join(re.findall(r'#(\w+)', caption_text))

            thumbnail_url = ""
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
            if thumbnail_url:
                thumb_path = os.path.join(post_dir, f"{m.pk}.jpg")
                if download_image(thumbnail_url, thumb_path):
                    thumb_local = f"posts/{pk}/{m.pk}.jpg"

            is_sponsored = 1 if SPONSOR_KEYWORDS.search(caption_text) else 0

            upsert_post({
                "influencer_pk": pk,
                "post_id": str(m.pk),
                "post_url": f"https://www.instagram.com/p/{m.code}/",
                "post_type": post_type,
                "likes": getattr(m, "like_count", 0) or 0,
                "comments": getattr(m, "comment_count", 0) or 0,
                "views": getattr(m, "view_count", 0) or getattr(m, "play_count", 0) or 0,
                "caption": caption_text[:500],
                "hashtags_used": hashtags_in_post,
                "is_sponsored": is_sponsored,
                "thumbnail_url": thumbnail_url,
                "thumbnail_local": thumb_local,
                "taken_at": taken_ts,
            })

        # 통계 계산
        stats = calc_stats(pk, username, medias, follower_count)

        # 프로필 정보 갱신 (팔로워/팔로잉/게시물수/바이오 등) + 프로필 사진
        pic_local = ""
        try:
            u_info = cl.user_info(int(pk))
            # 팔로워 수 등 기본 정보 DB 갱신
            profile_updates = {}
            if hasattr(u_info, "follower_count") and u_info.follower_count:
                profile_updates["follower_count"] = u_info.follower_count
            if hasattr(u_info, "following_count") and u_info.following_count:
                profile_updates["following_count"] = u_info.following_count
            if hasattr(u_info, "media_count") and u_info.media_count:
                profile_updates["media_count"] = u_info.media_count
            if hasattr(u_info, "biography") and u_info.biography:
                profile_updates["bio"] = str(u_info.biography)
            if hasattr(u_info, "full_name") and u_info.full_name:
                profile_updates["full_name"] = str(u_info.full_name)
            # 브랜드/기업 계정 자동 감지
            is_biz = 1 if getattr(u_info, "is_business", False) else 0
            cat = str(getattr(u_info, "category", "") or "")
            profile_updates["is_business"] = is_biz
            if cat:
                profile_updates["category"] = cat
            # is_business=True 면 manual.is_brand 도 자동 태깅
            if is_biz:
                from database import save_manual, get_manual
                m = get_manual(pk)
                if not m.get("is_brand"):
                    save_manual(pk, {**m, "is_brand": 1})
            if profile_updates:
                from database import update_influencer_profile
                update_influencer_profile(pk, profile_updates)
            # 프로필 사진 다운로드
            pic_url = str(u_info.profile_pic_url) if u_info.profile_pic_url else ""
            if pic_url:
                pic_path = os.path.join(PROFILE_PIC_DIR, f"{username}.jpg")
                if download_image(pic_url, pic_path):
                    pic_local = f"profile_pics/{username}.jpg"
        except Exception as e:
            log.debug(f"프로필 갱신 오류 {username}: {e}")

        stats["profile_pic_local"] = pic_local

        # Top 게시물 URL 업데이트
        try:
            sorted_medias = sorted(medias, key=lambda m: getattr(m, "like_count", 0) or 0, reverse=True)
            top_likes_urls = []
            for m in sorted_medias[:3]:
                thumb = ""
                try:
                    if m.thumbnail_url: thumb = str(m.thumbnail_url)
                except: pass
                top_likes_urls.append({
                    "url": f"https://www.instagram.com/p/{m.code}/",
                    "likes": getattr(m, "like_count", 0) or 0,
                    "thumbnail": thumb,
                    "post_id": str(m.pk),
                })

            sorted_by_comments = sorted(medias, key=lambda m: getattr(m, "comment_count", 0) or 0, reverse=True)
            top_comments_urls = []
            for m in sorted_by_comments[:3]:
                thumb = ""
                try:
                    if m.thumbnail_url: thumb = str(m.thumbnail_url)
                except: pass
                top_comments_urls.append({
                    "url": f"https://www.instagram.com/p/{m.code}/",
                    "comments": getattr(m, "comment_count", 0) or 0,
                    "thumbnail": thumb,
                    "post_id": str(m.pk),
                })

            # 릴스만 조회수 기준
            reel_medias = [m for m in medias
                           if getattr(m, "media_type", 1) == 2 and getattr(m, "product_type", "") == "clips"]
            sorted_reels = sorted(reel_medias, key=lambda m: getattr(m, "view_count", 0) or 0, reverse=True)
            top_reels_urls = []
            for m in sorted_reels[:3]:
                thumb = ""
                try:
                    if m.thumbnail_url: thumb = str(m.thumbnail_url)
                except: pass
                top_reels_urls.append({
                    "url": f"https://www.instagram.com/p/{m.code}/",
                    "views": getattr(m, "view_count", 0) or getattr(m, "play_count", 0) or 0,
                    "thumbnail": thumb,
                    "post_id": str(m.pk),
                })

            stats["top_posts_likes"] = top_likes_urls
            stats["top_posts_comments"] = top_comments_urls
            stats["top_reels_views"] = top_reels_urls
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
    DB에 없는 계정을 검색할 때 실시간으로 호출.
    """
    from database import upsert_influencer, get_influencer_by_username

    # 이미 DB에 있으면 상세만 갱신
    existing = get_influencer_by_username(target_username)

    try:
        cl, used_acc_id = get_client_from_pool()
    except Exception as e:
        log.error(f"계정 풀 로그인 실패: {e}")
        return {"error": str(e)}

    try:
        u = cl.user_info_by_username(target_username)
        pk = str(u.pk)

        # 기본 프로필 upsert
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

        # 상세 수집 (게시물/통계/프로필 사진)
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
                  proxy_user: str = "", proxy_pass: str = ""):
    """해시태그 크롤링 - 계정 풀 자동 사용"""
    import time as _time
    from database import add_collect_job, update_collect_job

    if not job_id:
        import uuid
        job_id = str(uuid.uuid4())

    progress[job_id] = {
        "status": "로그인 중", "hashtag": hashtag,
        "posts": 0, "new": 0, "updated": 0,
        "requested": requested_count, "done": False, "error": None
    }

    try:
        job_db_id = add_collect_job(hashtag, "running", requested_count)
    except Exception:
        job_db_id = None

    try:
        # 계정 풀 우선, 레거시 단일 계정 폴백
        if username:
            cl = get_client(username, password, totp_secret, proxy_host, proxy_port, proxy_user, proxy_pass)
            used_acc_id = None
        else:
            cl, used_acc_id = get_client_from_pool()
        progress[job_id]["status"] = "게시물 수집 중"

        all_pks = set()
        collected_posts = 0
        next_page = None

        while collected_posts < requested_count:
            try:
                resp = cl.private_request(
                    f"tags/{hashtag}/sections/",
                    data={"max_id": next_page or "", "page": 1, "surface": "grid", "count": 18, "_uuid": cl.uuid}
                )
            except Exception as e:
                log.warning(f"fallback to hashtag_medias_recent: {e}")
                try:
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
            progress[job_id].update({"posts": collected_posts, "status": f"게시물 수집 중 ({collected_posts}/{requested_count})"})
            if not next_page or page_posts == 0:
                break
            time.sleep(1)

        # 유저 상세 정보 조회
        total_pks = len(all_pks)
        progress[job_id]["status"] = f"유저 정보 조회 중 (0/{total_pks})"
        new_cnt = updated_cnt = 0

        for i, pk in enumerate(all_pks):
            try:
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
                result = upsert_influencer(data)
                if result == "new": new_cnt += 1
                else: updated_cnt += 1
                progress[job_id].update({"new": new_cnt, "updated": updated_cnt,
                                          "status": f"유저 정보 조회 중 ({i+1}/{total_pks})"})
                time.sleep(0.5)
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
    - 계정 풀 자동 사용 (라운드로빈, N건마다 계정 교체)
    - requests_per_account: 계정당 처리 건수 (기본 10)
    """
    from database import get_influencers, update_collect_job

    refresh_progress["current"] = {"done": 0, "total": 0, "current_user": "", "running": True,
                                   "error": None, "current_account": ""}

    try:
        # 24시간 이상 지난 계정 우선
        cutoff = time.time() - 86400
        all_infs = get_influencers(per_page=99999, page=1)
        rows = [r for r in all_infs.get("items", [])
                if not r.get("stats_updated_at") or r["stats_updated_at"] < cutoff]

        total = len(rows)
        refresh_progress["current"]["total"] = total

        request_count = 0
        cl = None
        used_acc_id = None

        for i, row in enumerate(rows):
            pk = row.get("pk") or row.get("id")
            uname = row.get("username", "")
            followers = row.get("follower_count", 0)
            refresh_progress["current"]["current_user"] = uname

            # 계정 교체 타이밍
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
                # rate limit 감지 → 즉시 계정 교체
                if "rate" in str(e).lower() or "wait" in str(e).lower():
                    cl = None

            refresh_progress["current"]["done"] = i + 1
            time.sleep(2)

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
