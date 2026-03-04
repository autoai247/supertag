import time, logging, pyotp, os, json, re
from collections import Counter
from datetime import datetime
import requests as req_lib
from database import upsert_influencer, update_influencer_stats, upsert_post

log = logging.getLogger(__name__)

progress: dict = {}   # job_id → {status, hashtag, posts, new, updated, total, done}
refresh_progress: dict = {}  # single key "current" → {done, total, current_user, ...}

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
PROFILE_PIC_DIR = os.path.join(DATA_DIR, "profile_pics")
POSTS_DIR = os.path.join(DATA_DIR, "posts")
os.makedirs(PROFILE_PIC_DIR, exist_ok=True)
os.makedirs(POSTS_DIR, exist_ok=True)

SPONSOR_KEYWORDS = re.compile(
    r'#(ad|advertisement|sponsored|협찬|광고|제공|유료광고|ppㅣ|ppl|협찬제품|제품협찬|협업|파트너십|홍보)',
    re.IGNORECASE
)

_cl = None


def get_client(username, password, totp_secret, proxy_host="", proxy_port="",
               proxy_user="", proxy_pass=""):
    global _cl
    if _cl is not None:
        try:
            _cl.get_timeline_feed()
            return _cl
        except:
            _cl = None

    from instagrapi import Client
    cl = Client()
    cl.delay_range = [1, 3]

    # 프록시 설정
    if proxy_host and proxy_port:
        if proxy_user and proxy_pass:
            proxy_url = f"http://{proxy_user}:{proxy_pass}@{proxy_host}:{proxy_port}"
        else:
            proxy_url = f"http://{proxy_host}:{proxy_port}"
        cl.set_proxy(proxy_url)
        log.info(f"프록시 설정: {proxy_host}:{proxy_port}")

    try:
        code = pyotp.TOTP(totp_secret).now() if totp_secret else None
        cl.login(username, password, verification_code=code)
        log.info(f"인스타 로그인 성공: {username}")
        _cl = cl
        return cl
    except Exception as e:
        log.error(f"로그인 실패: {e}")
        raise


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

        # 프로필 사진 다운로드
        pic_local = ""
        try:
            u_info = cl.user_info(int(pk))
            pic_url = str(u_info.profile_pic_url) if u_info.profile_pic_url else ""
            if pic_url:
                pic_path = os.path.join(PROFILE_PIC_DIR, f"{username}.jpg")
                if download_image(pic_url, pic_path):
                    pic_local = f"profile_pics/{username}.jpg"
        except:
            pass

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


def crawl_hashtag(hashtag: str, requested_count: int,
                  username: str, password: str, totp_secret: str,
                  job_id: str,
                  proxy_host: str = "", proxy_port: str = "",
                  proxy_user: str = "", proxy_pass: str = ""):
    """해시태그 크롤링"""
    from database import get_conn, DB_PATH
    import time as _time

    progress[job_id] = {
        "status": "로그인 중", "hashtag": hashtag,
        "posts": 0, "new": 0, "updated": 0,
        "requested": requested_count, "done": False, "error": None
    }

    conn = get_conn()
    job_db_id = conn.execute(
        "INSERT INTO collect_jobs (hashtag, status, requested_count, started_at) VALUES (?,?,?,?)",
        (hashtag, "running", requested_count, _time.time())
    ).lastrowid
    conn.commit()
    conn.close()

    try:
        cl = get_client(username, password, totp_secret, proxy_host, proxy_port, proxy_user, proxy_pass)
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

        conn = get_conn()
        conn.execute("""
            UPDATE collect_jobs SET status='done', collected_posts=?, new_users=?, updated_users=?, finished_at=?
            WHERE id=?
        """, (collected_posts, new_cnt, updated_cnt, _time.time(), job_db_id))
        conn.execute("""
            UPDATE hashtags SET status='idle', total_collected=total_collected+?, last_run_at=?
            WHERE name=?
        """, (new_cnt + updated_cnt, _time.time(), hashtag))
        conn.commit()
        conn.close()

        progress[job_id].update({"status": "완료", "done": True, "new": new_cnt, "updated": updated_cnt})
        log.info(f"[{hashtag}] 완료: 게시물 {collected_posts}, 신규 {new_cnt}, 업데이트 {updated_cnt}")

    except Exception as e:
        log.error(f"[{hashtag}] 크롤링 에러: {e}")
        conn = get_conn()
        conn.execute("UPDATE collect_jobs SET status='error', error_msg=?, finished_at=? WHERE id=?",
                     (str(e), _time.time(), job_db_id))
        conn.execute("UPDATE hashtags SET status='error' WHERE name=?", (hashtag,))
        conn.commit()
        conn.close()
        progress[job_id].update({"status": "에러", "error": str(e), "done": True})


def refresh_all(username: str, password: str, totp_secret: str,
                proxy_host: str = "", proxy_port: str = "",
                proxy_user: str = "", proxy_pass: str = ""):
    """전체 인플루언서 상세 갱신 (24시간 이상 경과된 계정 우선)"""
    from database import get_conn

    refresh_progress["current"] = {"done": 0, "total": 0, "current_user": "", "running": True, "error": None}

    conn = get_conn()
    job_id = conn.execute(
        "INSERT INTO refresh_jobs (status, started_at) VALUES (?,?)",
        ("running", time.time())
    ).lastrowid
    conn.commit()
    conn.close()

    try:
        cl = get_client(username, password, totp_secret, proxy_host, proxy_port, proxy_user, proxy_pass)

        conn = get_conn()
        # 24시간 이상 지난 계정 or 한번도 수집 안한 계정
        cutoff = time.time() - 86400
        rows = conn.execute(
            "SELECT pk, username, follower_count FROM influencers "
            "WHERE stats_updated_at < ? OR stats_updated_at IS NULL "
            "ORDER BY stats_updated_at ASC",
            (cutoff,)
        ).fetchall()
        conn.close()

        total = len(rows)
        refresh_progress["current"]["total"] = total

        conn2 = get_conn()
        conn2.execute("UPDATE refresh_jobs SET total=? WHERE id=?", (total, job_id))
        conn2.commit()
        conn2.close()

        for i, row in enumerate(rows):
            pk, uname, followers = row["pk"], row["username"], row["follower_count"]
            refresh_progress["current"]["current_user"] = uname
            log.info(f"갱신 중 [{i+1}/{total}]: @{uname}")

            crawl_user_detail(cl, pk, uname, followers or 0)

            refresh_progress["current"]["done"] = i + 1
            conn3 = get_conn()
            conn3.execute("UPDATE refresh_jobs SET done=?, current_user=? WHERE id=?", (i + 1, uname, job_id))
            conn3.commit()
            conn3.close()
            time.sleep(2)

        conn4 = get_conn()
        conn4.execute("UPDATE refresh_jobs SET status='done', finished_at=? WHERE id=?",
                      (time.time(), job_id))
        conn4.commit()
        conn4.close()

        refresh_progress["current"]["running"] = False
        log.info(f"전체 갱신 완료: {total}개")

    except Exception as e:
        log.error(f"전체 갱신 에러: {e}")
        conn5 = get_conn()
        conn5.execute("UPDATE refresh_jobs SET status='error', error_msg=?, finished_at=? WHERE id=?",
                      (str(e), time.time(), job_id))
        conn5.commit()
        conn5.close()
        refresh_progress["current"]["running"] = False
        refresh_progress["current"]["error"] = str(e)
