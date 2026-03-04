"""
최초 배포 시 샘플 데이터 생성 스크립트
fly ssh console 에서 python seed.py 로 실행
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))

from database import init_db
import sqlite3, time, json, random

DB_PATH = os.environ.get("DB_PATH", os.path.join(os.path.dirname(__file__), "insta.db"))

def seed():
    init_db()
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM influencers")
    if cur.fetchone()[0] > 0:
        print("데이터 이미 존재, 스킵")
        conn.close()
        return

    print("샘플 데이터 생성 중...")

    categories = ['뷰티','패션','푸드','여행','운동','육아','라이프','게임','음악','교육']
    hashtag_pool = {
        '뷰티': ['스킨케어','메이크업','화장품','피부관리','뷰티'],
        '패션': ['ootd','패션','데일리룩','스타일','코디'],
        '푸드': ['맛집','먹방','레시피','카페','디저트'],
        '여행': ['여행','여행스타그램','해외여행','국내여행','제주도'],
        '운동': ['헬스','다이어트','필라테스','요가','홈트'],
        '육아': ['육아','아기','맘스타그램','임신','이유식'],
        '라이프': ['일상','홈인테리어','미니멀라이프','반려동물','제로웨이스트'],
        '게임': ['게임','게이머','스트리머','롤','배그'],
        '음악': ['음악','뮤지션','커버곡','힙합','인디'],
        '교육': ['공부','스터디','독서','취업','코딩'],
    }

    # 1000명 인플루언서 생성
    now = time.time()
    influencers = []
    for i in range(1000):
        cat = random.choice(categories)
        followers = random.choice([
            random.randint(5000, 10000),
            random.randint(10000, 50000),
            random.randint(50000, 200000),
            random.randint(200000, 1000000),
        ])
        pk = str(300000 + i)
        uname = f"{cat.lower()}_{i:04d}"
        er = round(random.uniform(1.0, 8.0), 2)
        avg_rv = int(followers * random.uniform(0.5, 3.0))
        avg_fl = int(followers * er / 100 * random.uniform(0.6, 1.4))

        tags = ','.join(random.sample(hashtag_pool.get(cat, ['일상']), min(3, len(hashtag_pool.get(cat, ['일상'])))))
        influencers.append((pk, uname, f"인플루언서 {i}", "", followers,
                            random.randint(100, 2000), random.randint(20, 500),
                            0, 0, 1, cat, "", "", "", "", tags,
                            er, round(avg_fl * 0.05, 1), er,
                            avg_rv, avg_fl, round(avg_fl * 0.04, 1),
                            round(avg_rv * 0.06, 1), round(avg_rv * 0.005, 1),
                            random.randint(15, 35), random.randint(20, 50),
                            round(random.uniform(10, 40), 1),
                            round(random.randint(15, 40) / (15 + 40) * 100, 1),
                            now, now))

    cur.executemany("""
        INSERT OR IGNORE INTO influencers
        (pk, username, full_name, biography, follower_count, following_count, media_count,
         is_private, is_verified, is_business, category, public_email, public_phone, external_url,
         profile_pic_url, hashtags,
         engagement_rate, avg_comments, avg_likes, avg_reel_views, avg_feed_likes,
         avg_feed_comments, avg_reel_likes, avg_reel_comments,
         reel_count, feed_count, sponsored_ratio, reels_ratio,
         created_at, updated_at)
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, influencers)

    # manual 데이터
    for i in range(1000):
        pk = str(300000 + i)
        cat = categories[i % len(categories)]
        cur.execute("""
            INSERT OR IGNORE INTO influencer_manual
            (pk, can_live, main_category, feed_price, reel_price, live_price,
             collab_types, is_approved, quality_score,
             has_pet, is_married, has_kids, has_car, is_visual)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (pk, 1 if random.random() < 0.2 else 0, cat,
              random.choice([5,10,15,20,30,50]),
              random.choice([10,15,20,30,50,80]),
              random.choice([0,100,150,200,300]),
              random.choice(['체험단','PPL','공동구매','라이브']),
              1 if random.random() < 0.6 else 0,
              random.randint(1, 5),
              1 if random.random() < 0.3 else 0,
              1 if random.random() < 0.4 else 0,
              1 if random.random() < 0.35 else 0,
              1 if random.random() < 0.25 else 0,
              1 if random.random() < 0.3 else 0))

    conn.commit()
    print(f"완료: {cur.execute('SELECT COUNT(*) FROM influencers').fetchone()[0]}명 생성")
    conn.close()

if __name__ == "__main__":
    seed()
