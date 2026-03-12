"""PPT 생성 모듈 — 보고용 프리미엄 디자인"""
import io, os, json
from datetime import datetime
from pptx import Presentation
from pptx.util import Inches, Pt, Emu
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN
from pptx.util import Cm

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
KR_FONT = "맑은 고딕"  # Windows 기본 한국어 폰트 (PowerPoint에서 렌더링)

# 색상 팔레트
C_PURPLE  = RGBColor(0x63, 0x66, 0xf1)
C_DPURPLE = RGBColor(0x4f, 0x46, 0xe5)
C_DARK    = RGBColor(0x0f, 0x17, 0x2a)
C_GRAY    = RGBColor(0x64, 0x74, 0x8b)
C_LGRAY   = RGBColor(0x94, 0xa3, 0xb8)
C_WHITE   = RGBColor(0xff, 0xff, 0xff)
C_GREEN   = RGBColor(0x10, 0xb9, 0x81)
C_ORANGE  = RGBColor(0xf5, 0x9e, 0x0b)
C_BLUE    = RGBColor(0x3b, 0x82, 0xf6)
C_LIGHT   = RGBColor(0xf8, 0xfa, 0xfc)
C_BORDER  = RGBColor(0xe2, 0xe8, 0xf0)
C_PINK    = RGBColor(0xec, 0x48, 0x99)

def _fmt(n, suffix=""):
    try:
        n = int(n or 0)
        if n >= 10_000_000: return f"{n/10_000_000:.1f}천만{suffix}"
        if n >= 10_000: return f"{n/10_000:.1f}만{suffix}"
        return f"{n:,}{suffix}"
    except: return str(n or "-")

def _fmt_rate(r):
    try: return f"{float(r or 0):.2f}%"
    except: return "-"

def _tb(slide, text, left, top, w, h, size=10, bold=False, color=C_DARK, align=PP_ALIGN.LEFT):
    """텍스트 박스 추가"""
    box = slide.shapes.add_textbox(Cm(left), Cm(top), Cm(w), Cm(h))
    box.text_frame.word_wrap = True
    p = box.text_frame.paragraphs[0]
    p.alignment = align
    r = p.add_run()
    r.text = str(text)
    r.font.size = Pt(size)
    r.font.bold = bold
    r.font.color.rgb = color
    r.font.name = KR_FONT
    return box

def _rect(slide, left, top, w, h, fill_color, line=False):
    """사각형 도형"""
    shape = slide.shapes.add_shape(1, Cm(left), Cm(top), Cm(w), Cm(h))
    shape.fill.solid()
    shape.fill.fore_color.rgb = fill_color
    if line:
        shape.line.color.rgb = C_BORDER
        shape.line.width = Pt(0.5)
    else:
        shape.line.fill.background()
    return shape

def _stat_card(slide, label, value, x, y, w, h, value_color=C_PURPLE):
    """통계 카드 (배경 + 값 + 라벨)"""
    _rect(slide, x, y, w, h, C_WHITE, line=True)
    _tb(slide, value, x, y+0.2, w, h*0.55, size=15, bold=True, color=value_color, align=PP_ALIGN.CENTER)
    _tb(slide, label, x, y+h*0.55, w, h*0.4, size=8, color=C_GRAY, align=PP_ALIGN.CENTER)

def _perf_row(slide, label, vals, x, y, w, label_color=C_DARK):
    """성과 행 (라벨 + 3개 값)"""
    cw = w / 4
    _rect(slide, x, y, cw, 0.65, C_LIGHT, line=True)
    _tb(slide, label, x+0.2, y+0.05, cw-0.4, 0.55, size=8, bold=True, color=label_color)
    for i, v in enumerate(vals):
        _rect(slide, x+cw*(i+1), y, cw, 0.65, C_WHITE, line=True)
        _tb(slide, v, x+cw*(i+1), y+0.05, cw, 0.55, size=8, color=C_DARK, align=PP_ALIGN.CENTER)


def _add_scorecard_slide(prs, inf, manual):
    """인플루언서 1명 — 프리미엄 보고서 슬라이드"""
    slide = prs.slides.add_slide(prs.slide_layouts[6])
    W, H = 33.87, 19.05

    # 배경
    slide.background.fill.solid()
    slide.background.fill.fore_color.rgb = C_LIGHT

    username = inf.get("username", "")
    full_name = inf.get("full_name", "")

    # ═══ 헤더 바 ═══
    _rect(slide, 0, 0, W, 3.0, C_PURPLE)

    # 프로필 사진 (Supabase Storage → CDN → 로컬)
    _pic_added = False
    import requests as _req
    pk = inf.get("pk", "")
    _pic_urls = []
    if pk:
        sb_url = os.environ.get("SUPABASE_URL", "https://ysqnixgdpltguatvjjcb.supabase.co")
        _pic_urls.append(f"{sb_url}/storage/v1/object/public/profile-pics/{pk}.jpg")
    _local_val = inf.get("profile_pic_local", "") or ""
    if _local_val.startswith("http"):
        _pic_urls.append(_local_val)
    _cdn_val = inf.get("profile_pic_url", "") or ""
    if _cdn_val:
        _pic_urls.append(_cdn_val)
    for _purl in _pic_urls:
        try:
            _pr = _req.get(_purl, timeout=5)
            if _pr.status_code == 200 and len(_pr.content) > 500:
                _pbuf = io.BytesIO(_pr.content)
                slide.shapes.add_picture(_pbuf, Cm(1), Cm(0.25), Cm(2.5), Cm(2.5))
                _pic_added = True
                break
        except: continue
    if not _pic_added:
        pic_path = os.path.join(DATA_DIR, "profile_pics", f"{username}.jpg")
        if os.path.exists(pic_path):
            try:
                slide.shapes.add_picture(pic_path, Cm(1), Cm(0.25), Cm(2.5), Cm(2.5))
            except: pass

    _tb(slide, f"@{username}", 4, 0.2, 15, 1.0, size=20, bold=True, color=C_WHITE)
    sub_text = full_name
    if inf.get("is_verified"): sub_text += "  |  인증 계정"
    if manual.get("can_live"): sub_text += "  |  라이브 가능"
    _tb(slide, sub_text, 4, 1.2, 15, 0.6, size=10, color=RGBColor(0xc7, 0xd2, 0xfe))
    _tb(slide, f"instagram.com/{username}", 4, 1.9, 15, 0.5, size=8, color=RGBColor(0xa5, 0xb4, 0xfc))

    # SuperTag 로고
    _tb(slide, "SuperTag", W-5, 0.3, 4.5, 0.8, size=11, bold=True, color=C_WHITE, align=PP_ALIGN.RIGHT)
    _tb(slide, datetime.now().strftime('%Y.%m.%d'), W-5, 1.1, 4.5, 0.5, size=8,
        color=RGBColor(0xa5, 0xb4, 0xfc), align=PP_ALIGN.RIGHT)

    # ═══ 핵심 지표 6개 ═══
    y = 3.4
    card_w = (W - 1.6) / 6
    stats = [
        ("팔로워", _fmt(inf.get("follower_count")), C_PURPLE),
        ("팔로잉", _fmt(inf.get("following_count")), C_DARK),
        ("총 게시물", _fmt(inf.get("media_count")), C_DARK),
        ("참여율", _fmt_rate(inf.get("engagement_rate")), C_GREEN),
        ("릴스 비율", _fmt_rate(inf.get("reels_ratio")), C_BLUE),
        ("협찬 비율", _fmt_rate(inf.get("sponsored_ratio")), C_ORANGE),
    ]
    for i, (lbl, val, clr) in enumerate(stats):
        _stat_card(slide, lbl, val, 0.5 + i*(card_w+0.1), y, card_w, 1.9, clr)

    # ═══ 피드/릴스 성과 비교 ═══
    y2 = 5.7
    sec_w = 16
    _rect(slide, 0.5, y2, sec_w, 0.5, C_PURPLE)
    _tb(slide, "피드 / 릴스 성과 비교", 0.7, y2+0.03, 10, 0.45, size=9, bold=True, color=C_WHITE)

    # 헤더
    cw4 = sec_w / 4
    header_y = y2 + 0.55
    for i, h in enumerate(["", "평균 좋아요", "평균 댓글", "평균 조회수"]):
        _rect(slide, 0.5 + i*cw4, header_y, cw4, 0.55, RGBColor(0xf1, 0xf5, 0xf9), line=True)
        _tb(slide, h, 0.5 + i*cw4, header_y+0.05, cw4, 0.45, size=7.5, bold=True, color=C_GRAY, align=PP_ALIGN.CENTER)

    _perf_row(slide, "피드", [
        _fmt(inf.get("avg_feed_likes")), _fmt(inf.get("avg_feed_comments")), "-"
    ], 0.5, header_y+0.6, sec_w, C_ORANGE)

    _perf_row(slide, "릴스", [
        _fmt(inf.get("avg_reel_likes")), _fmt(inf.get("avg_reel_comments")), _fmt(inf.get("avg_reel_views"))
    ], 0.5, header_y+1.25, sec_w, C_BLUE)

    # ═══ 계정 정보 (오른쪽) ═══
    rx = 17
    rw = W - rx - 0.5
    _rect(slide, rx, y2, rw, 0.5, C_PURPLE)
    _tb(slide, "계정 정보 / 단가", rx+0.2, y2+0.03, 10, 0.45, size=9, bold=True, color=C_WHITE)

    info_rows = [
        ("카테고리", inf.get("category") or manual.get("main_category") or "-"),
        ("업로드 빈도", inf.get("upload_frequency") or "-"),
        ("마지막 게시", inf.get("last_post_date") or "-"),
        ("프로필 링크", (inf.get("external_url") or "-")[:35]),
        ("이메일", manual.get("contact_email") or inf.get("public_email") or "-"),
        ("피드 단가", f"{manual.get('feed_price') or 0:,}만원" if manual.get("feed_price") else "-"),
        ("릴스 단가", f"{manual.get('reel_price') or 0:,}만원" if manual.get("reel_price") else "-"),
        ("협업 유형", manual.get("collab_types") or "-"),
    ]
    iy = y2 + 0.55
    for i, (lbl, val) in enumerate(info_rows):
        row_bg = C_WHITE if i % 2 == 0 else C_LIGHT
        _rect(slide, rx, iy + i*0.55, rw, 0.55, row_bg, line=True)
        _tb(slide, lbl, rx+0.2, iy + i*0.55 + 0.05, 3.5, 0.45, size=7.5, bold=True, color=C_GRAY)
        _tb(slide, val, rx+3.8, iy + i*0.55 + 0.05, rw-4, 0.45, size=7.5, color=C_DARK)

    # ═══ 인기 게시물 ═══
    y3 = 10.2
    _rect(slide, 0.5, y3, W-1, 0.5, C_PURPLE)
    _tb(slide, "인기 게시물 TOP 3", 0.7, y3+0.03, 10, 0.45, size=9, bold=True, color=C_WHITE)

    top_likes = inf.get("top_posts_likes", [])
    top_reels = inf.get("top_reels_views", [])

    post_y = y3 + 0.55
    # 헤더
    pcols = [3, 8, 4, 4, 4, 4]  # 유형, URL, 좋아요, 댓글, 조회수, 유형
    pheaders = ["유형", "게시물 코드", "좋아요", "댓글", "조회수"]
    px = 0.5
    for i, (h, pw) in enumerate(zip(pheaders, pcols)):
        _rect(slide, px, post_y, pw, 0.5, RGBColor(0xf1,0xf5,0xf9), line=True)
        _tb(slide, h, px, post_y+0.05, pw, 0.4, size=7.5, bold=True, color=C_GRAY, align=PP_ALIGN.CENTER)
        px += pw

    all_posts = []
    for p in (top_likes or [])[:3]:
        if isinstance(p, dict):
            url = p.get("url", "")
            code = url.split("/p/")[-1].rstrip("/")[:15] if "/p/" in url else "-"
            all_posts.append(("피드", code, _fmt(p.get("likes",0)), _fmt(p.get("comments",0)), "-"))
    for p in (top_reels or [])[:3]:
        if isinstance(p, dict):
            url = p.get("url", "")
            code = url.split("/p/")[-1].rstrip("/")[:15] if "/p/" in url else "-"
            all_posts.append(("릴스", code, _fmt(p.get("likes",0)), "-", _fmt(p.get("views",0))))

    for ri, row_data in enumerate(all_posts[:5]):
        ry = post_y + 0.55 + ri * 0.5
        bg = C_WHITE if ri % 2 == 0 else C_LIGHT
        px = 0.5
        for ci, (val, pw) in enumerate(zip(row_data, pcols)):
            _rect(slide, px, ry, pw, 0.5, bg, line=True)
            clr = C_BLUE if ci == 0 and val == "릴스" else (C_ORANGE if ci == 0 else C_DARK)
            _tb(slide, val, px, ry+0.05, pw, 0.4, size=7.5, color=clr,
                align=PP_ALIGN.CENTER if ci > 0 else PP_ALIGN.CENTER, bold=(ci==0))
            px += pw

    # ═══ 해시태그 ═══
    y4 = 14
    htags_raw = inf.get("top_hashtags", "[]")
    try: htags = json.loads(htags_raw) if isinstance(htags_raw, str) else (htags_raw or [])
    except: htags = []
    if htags:
        _rect(slide, 0.5, y4, W-1, 0.45, C_PURPLE)
        _tb(slide, "자주 사용하는 해시태그", 0.7, y4+0.02, 10, 0.4, size=8.5, bold=True, color=C_WHITE)
        tag_text = "  ".join([f"#{h['tag']}({h['count']})" for h in htags[:12]])
        _rect(slide, 0.5, y4+0.5, W-1, 0.7, C_WHITE, line=True)
        _tb(slide, tag_text, 0.7, y4+0.55, W-1.4, 0.6, size=8, color=C_PURPLE)

    # ═══ 메모 ═══
    memo_y = 15.2
    notes = manual.get("notes", "")
    brands = manual.get("past_brands", "")
    if notes or brands:
        _rect(slide, 0.5, memo_y, W-1, 1.2, C_WHITE, line=True)
        if notes:
            _tb(slide, f"메모: {notes}", 0.7, memo_y+0.1, W-1.4, 0.5, size=7.5, color=C_GRAY)
        if brands:
            _tb(slide, f"협업 브랜드: {brands}", 0.7, memo_y+0.6, W-1.4, 0.5, size=7.5, color=C_GRAY)

    # ═══ 바이오 ═══
    bio = inf.get("biography", "")
    if bio:
        _rect(slide, 0.5, 16.5, W-1, 0.8, C_WHITE, line=True)
        _tb(slide, f"바이오: {bio[:100]}", 0.7, 16.55, W-1.4, 0.7, size=7, color=C_GRAY)

    # ═══ 푸터 ═══
    _rect(slide, 0, H-0.6, W, 0.6, C_PURPLE)
    _tb(slide, f"SuperTag  |  {datetime.now().strftime('%Y-%m-%d %H:%M')}  |  Confidential",
        0.5, H-0.55, W-1, 0.5, size=7, color=RGBColor(0xc7,0xd2,0xfe), align=PP_ALIGN.RIGHT)


def export_single_ppt(inf: dict, manual: dict) -> bytes:
    prs = Presentation()
    prs.slide_width = Cm(33.87)
    prs.slide_height = Cm(19.05)
    _add_scorecard_slide(prs, inf, manual)
    buf = io.BytesIO()
    prs.save(buf)
    return buf.getvalue()


def export_multi_ppt(inf_list: list) -> bytes:
    prs = Presentation()
    prs.slide_width = Cm(33.87)
    prs.slide_height = Cm(19.05)
    for inf, manual in inf_list:
        _add_scorecard_slide(prs, inf, manual)
    buf = io.BytesIO()
    prs.save(buf)
    return buf.getvalue()


def export_list_ppt(inf_list: list) -> bytes:
    prs = Presentation()
    prs.slide_width = Cm(33.87)
    prs.slide_height = Cm(19.05)

    slide = prs.slides.add_slide(prs.slide_layouts[6])
    W, H = 33.87, 19.05

    slide.background.fill.solid()
    slide.background.fill.fore_color.rgb = C_LIGHT

    _rect(slide, 0, 0, W, 2.2, C_PURPLE)
    _tb(slide, "인플루언서 비교 리스트", 0.8, 0.3, 20, 1, size=20, bold=True, color=C_WHITE)
    _tb(slide, f"총 {len(inf_list)}명  |  {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        0.8, 1.3, 20, 0.6, size=9, color=RGBColor(0xc7, 0xd2, 0xfe))

    rows = len(inf_list) + 1
    cols = 11
    left, top = Cm(0.3), Cm(2.5)
    width, height = Cm(W-0.6), Cm(H-3)

    tbl = slide.shapes.add_table(rows, cols, left, top, width, height).table
    headers = ["계정", "팔로워", "참여율", "릴스조회", "릴스좋아요", "피드좋아요",
               "게시물", "카테고리", "피드단가", "릴스단가", "협업유형"]

    for j, h in enumerate(headers):
        cell = tbl.cell(0, j)
        cell.text = h
        cell.fill.solid()
        cell.fill.fore_color.rgb = C_PURPLE
        p = cell.text_frame.paragraphs[0]
        p.alignment = PP_ALIGN.CENTER
        r = p.runs[0] if p.runs else p.add_run()
        r.font.bold = True
        r.font.size = Pt(8)
        r.font.color.rgb = C_WHITE
        r.font.name = KR_FONT

    for i, (inf, manual) in enumerate(inf_list):
        row_vals = [
            f"@{inf.get('username', '')}",
            _fmt(inf.get("follower_count")),
            _fmt_rate(inf.get("engagement_rate")),
            _fmt(inf.get("avg_reel_views")),
            _fmt(inf.get("avg_reel_likes")),
            _fmt(inf.get("avg_feed_likes")),
            _fmt(inf.get("media_count")),
            (inf.get("category") or manual.get("main_category") or "-")[:10],
            f"{manual.get('feed_price') or 0}만" if manual.get("feed_price") else "-",
            f"{manual.get('reel_price') or 0}만" if manual.get("reel_price") else "-",
            manual.get("collab_types") or "-",
        ]
        bg = C_LIGHT if i % 2 == 0 else C_WHITE
        for j, val in enumerate(row_vals):
            cell = tbl.cell(i+1, j)
            cell.text = str(val)
            cell.fill.solid()
            cell.fill.fore_color.rgb = bg
            p = cell.text_frame.paragraphs[0]
            p.alignment = PP_ALIGN.CENTER if j > 0 else PP_ALIGN.LEFT
            r = p.runs[0] if p.runs else p.add_run()
            r.font.size = Pt(8)
            r.font.color.rgb = C_DARK
            r.font.name = KR_FONT

    # 푸터
    _rect(slide, 0, H-0.5, W, 0.5, C_PURPLE)
    _tb(slide, "SuperTag  |  Confidential", 0.5, H-0.45, W-1, 0.4,
        size=7, color=RGBColor(0xc7, 0xd2, 0xfe), align=PP_ALIGN.RIGHT)

    buf = io.BytesIO()
    prs.save(buf)
    return buf.getvalue()
