"""PPT 생성 모듈"""
import io, os, json
from datetime import datetime
from pptx import Presentation
from pptx.util import Inches, Pt, Emu
from pptx.dml.color import RGBColor
from pptx.enum.text import PP_ALIGN
from pptx.util import Cm

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

C_PURPLE = RGBColor(0x63, 0x66, 0xf1)
C_DARK   = RGBColor(0x0f, 0x17, 0x2a)
C_GRAY   = RGBColor(0x64, 0x74, 0x8b)
C_WHITE  = RGBColor(0xff, 0xff, 0xff)
C_GREEN  = RGBColor(0x10, 0xb9, 0x81)
C_ORANGE = RGBColor(0xf5, 0x9e, 0x0b)
C_BLUE   = RGBColor(0x3b, 0x82, 0xf6)
C_LIGHT  = RGBColor(0xf1, 0xf5, 0xf9)

def _fmt_num(n):
    try:
        n = int(n or 0)
        if n >= 10000: return f"{n//10000}만{(n%10000)//1000}천" if (n%10000)//1000 else f"{n//10000}만"
        return f"{n:,}"
    except: return str(n or "-")

def _fmt_rate(r):
    try: return f"{float(r or 0):.2f}%"
    except: return "-"

def _add_text_box(slide, text, left, top, width, height,
                  font_size=12, bold=False, color=C_DARK, align=PP_ALIGN.LEFT, bg=None):
    txBox = slide.shapes.add_textbox(Cm(left), Cm(top), Cm(width), Cm(height))
    if bg:
        txBox.fill.solid()
        txBox.fill.fore_color.rgb = bg
    tf = txBox.text_frame
    tf.word_wrap = True
    p = tf.paragraphs[0]
    p.alignment = align
    run = p.add_run()
    run.text = str(text)
    run.font.size = Pt(font_size)
    run.font.bold = bold
    run.font.color.rgb = color
    return txBox

def _add_stat_box(slide, label, value, left, top, color):
    # 배경 박스
    shape = slide.shapes.add_shape(
        1,  # MSO_SHAPE_TYPE.RECTANGLE
        Cm(left), Cm(top), Cm(5.8), Cm(2.4)
    )
    shape.fill.solid()
    shape.fill.fore_color.rgb = C_LIGHT
    shape.line.color.rgb = RGBColor(0xe2, 0xe8, 0xf0)
    shape.line.width = Pt(0.5)

    _add_text_box(slide, value, left+0.2, top+0.1, 5.4, 1.2,
                  font_size=16, bold=True, color=color, align=PP_ALIGN.CENTER)
    _add_text_box(slide, label, left+0.2, top+1.3, 5.4, 0.8,
                  font_size=9, bold=False, color=C_GRAY, align=PP_ALIGN.CENTER)

def _add_info_row(slide, label, value, left, top, width=11):
    _add_text_box(slide, label, left, top, 3, 0.5,
                  font_size=8, bold=True, color=C_GRAY)
    _add_text_box(slide, value, left+3, top, width-3, 0.5,
                  font_size=8, bold=False, color=C_DARK)

def _add_scorecard_slide(prs, inf, manual):
    """인플루언서 1명 슬라이드 추가"""
    slide_layout = prs.slide_layouts[6]  # blank
    slide = prs.slides.add_slide(slide_layout)

    W, H = 33.87, 19.05  # 가로형 cm

    # ── 배경 ──────────────────────────────────────────────
    bg = slide.background
    fill = bg.fill
    fill.solid()
    fill.fore_color.rgb = RGBColor(0xff, 0xff, 0xff)

    # ── 상단 헤더 바 ───────────────────────────────────────
    header_bg = slide.shapes.add_shape(1, Cm(0), Cm(0), Cm(W), Cm(3.2))
    header_bg.fill.solid()
    header_bg.fill.fore_color.rgb = C_PURPLE
    header_bg.line.fill.background()

    username = inf.get("username", "")
    full_name = inf.get("full_name", "")

    # 프로필 사진 (있으면)
    pic_path = os.path.join(DATA_DIR, "profile_pics", f"{username}.jpg")
    if os.path.exists(pic_path):
        try:
            slide.shapes.add_picture(pic_path, Cm(0.5), Cm(0.3), Cm(2.6), Cm(2.6))
        except: pass

    _add_text_box(slide, f"@{username}", 3.5, 0.3, 15, 1.2,
                  font_size=18, bold=True, color=C_WHITE)
    _add_text_box(slide, full_name, 3.5, 1.4, 15, 0.8,
                  font_size=11, color=RGBColor(0xc7, 0xd2, 0xfe))
    _add_text_box(slide, f"instagram.com/{username}", 3.5, 2.1, 15, 0.7,
                  font_size=9, color=RGBColor(0xa5, 0xb4, 0xfc))

    # 배지
    badge_x = 19
    if inf.get("is_verified"):
        badge = slide.shapes.add_shape(1, Cm(badge_x), Cm(0.5), Cm(3), Cm(0.8))
        badge.fill.solid(); badge.fill.fore_color.rgb = C_BLUE
        badge.line.fill.background()
        _add_text_box(slide, "✓ 인증 계정", badge_x+0.1, 0.5, 2.8, 0.8,
                      font_size=8, bold=True, color=C_WHITE, align=PP_ALIGN.CENTER)
        badge_x += 3.5

    if manual.get("can_live"):
        badge = slide.shapes.add_shape(1, Cm(badge_x), Cm(0.5), Cm(4), Cm(0.8))
        badge.fill.solid(); badge.fill.fore_color.rgb = C_GREEN
        badge.line.fill.background()
        _add_text_box(slide, "📡 라이브커머스 가능", badge_x+0.1, 0.5, 3.8, 0.8,
                      font_size=8, bold=True, color=C_WHITE, align=PP_ALIGN.CENTER)

    # ── 핵심 통계 4박스 ───────────────────────────────────
    y_stat = 3.6
    _add_stat_box(slide, "팔로워",    _fmt_num(inf.get("follower_count")), 0.5, y_stat, C_PURPLE)
    _add_stat_box(slide, "참여율",    _fmt_rate(inf.get("engagement_rate")), 6.5, y_stat, C_GREEN)
    _add_stat_box(slide, "평균릴스조회", _fmt_num(inf.get("avg_reel_views")), 12.5, y_stat, C_BLUE)
    _add_stat_box(slide, "평균좋아요", _fmt_num(inf.get("avg_likes")), 18.5, y_stat, C_ORANGE)

    # ── 정보 행들 ─────────────────────────────────────────
    y_info = 6.4
    # 왼쪽 컬럼 (크롤링 데이터)
    left_info = [
        ("팔로잉",    _fmt_num(inf.get("following_count"))),
        ("총 게시물", _fmt_num(inf.get("media_count"))),
        ("릴스 비율", _fmt_rate(inf.get("reels_ratio"))),
        ("협찬 비율", _fmt_rate(inf.get("sponsored_ratio"))),
        ("업로드 빈도", inf.get("upload_frequency") or "-"),
        ("활성 시간", inf.get("active_hours") or "-"),
        ("마지막 게시", inf.get("last_post_date") or "-"),
        ("카테고리",  inf.get("category") or manual.get("main_category") or "-"),
    ]
    for i, (lbl, val) in enumerate(left_info):
        _add_info_row(slide, lbl, val, 0.5, y_info + i*0.65)

    # 오른쪽 컬럼 (수동 입력)
    right_info = [
        ("담당자",    manual.get("contact_name") or "-"),
        ("카카오",    manual.get("contact_kakao") or "-"),
        ("이메일",    manual.get("contact_email") or inf.get("public_email") or "-"),
        ("협업 유형", manual.get("collab_types") or "-"),
        ("피드 단가", f"{manual.get('feed_price') or 0}만원" if manual.get("feed_price") else "-"),
        ("릴스 단가", f"{manual.get('reel_price') or 0}만원" if manual.get("reel_price") else "-"),
        ("라이브 단가", f"{manual.get('live_price') or 0}만원" if manual.get("live_price") else "-"),
        ("품질 점수", "★" * int(manual.get("quality_score") or 0) or "-"),
    ]
    for i, (lbl, val) in enumerate(right_info):
        _add_info_row(slide, lbl, val, 16.5, y_info + i*0.65)

    # ── 메모 ──────────────────────────────────────────────
    if manual.get("notes"):
        _add_text_box(slide, f"📝 {manual.get('notes')}", 0.5, 12.2, W-1, 0.8,
                      font_size=8, color=C_GRAY)

    # ── 과거 브랜드 ───────────────────────────────────────
    if manual.get("past_brands"):
        _add_text_box(slide, f"🏷 협업 브랜드: {manual.get('past_brands')}", 0.5, 12.9, W-1, 0.6,
                      font_size=8, color=C_GRAY)

    # ── 푸터 ──────────────────────────────────────────────
    footer_bg = slide.shapes.add_shape(1, Cm(0), Cm(H-0.8), Cm(W), Cm(0.8))
    footer_bg.fill.solid(); footer_bg.fill.fore_color.rgb = C_LIGHT
    footer_bg.line.fill.background()
    _add_text_box(slide, f"SuperTag  |  생성: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                  0.5, H-0.75, W-1, 0.7,
                  font_size=7, color=C_GRAY, align=PP_ALIGN.RIGHT)


def export_single_ppt(inf: dict, manual: dict) -> bytes:
    prs = Presentation()
    prs.slide_width  = Cm(33.87)
    prs.slide_height = Cm(19.05)
    _add_scorecard_slide(prs, inf, manual)
    buf = io.BytesIO()
    prs.save(buf)
    return buf.getvalue()


def export_multi_ppt(inf_list: list) -> bytes:
    """여러 인플루언서 스코어카드 PPT (1슬라이드 1명)"""
    prs = Presentation()
    prs.slide_width  = Cm(33.87)
    prs.slide_height = Cm(19.05)
    for inf, manual in inf_list:
        _add_scorecard_slide(prs, inf, manual)
    buf = io.BytesIO()
    prs.save(buf)
    return buf.getvalue()


def export_list_ppt(inf_list: list) -> bytes:
    """비교 리스트형 PPT (1슬라이드에 표)"""
    prs = Presentation()
    prs.slide_width  = Cm(33.87)
    prs.slide_height = Cm(19.05)

    slide = prs.slides.add_slide(prs.slide_layouts[6])
    W, H = 33.87, 19.05

    bg = slide.background; bg.fill.solid(); bg.fill.fore_color.rgb = RGBColor(0xff,0xff,0xff)

    header_bg = slide.shapes.add_shape(1, Cm(0), Cm(0), Cm(W), Cm(2))
    header_bg.fill.solid(); header_bg.fill.fore_color.rgb = C_PURPLE
    header_bg.line.fill.background()

    _add_text_box(slide, "인플루언서 비교 리스트", 0.5, 0.3, 20, 1,
                  font_size=18, bold=True, color=C_WHITE)
    _add_text_box(slide, f"총 {len(inf_list)}명  |  {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                  0.5, 1.2, 20, 0.6, font_size=9, color=RGBColor(0xc7,0xd2,0xfe))

    # 표
    rows = len(inf_list) + 1
    cols = 12
    left, top = Cm(0.3), Cm(2.3)
    width, height = Cm(W-0.6), Cm(H-2.8)

    tbl = slide.shapes.add_table(rows, cols, left, top, width, height).table
    headers = ["계정", "팔로워", "참여율", "평균릴스뷰", "게시물", "릴스%",
               "카테고리", "라이브", "피드단가", "릴스단가", "협업유형", "품질"]
    for j, h in enumerate(headers):
        cell = tbl.cell(0, j)
        cell.text = h
        cell.fill.solid(); cell.fill.fore_color.rgb = C_PURPLE
        p = cell.text_frame.paragraphs[0]
        p.alignment = PP_ALIGN.CENTER
        run = p.runs[0] if p.runs else p.add_run()
        run.font.bold = True; run.font.size = Pt(8); run.font.color.rgb = C_WHITE

    for i, (inf, manual) in enumerate(inf_list):
        row_vals = [
            f"@{inf.get('username','')}",
            _fmt_num(inf.get("follower_count")),
            _fmt_rate(inf.get("engagement_rate")),
            _fmt_num(inf.get("avg_reel_views")),
            _fmt_num(inf.get("media_count")),
            _fmt_rate(inf.get("reels_ratio")),
            inf.get("category") or manual.get("main_category") or "-",
            "O" if manual.get("can_live") else "-",
            f"{manual.get('feed_price') or 0}만" if manual.get("feed_price") else "-",
            f"{manual.get('reel_price') or 0}만" if manual.get("reel_price") else "-",
            manual.get("collab_types") or "-",
            "★" * int(manual.get("quality_score") or 0) or "-",
        ]
        bg_color = C_LIGHT if i % 2 == 0 else RGBColor(0xff,0xff,0xff)
        for j, val in enumerate(row_vals):
            cell = tbl.cell(i+1, j)
            cell.text = str(val)
            cell.fill.solid(); cell.fill.fore_color.rgb = bg_color
            p = cell.text_frame.paragraphs[0]
            p.alignment = PP_ALIGN.CENTER if j > 0 else PP_ALIGN.LEFT
            run = p.runs[0] if p.runs else p.add_run()
            run.font.size = Pt(8); run.font.color.rgb = C_DARK

    buf = io.BytesIO()
    prs.save(buf)
    return buf.getvalue()
