"""PDF 생성 모듈"""
import io, json, os
from datetime import datetime
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib import colors
from reportlab.lib.units import cm
from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, Table,
                                 TableStyle, Image as RLImage, PageBreak, HRFlowable)
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from PIL import Image as PILImage

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

# 폰트 등록 (Nanum Gothic 사용, 없으면 Helvetica)
FONT_NAME = "Helvetica"
FONT_BOLD = "Helvetica-Bold"
try:
    from reportlab.pdfbase.cidfonts import UnicodeCIDFont
    pdfmetrics.registerFont(UnicodeCIDFont('HYSMyeongJoStd-Medium'))
    FONT_NAME = 'HYSMyeongJoStd-Medium'
    FONT_BOLD = 'HYSMyeongJoStd-Medium'
except:
    pass

C_PURPLE   = colors.HexColor("#6366f1")
C_INDIGO   = colors.HexColor("#4f46e5")
C_GRAY     = colors.HexColor("#64748b")
C_LIGHTGRAY= colors.HexColor("#f1f5f9")
C_DARK     = colors.HexColor("#0f172a")
C_GREEN    = colors.HexColor("#10b981")
C_ORANGE   = colors.HexColor("#f59e0b")
C_BLUE     = colors.HexColor("#3b82f6")
C_RED      = colors.HexColor("#ef4444")

def _fmt_num(n):
    try:
        n = int(n or 0)
        if n >= 10000: return f"{n//10000}만{(n%10000)//1000}천" if (n%10000)//1000 else f"{n//10000}만"
        return f"{n:,}"
    except: return str(n or "-")

def _fmt_rate(r):
    try: return f"{float(r or 0):.2f}%"
    except: return "-"

def _get_profile_img(username, size=(60, 60)):
    """프로필 이미지 로드"""
    path = os.path.join(DATA_DIR, "profile_pics", f"{username}.jpg")
    if os.path.exists(path):
        try:
            img = PILImage.open(path).convert("RGB")
            img.thumbnail(size, PILImage.LANCZOS)
            buf = io.BytesIO()
            img.save(buf, format="JPEG")
            buf.seek(0)
            return RLImage(buf, width=size[0], height=size[1])
        except: pass
    return None

def _scorecard_elements(inf, manual, posts_summary=None):
    """인플루언서 1명 스코어카드 요소 생성"""
    elements = []
    W = 25*cm  # 가로 A4
    col1 = 7*cm
    col2 = 18*cm

    styles = getSampleStyleSheet()
    h1 = ParagraphStyle("h1", fontName=FONT_BOLD, fontSize=18, textColor=C_DARK, spaceAfter=2)
    h2 = ParagraphStyle("h2", fontName=FONT_BOLD, fontSize=11, textColor=C_PURPLE, spaceAfter=4)
    body = ParagraphStyle("body", fontName=FONT_NAME, fontSize=9, textColor=C_GRAY, leading=14)
    small = ParagraphStyle("small", fontName=FONT_NAME, fontSize=8, textColor=C_GRAY)

    username = inf.get("username", "")
    full_name = inf.get("full_name", "")

    # ── 헤더 ──────────────────────────────────────────────
    header_data = [[]]
    pic = _get_profile_img(username, (55, 55))
    if pic:
        header_data[0].append(pic)
    else:
        # 이니셜 박스
        header_data[0].append(Paragraph(f"<b>{username[0].upper() if username else '?'}</b>",
                                         ParagraphStyle("init", fontName=FONT_BOLD, fontSize=22,
                                                        textColor=colors.white, alignment=TA_CENTER)))

    name_para = [
        Paragraph(f"<b>@{username}</b>", ParagraphStyle("un", fontName=FONT_BOLD, fontSize=14, textColor=C_DARK)),
        Paragraph(full_name, ParagraphStyle("fn", fontName=FONT_NAME, fontSize=10, textColor=C_GRAY)),
        Paragraph(f"<a href='https://www.instagram.com/{username}/'>instagram.com/{username}</a>",
                  ParagraphStyle("link", fontName=FONT_NAME, fontSize=8, textColor=C_PURPLE)),
    ]
    if inf.get("is_verified"):
        name_para.append(Paragraph("✓ 인증 계정", ParagraphStyle("v", fontName=FONT_BOLD, fontSize=8, textColor=C_BLUE)))
    if manual.get("can_live"):
        name_para.append(Paragraph("📡 라이브커머스 가능", ParagraphStyle("live", fontName=FONT_BOLD, fontSize=8, textColor=C_GREEN)))

    header_data[0].append(name_para)

    header_tbl = Table(header_data, colWidths=[65, W-65])
    header_tbl.setStyle(TableStyle([
        ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
        ("BACKGROUND", (0,0), (0,0), C_PURPLE),
        ("ROWBACKGROUNDS", (1,0), (-1,-1), [C_LIGHTGRAY]),
        ("BOX", (0,0), (-1,-1), 1, colors.HexColor("#e2e8f0")),
        ("PADDING", (0,0), (-1,-1), 8),
    ]))
    elements.append(header_tbl)
    elements.append(Spacer(1, 8))

    # ── 핵심 통계 박스 ─────────────────────────────────────
    stats_data = [
        [
            _stat_cell("팔로워", _fmt_num(inf.get("follower_count")), C_PURPLE),
            _stat_cell("참여율", _fmt_rate(inf.get("engagement_rate")), C_GREEN),
            _stat_cell("평균 릴스 조회", _fmt_num(inf.get("avg_reel_views")), C_BLUE),
            _stat_cell("평균 좋아요", _fmt_num(inf.get("avg_likes")), C_ORANGE),
        ]
    ]
    col_w = W / 4
    stats_tbl = Table(stats_data, colWidths=[col_w]*4)
    stats_tbl.setStyle(TableStyle([
        ("ALIGN", (0,0), (-1,-1), "CENTER"),
        ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
        ("GRID", (0,0), (-1,-1), 0.5, colors.HexColor("#e2e8f0")),
        ("PADDING", (0,0), (-1,-1), 10),
        ("ROWBACKGROUNDS", (0,0), (-1,-1), [colors.white]),
        ("BOX", (0,0), (-1,-1), 1, colors.HexColor("#e2e8f0")),
    ]))
    elements.append(stats_tbl)
    elements.append(Spacer(1, 8))

    # ── 상세 정보 2단 ──────────────────────────────────────
    left_rows = [
        ["팔로잉", _fmt_num(inf.get("following_count"))],
        ["총 게시물", _fmt_num(inf.get("media_count"))],
        ["릴스 비율", _fmt_rate(inf.get("reels_ratio"))],
        ["협찬 비율", _fmt_rate(inf.get("sponsored_ratio"))],
        ["업로드 빈도", inf.get("upload_frequency") or "-"],
        ["활성 시간", inf.get("active_hours") or "-"],
        ["마지막 게시", inf.get("last_post_date") or "-"],
        ["카테고리", inf.get("category") or manual.get("main_category") or "-"],
    ]
    right_rows = [
        ["연락처", manual.get("contact_name") or "-"],
        ["카카오", manual.get("contact_kakao") or "-"],
        ["이메일", manual.get("contact_email") or inf.get("public_email") or "-"],
        ["협업 유형", manual.get("collab_types") or "-"],
        ["피드 단가", f"{manual.get('feed_price') or 0:,}만원" if manual.get("feed_price") else "-"],
        ["릴스 단가", f"{manual.get('reel_price') or 0:,}만원" if manual.get("reel_price") else "-"],
        ["라이브 단가", f"{manual.get('live_price') or 0:,}만원" if manual.get("live_price") else "-"],
        ["품질 점수", "★" * int(manual.get("quality_score") or 0) or "-"],
    ]

    def make_detail_table(rows):
        t = Table(rows, colWidths=[3.5*cm, 8.5*cm])
        t.setStyle(TableStyle([
            ("FONTNAME", (0,0), (0,-1), FONT_BOLD),
            ("FONTNAME", (1,0), (1,-1), FONT_NAME),
            ("FONTSIZE", (0,0), (-1,-1), 8),
            ("TEXTCOLOR", (0,0), (0,-1), C_GRAY),
            ("TEXTCOLOR", (1,0), (1,-1), C_DARK),
            ("ROWBACKGROUNDS", (0,0), (-1,-1), [colors.white, C_LIGHTGRAY]),
            ("GRID", (0,0), (-1,-1), 0.3, colors.HexColor("#e2e8f0")),
            ("PADDING", (0,0), (-1,-1), 5),
        ]))
        return t

    detail_data = [[make_detail_table(left_rows), make_detail_table(right_rows)]]
    detail_tbl = Table(detail_data, colWidths=[12.5*cm, 12.5*cm])
    detail_tbl.setStyle(TableStyle([("VALIGN",(0,0),(-1,-1),"TOP"),("PADDING",(0,0),(-1,-1),0)]))
    elements.append(detail_tbl)

    # ── 메모 ──────────────────────────────────────────────
    if manual.get("notes"):
        elements.append(Spacer(1, 6))
        elements.append(Paragraph(f"<b>메모:</b> {manual.get('notes')}", body))

    # ── 과거 협업 브랜드 ───────────────────────────────────
    if manual.get("past_brands"):
        elements.append(Paragraph(f"<b>협업 브랜드:</b> {manual.get('past_brands')}", body))

    elements.append(Spacer(1, 4))
    elements.append(HRFlowable(width="100%", thickness=0.5, color=colors.HexColor("#e2e8f0")))
    elements.append(Paragraph(
        f"<font color='#94a3b8' size='7'>InstaFinder | 생성일: {datetime.now().strftime('%Y-%m-%d %H:%M')} | 통계갱신: {inf.get('last_post_date') or '-'}</font>",
        ParagraphStyle("footer", fontName=FONT_NAME, fontSize=7, textColor=C_GRAY, alignment=TA_RIGHT)
    ))

    return elements


def _stat_cell(label, value, color):
    return [
        Paragraph(f"<b>{value}</b>",
                  ParagraphStyle("sv", fontName=FONT_BOLD, fontSize=13, textColor=color, alignment=TA_CENTER)),
        Paragraph(label,
                  ParagraphStyle("sl", fontName=FONT_NAME, fontSize=8, textColor=C_GRAY, alignment=TA_CENTER)),
    ]


def export_single_pdf(inf: dict, manual: dict) -> bytes:
    """인플루언서 1명 PDF"""
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=landscape(A4),
                             leftMargin=1.5*cm, rightMargin=1.5*cm,
                             topMargin=1.5*cm, bottomMargin=1.5*cm)
    elements = _scorecard_elements(inf, manual)
    doc.build(elements)
    return buf.getvalue()


def export_multi_pdf(inf_list: list) -> bytes:
    """여러 인플루언서 스코어카드 (1명 1페이지)"""
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=landscape(A4),
                             leftMargin=1.5*cm, rightMargin=1.5*cm,
                             topMargin=1.5*cm, bottomMargin=1.5*cm)
    elements = []
    for i, (inf, manual) in enumerate(inf_list):
        if i > 0:
            elements.append(PageBreak())
        elements += _scorecard_elements(inf, manual)
    doc.build(elements)
    return buf.getvalue()


def export_list_pdf(inf_list: list) -> bytes:
    """비교 리스트형 PDF (표 형태, 여러 명 한눈에)"""
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=landscape(A4),
                             leftMargin=1*cm, rightMargin=1*cm,
                             topMargin=1.5*cm, bottomMargin=1*cm)
    elements = []

    title_style = ParagraphStyle("title", fontName=FONT_BOLD, fontSize=16, textColor=colors.HexColor("#0f172a"),
                                  spaceAfter=4)
    sub_style = ParagraphStyle("sub", fontName=FONT_NAME, fontSize=9, textColor=colors.HexColor("#64748b"),
                                spaceAfter=12)
    elements.append(Paragraph("인플루언서 비교 리스트", title_style))
    elements.append(Paragraph(f"총 {len(inf_list)}명 | 생성일: {datetime.now().strftime('%Y-%m-%d %H:%M')}", sub_style))

    headers = ["#", "계정", "팔로워", "참여율", "평균릴스뷰", "게시물", "릴스%", "카테고리",
               "라이브", "피드단가", "릴스단가", "협업유형", "품질"]
    data = [headers]

    for i, (inf, manual) in enumerate(inf_list):
        uname = inf.get("username", "")
        row = [
            str(i+1),
            f"@{uname}",
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
        data.append(row)

    col_widths = [1*cm, 4*cm, 2.5*cm, 2*cm, 2.5*cm, 1.8*cm, 1.8*cm,
                  3*cm, 1.5*cm, 2*cm, 2*cm, 3*cm, 1.5*cm]
    tbl = Table(data, colWidths=col_widths, repeatRows=1)
    tbl.setStyle(TableStyle([
        ("BACKGROUND",  (0,0), (-1,0), C_PURPLE),
        ("TEXTCOLOR",   (0,0), (-1,0), colors.white),
        ("FONTNAME",    (0,0), (-1,0), FONT_BOLD),
        ("FONTSIZE",    (0,0), (-1,-1), 8),
        ("FONTNAME",    (0,1), (-1,-1), FONT_NAME),
        ("ROWBACKGROUNDS", (0,1), (-1,-1), [colors.white, C_LIGHTGRAY]),
        ("GRID",        (0,0), (-1,-1), 0.3, colors.HexColor("#e2e8f0")),
        ("ALIGN",       (0,0), (-1,-1), "CENTER"),
        ("ALIGN",       (1,1), (1,-1), "LEFT"),
        ("ALIGN",       (7,1), (7,-1), "LEFT"),
        ("PADDING",     (0,0), (-1,-1), 4),
        ("VALIGN",      (0,0), (-1,-1), "MIDDLE"),
    ]))
    elements.append(tbl)
    doc.build(elements)
    return buf.getvalue()
