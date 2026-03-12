"""PDF 생성 모듈 — 인플루언서 프리미엄 스코어카드 & 리스트"""
import io, json, os
from datetime import datetime
from reportlab.lib.pagesizes import A4, landscape
from reportlab.lib import colors
from reportlab.lib.units import cm, mm
from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer, Table,
                                 TableStyle, Image as RLImage, PageBreak,
                                 HRFlowable, Flowable)
from reportlab.lib.styles import ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.graphics.shapes import Drawing, Rect, String, Circle
from PIL import Image as PILImage

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")

# ── 폰트 등록 ──
FONT_NAME = "Helvetica"
FONT_BOLD = "Helvetica-Bold"
_FONT_DIR = os.path.join(os.path.dirname(__file__), "fonts")
try:
    _ng = os.path.join(_FONT_DIR, "NanumGothic.ttf")
    _ngb = os.path.join(_FONT_DIR, "NanumGothicBold.ttf")
    if not os.path.exists(_ng):
        _ng = "/usr/share/fonts/truetype/nanum/NanumGothic.ttf"
        _ngb = "/usr/share/fonts/truetype/nanum/NanumGothicBold.ttf"
    if os.path.exists(_ng):
        pdfmetrics.registerFont(TTFont('NanumGothic', _ng))
        pdfmetrics.registerFont(TTFont('NanumGothicBold', _ngb if os.path.exists(_ngb) else _ng))
        FONT_NAME = 'NanumGothic'
        FONT_BOLD = 'NanumGothicBold'
except Exception:
    pass

# ── 색상 팔레트 ──
C_PRIMARY   = colors.HexColor("#1e3a8a")
C_ACCENT    = colors.HexColor("#3b82f6")
C_PURPLE    = colors.HexColor("#7c3aed")
C_DARK      = colors.HexColor("#0f172a")
C_TEXT      = colors.HexColor("#334155")
C_GRAY      = colors.HexColor("#64748b")
C_LIGHT     = colors.HexColor("#f1f5f9")
C_LIGHTER   = colors.HexColor("#f8fafc")
C_BORDER    = colors.HexColor("#e2e8f0")
C_GREEN     = colors.HexColor("#059669")
C_ORANGE    = colors.HexColor("#d97706")
C_RED       = colors.HexColor("#dc2626")
C_WHITE     = colors.white
C_BLUE_BG   = colors.HexColor("#eff6ff")
C_PURP_BG   = colors.HexColor("#f5f3ff")
C_CARD_BG   = colors.HexColor("#f8fafc")


# ── 커스텀 Flowable: 둥근 박스 ──
class RoundedBox(Flowable):
    """둥근 모서리 배경 박스"""
    def __init__(self, width, height, bg_color=C_CARD_BG, border_color=C_BORDER,
                 radius=6, border_width=0.5):
        super().__init__()
        self.width = width
        self.height = height
        self.bg_color = bg_color
        self.border_color = border_color
        self.radius = radius
        self.border_width = border_width

    def draw(self):
        self.canv.setFillColor(self.bg_color)
        self.canv.setStrokeColor(self.border_color)
        self.canv.setLineWidth(self.border_width)
        self.canv.roundRect(0, 0, self.width, self.height, self.radius, fill=1, stroke=1)


class SectionHeader(Flowable):
    """섹션 헤더: 왼쪽 액센트 바 + 타이틀"""
    def __init__(self, text, width, bar_color=C_PRIMARY, font_size=10):
        super().__init__()
        self.text = text
        self.width = width
        self.bar_color = bar_color
        self.font_size = font_size
        self.height = font_size + 8

    def draw(self):
        c = self.canv
        c.setFillColor(self.bar_color)
        c.roundRect(0, 0, 4, self.height, 2, fill=1, stroke=0)
        c.setFillColor(C_DARK)
        c.setFont(FONT_BOLD, self.font_size)
        c.drawString(10, 4, self.text)


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

def _p(text, size=8, color=C_DARK, bold=False, align=TA_LEFT, leading=None):
    fn = FONT_BOLD if bold else FONT_NAME
    ld = leading or size * 1.5
    return Paragraph(str(text), ParagraphStyle("_", fontName=fn, fontSize=size,
                                                textColor=color, alignment=align, leading=ld))

def _get_profile_img(inf, size=(50, 50)):
    import requests as _req
    pk = inf.get("pk", "") if isinstance(inf, dict) else ""
    urls = []
    if pk:
        sb_url = os.environ.get("SUPABASE_URL", "https://ysqnixgdpltguatvjjcb.supabase.co")
        urls.append(f"{sb_url}/storage/v1/object/public/profile-pics/{pk}.jpg")
    local_val = (inf.get("profile_pic_local", "") or "") if isinstance(inf, dict) else ""
    if local_val.startswith("http"):
        urls.append(local_val)
    cdn = (inf.get("profile_pic_url", "") or "") if isinstance(inf, dict) else ""
    if cdn:
        urls.append(cdn)
    for url in urls:
        try:
            r = _req.get(url, timeout=5)
            if r.status_code == 200 and len(r.content) > 500:
                img = PILImage.open(io.BytesIO(r.content)).convert("RGB")
                img.thumbnail(size, PILImage.LANCZOS)
                buf = io.BytesIO()
                img.save(buf, format="JPEG")
                buf.seek(0)
                return RLImage(buf, width=size[0], height=size[1])
        except:
            continue
    return None

def _er_color(er):
    try:
        v = float(er or 0)
        if v >= 5: return C_GREEN
        if v >= 2: return C_ACCENT
        return C_ORANGE
    except: return C_GRAY

def _er_label(er):
    try:
        v = float(er or 0)
        if v >= 5: return "Excellent"
        if v >= 3: return "Good"
        if v >= 1: return "Average"
        return "Low"
    except: return "-"

def _ts_to_date(v):
    if not v or v == "-":
        return "-"
    try:
        fv = float(v)
        if fv > 1_000_000_000:
            return datetime.fromtimestamp(fv).strftime("%Y-%m-%d")
    except (ValueError, TypeError, OSError):
        pass
    s = str(v)
    if len(s) >= 10 and s[:4].isdigit():
        return s[:10]
    return s


# ═══════════════════════════════════════════════════════════
# 스코어카드 (1인 상세 PDF)
# ═══════════════════════════════════════════════════════════

def _scorecard_elements(inf, manual, posts_summary=None):
    elements = []
    W = 25.7 * cm

    username = inf.get("username", "")
    full_name = inf.get("full_name", "")
    er = inf.get("engagement_rate", 0)
    cat = inf.get("category") or manual.get("main_category") or ""

    # ════════ 1. 헤더 (풀폭 딥블루) ════════
    pic = _get_profile_img(inf, (52, 52))
    pic_cell = pic if pic else _p(username[0].upper() if username else "?",
                                   size=24, bold=True, color=C_WHITE, align=TA_CENTER)

    name_parts = [_p(f"@{username}", size=18, bold=True, color=C_WHITE)]
    sub = full_name
    if cat: sub += f"  ·  {cat}" if sub else cat
    badges = []
    if inf.get("is_verified"): badges.append("Verified")
    if inf.get("is_business"): badges.append("Business")
    if manual.get("can_live"): badges.append("Live")
    if badges: sub += ("  |  " if sub else "") + " · ".join(badges)
    if sub:
        name_parts.append(_p(sub, size=8, color=colors.HexColor("#bfdbfe")))

    right_parts = [
        _p("SUPERTAG", size=14, bold=True, color=C_WHITE, align=TA_RIGHT),
        _p("Influencer Scorecard", size=7, color=colors.HexColor("#93c5fd"), align=TA_RIGHT),
        _p(datetime.now().strftime("%Y.%m.%d"), size=7, color=colors.HexColor("#93c5fd"), align=TA_RIGHT),
    ]

    hdr = Table([[pic_cell, name_parts, right_parts]], colWidths=[64, W - 64 - 6*cm, 6*cm])
    hdr.setStyle(TableStyle([
        ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
        ("BACKGROUND", (0,0), (-1,-1), C_PRIMARY),
        ("LEFTPADDING", (0,0), (0,0), 12), ("LEFTPADDING", (1,0), (1,0), 14),
        ("RIGHTPADDING", (-1,0), (-1,0), 14),
        ("TOPPADDING", (0,0), (-1,-1), 12), ("BOTTOMPADDING", (0,0), (-1,-1), 12),
    ]))
    elements.append(hdr)
    elements.append(Spacer(1, 6))

    # ════════ 2. 핵심 지표 (6 카드 — 배경색 있는 개별 카드) ════════
    def _card(label, value, vc=C_PRIMARY, bg=C_LIGHTER):
        return Table(
            [[_p(value, size=16, bold=True, color=vc, align=TA_CENTER)],
             [_p(label, size=7, color=C_GRAY, align=TA_CENTER)]],
            colWidths=[W/6 - 4],
        )

    cards_data = [
        ("팔로워", _fmt(inf.get("follower_count")), C_PRIMARY, C_BLUE_BG),
        ("팔로잉", _fmt(inf.get("following_count")), C_TEXT, C_LIGHTER),
        ("총 게시물", _fmt(inf.get("media_count")), C_TEXT, C_LIGHTER),
        ("참여율", _fmt_rate(er), _er_color(er), colors.HexColor("#f0fdf4") if float(er or 0) >= 2 else colors.HexColor("#fffbeb")),
        ("릴스 비율", _fmt_rate(inf.get("reels_ratio")), C_PURPLE, C_PURP_BG),
        ("협찬 비율", _fmt_rate(inf.get("sponsored_ratio")), C_ORANGE, C_LIGHTER),
    ]
    row = [_card(l, v, vc, bg) for l, v, vc, bg in cards_data]
    cw = W / 6
    mt = Table([row], colWidths=[cw]*6)
    mt.setStyle(TableStyle([
        ("ALIGN",(0,0),(-1,-1),"CENTER"), ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
        ("BACKGROUND",(0,0),(0,0), cards_data[0][3]),
        ("BACKGROUND",(1,0),(1,0), cards_data[1][3]),
        ("BACKGROUND",(2,0),(2,0), cards_data[2][3]),
        ("BACKGROUND",(3,0),(3,0), cards_data[3][3]),
        ("BACKGROUND",(4,0),(4,0), cards_data[4][3]),
        ("BACKGROUND",(5,0),(5,0), cards_data[5][3]),
        ("BOX",(0,0),(-1,-1), 0.5, C_BORDER),
        ("LINEAFTER",(0,0),(-2,-1), 0.3, C_BORDER),
        ("TOPPADDING",(0,0),(-1,-1), 8), ("BOTTOMPADDING",(0,0),(-1,-1), 8),
    ]))
    elements.append(mt)
    elements.append(Spacer(1, 8))

    # ════════ 3. 2단 레이아웃: 왼쪽(피드릴스 + 인기게시물) | 오른쪽(계정정보) ════════
    L = []
    R = []
    LW = 13.2 * cm
    RW = W - LW - 0.3*cm

    # --- 왼쪽: 피드/릴스 성과 비교 ---
    L.append(SectionHeader("피드 / 릴스 성과 비교", LW, C_ACCENT))
    L.append(Spacer(1, 4))

    compare_hdr = [
        _p("", size=7),
        _p("평균 좋아요", size=7.5, bold=True, color=C_WHITE, align=TA_CENTER),
        _p("평균 댓글", size=7.5, bold=True, color=C_WHITE, align=TA_CENTER),
        _p("평균 조회수", size=7.5, bold=True, color=C_WHITE, align=TA_CENTER),
    ]
    compare_feed = [
        _p("  피드 (Feed)", size=9, bold=True, color=C_ACCENT),
        _p(_fmt(inf.get("avg_feed_likes")), size=11, bold=True, color=C_DARK, align=TA_CENTER),
        _p(_fmt(inf.get("avg_feed_comments")), size=11, color=C_DARK, align=TA_CENTER),
        _p("-", size=9, color=C_GRAY, align=TA_CENTER),
    ]
    compare_reel = [
        _p("  릴스 (Reels)", size=9, bold=True, color=C_PURPLE),
        _p(_fmt(inf.get("avg_reel_likes")), size=11, bold=True, color=C_DARK, align=TA_CENTER),
        _p(_fmt(inf.get("avg_reel_comments")), size=11, color=C_DARK, align=TA_CENTER),
        _p(_fmt(inf.get("avg_reel_views")), size=11, bold=True, color=C_PURPLE, align=TA_CENTER),
    ]
    tcw = [3.2*cm, 3.3*cm, 3.3*cm, 3.3*cm]
    ct = Table([compare_hdr, compare_feed, compare_reel], colWidths=tcw)
    ct.setStyle(TableStyle([
        ("BACKGROUND",(1,0),(3,0), C_PRIMARY),
        ("BACKGROUND",(0,0),(0,0), C_PRIMARY),
        ("BACKGROUND",(0,1),(-1,1), C_BLUE_BG),
        ("BACKGROUND",(0,2),(-1,2), C_PURP_BG),
        ("BOX",(0,0),(-1,-1), 0.5, C_BORDER),
        ("LINEBELOW",(0,0),(-1,1), 0.5, C_BORDER),
        ("PADDING",(0,0),(-1,-1), 6),
        ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
        ("ROUNDEDCORNERS", [4,4,4,4]),
    ]))
    L.append(ct)
    L.append(Spacer(1, 8))

    # --- 왼쪽: 인기 게시물 TOP ---
    top_likes = inf.get("top_posts_likes", [])
    top_reels = inf.get("top_reels_views", [])
    if top_likes or top_reels:
        L.append(SectionHeader("인기 게시물 TOP", LW, C_PURPLE))
        L.append(Spacer(1, 4))
        ph = [_p("유형", size=7, bold=True, color=C_WHITE, align=TA_CENTER),
              _p("게시물 코드", size=7, bold=True, color=C_WHITE, align=TA_CENTER),
              _p("좋아요", size=7, bold=True, color=C_WHITE, align=TA_CENTER),
              _p("댓글", size=7, bold=True, color=C_WHITE, align=TA_CENTER),
              _p("조회수", size=7, bold=True, color=C_WHITE, align=TA_CENTER)]
        pd = [ph]
        for p in (top_likes or [])[:3]:
            if not isinstance(p, dict): continue
            url = p.get("url", "")
            code = url.split("/p/")[-1].rstrip("/")[:14] if "/p/" in url else "-"
            pd.append([
                _p("피드", size=7.5, bold=True, color=C_ACCENT, align=TA_CENTER),
                _p(code, size=7.5, color=C_TEXT, align=TA_CENTER),
                _p(_fmt(p.get("likes", 0)), size=7.5, bold=True, color=C_DARK, align=TA_CENTER),
                _p(_fmt(p.get("comments", 0)), size=7.5, color=C_DARK, align=TA_CENTER),
                _p("-", size=7.5, color=C_GRAY, align=TA_CENTER),
            ])
        for p in (top_reels or [])[:3]:
            if not isinstance(p, dict): continue
            url = p.get("url", "")
            code = url.split("/p/")[-1].rstrip("/")[:14] if "/p/" in url else "-"
            pd.append([
                _p("릴스", size=7.5, bold=True, color=C_PURPLE, align=TA_CENTER),
                _p(code, size=7.5, color=C_TEXT, align=TA_CENTER),
                _p(_fmt(p.get("likes", 0)), size=7.5, bold=True, color=C_DARK, align=TA_CENTER),
                _p("-", size=7.5, color=C_GRAY, align=TA_CENTER),
                _p(_fmt(p.get("views", 0)), size=7.5, bold=True, color=C_PURPLE, align=TA_CENTER),
            ])
        if len(pd) > 1:
            pw = [2*cm, 3.8*cm, 2.5*cm, 2.5*cm, 2.5*cm]
            ptbl = Table(pd, colWidths=pw)
            ptbl.setStyle(TableStyle([
                ("BACKGROUND",(0,0),(-1,0), colors.HexColor("#4338ca")),
                ("ROWBACKGROUNDS",(0,1),(-1,-1), [C_WHITE, C_PURP_BG]),
                ("BOX",(0,0),(-1,-1), 0.5, C_BORDER),
                ("LINEBELOW",(0,0),(-1,-1), 0.3, C_BORDER),
                ("PADDING",(0,0),(-1,-1), 5), ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
            ]))
            L.append(ptbl)

    # --- 오른쪽: 계정 정보 ---
    R.append(SectionHeader("계정 정보", RW, C_PRIMARY))
    R.append(Spacer(1, 4))

    def _irow(label, val, hl=False):
        vc = C_PRIMARY if hl else C_DARK
        return [_p(label, size=7.5, bold=True, color=C_GRAY),
                _p(str(val)[:55], size=7.5, color=vc, bold=hl)]

    info_data = [
        _irow("업로드 빈도", inf.get("upload_frequency") or "-"),
        _irow("활성 시간", inf.get("active_hours") or "-"),
        _irow("마지막 게시", inf.get("last_post_date") or "-"),
        _irow("카테고리", cat or "-"),
        _irow("바이오", (inf.get("biography") or "-")[:55]),
        _irow("프로필 링크", (inf.get("external_url") or "-")[:50]),
    ]
    # 구분선 역할의 빈 행
    info_data.append([_p("연락처 / 단가", size=7, bold=True, color=C_PRIMARY), _p("", size=7)])

    m_items = [
        ("담당자", manual.get("contact_name") or "-"),
        ("카카오", manual.get("contact_kakao") or "-"),
        ("이메일", manual.get("contact_email") or inf.get("public_email") or "-"),
        ("피드 단가", f"{manual.get('feed_price'):,}만원" if manual.get("feed_price") else "-"),
        ("릴스 단가", f"{manual.get('reel_price'):,}만원" if manual.get("reel_price") else "-"),
        ("협업 유형", manual.get("collab_types") or "-"),
    ]
    for label, val in m_items:
        info_data.append(_irow(label, val))

    it = Table(info_data, colWidths=[3*cm, RW - 3*cm - 0.6*cm])
    styles = [
        ("ROWBACKGROUNDS",(0,0),(-1,-1), [C_WHITE, C_CARD_BG]),
        ("BOX",(0,0),(-1,-1), 0.5, C_BORDER),
        ("LINEBELOW",(0,0),(-1,-2), 0.3, C_BORDER),
        ("PADDING",(0,0),(-1,-1), 4.5), ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
        # 구분선 행 강조
        ("BACKGROUND",(0,6),(-1,6), colors.HexColor("#dbeafe")),
        ("LINEBELOW",(0,6),(-1,6), 0.8, C_ACCENT),
    ]
    it.setStyle(TableStyle(styles))
    R.append(it)

    # 2단 결합
    layout = Table([[L, R]], colWidths=[LW, RW])
    layout.setStyle(TableStyle([
        ("VALIGN",(0,0),(-1,-1),"TOP"),
        ("LEFTPADDING",(0,0),(0,0),0), ("RIGHTPADDING",(0,0),(0,0),6),
        ("LEFTPADDING",(1,0),(1,0),6), ("RIGHTPADDING",(1,0),(1,0),0),
        ("TOPPADDING",(0,0),(-1,-1),0), ("BOTTOMPADDING",(0,0),(-1,-1),0),
    ]))
    elements.append(layout)
    elements.append(Spacer(1, 6))

    # ════════ 4. 해시태그 (태그 스타일) ════════
    htags_raw = inf.get("top_hashtags", "[]")
    try:
        htags = json.loads(htags_raw) if isinstance(htags_raw, str) else (htags_raw or [])
    except: htags = []
    if htags:
        tags = []
        for h in htags[:15]:
            if isinstance(h, dict):
                tags.append(f'<font color="#7c3aed"><b>#{h["tag"]}</b></font><font color="#94a3b8">({h.get("count",1)})</font>')
            elif isinstance(h, str):
                tags.append(f'<font color="#7c3aed"><b>#{h}</b></font>')
        elements.append(_p('<font color="#1e3a8a"><b>Top 해시태그</b></font>    ' + "    ".join(tags),
                           size=7.5, color=C_TEXT))
        elements.append(Spacer(1, 3))

    # ════════ 5. 메모 ════════
    notes = manual.get("notes", "")
    brands = manual.get("past_brands", "")
    if notes or brands:
        parts = []
        if notes: parts.append(f"<b>메모:</b> {notes}")
        if brands: parts.append(f"<b>협업 브랜드:</b> {brands}")
        elements.append(_p("    ".join(parts), size=7.5, color=C_TEXT))
        elements.append(Spacer(1, 3))

    # ════════ 6. 푸터 ════════
    elements.append(HRFlowable(width="100%", thickness=1.2, color=C_PRIMARY))
    elements.append(Spacer(1, 3))
    updated = _ts_to_date(inf.get("stats_updated_at") or inf.get("last_post_date") or "-")
    ft = Table([
        [_p(f"SuperTag  |  Confidential  |  {datetime.now().strftime('%Y-%m-%d %H:%M')}",
            size=7, color=C_GRAY),
         _p(f"통계 갱신: {updated}", size=7, color=C_GRAY, align=TA_RIGHT)]
    ], colWidths=[W/2, W/2])
    ft.setStyle(TableStyle([("PADDING",(0,0),(-1,-1),0)]))
    elements.append(ft)

    return elements


def export_single_pdf(inf: dict, manual: dict) -> bytes:
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=landscape(A4),
                             leftMargin=1.2*cm, rightMargin=1.2*cm,
                             topMargin=0.8*cm, bottomMargin=0.6*cm)
    doc.build(_scorecard_elements(inf, manual))
    return buf.getvalue()


def export_multi_pdf(inf_list: list) -> bytes:
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=landscape(A4),
                             leftMargin=1.2*cm, rightMargin=1.2*cm,
                             topMargin=0.8*cm, bottomMargin=0.6*cm)
    elements = []
    for i, (inf, manual) in enumerate(inf_list):
        if i > 0:
            elements.append(PageBreak())
        elements += _scorecard_elements(inf, manual)
    doc.build(elements)
    return buf.getvalue()


def export_list_pdf(inf_list: list) -> bytes:
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=landscape(A4),
                             leftMargin=0.8*cm, rightMargin=0.8*cm,
                             topMargin=1*cm, bottomMargin=0.8*cm)
    elements = []

    # 타이틀
    elements.append(_p("인플루언서 비교 리스트", size=16, bold=True, color=C_PRIMARY))
    elements.append(_p(f"총 {len(inf_list)}명  |  {datetime.now().strftime('%Y-%m-%d %H:%M')}  |  SuperTag",
                       size=8, color=C_GRAY))
    elements.append(Spacer(1, 10))

    headers = [
        _p("#", size=7, bold=True, color=C_WHITE, align=TA_CENTER),
        _p("계정", size=7, bold=True, color=C_WHITE),
        _p("팔로워", size=7, bold=True, color=C_WHITE, align=TA_CENTER),
        _p("참여율", size=7, bold=True, color=C_WHITE, align=TA_CENTER),
        _p("피드 평균\n좋아요", size=6.5, bold=True, color=C_WHITE, align=TA_CENTER),
        _p("피드 평균\n댓글", size=6.5, bold=True, color=C_WHITE, align=TA_CENTER),
        _p("릴스 평균\n조회수", size=6.5, bold=True, color=C_WHITE, align=TA_CENTER),
        _p("릴스 평균\n좋아요", size=6.5, bold=True, color=C_WHITE, align=TA_CENTER),
        _p("게시물", size=7, bold=True, color=C_WHITE, align=TA_CENTER),
        _p("카테고리", size=7, bold=True, color=C_WHITE, align=TA_CENTER),
    ]
    data = [headers]

    for i, (inf, manual) in enumerate(inf_list):
        er = inf.get("engagement_rate", 0)
        data.append([
            _p(str(i+1), size=7, color=C_TEXT, align=TA_CENTER),
            _p(f"@{inf.get('username', '')}", size=7, bold=True, color=C_DARK),
            _p(_fmt(inf.get("follower_count")), size=7, color=C_DARK, align=TA_CENTER),
            _p(_fmt_rate(er), size=7, color=_er_color(er), bold=True, align=TA_CENTER),
            _p(_fmt(inf.get("avg_feed_likes")), size=7, color=C_TEXT, align=TA_CENTER),
            _p(_fmt(inf.get("avg_feed_comments")), size=7, color=C_TEXT, align=TA_CENTER),
            _p(_fmt(inf.get("avg_reel_views")), size=7, bold=True, color=C_PURPLE, align=TA_CENTER),
            _p(_fmt(inf.get("avg_reel_likes")), size=7, color=C_TEXT, align=TA_CENTER),
            _p(_fmt(inf.get("media_count")), size=7, color=C_TEXT, align=TA_CENTER),
            _p((inf.get("category") or manual.get("main_category") or "-")[:10], size=7, color=C_TEXT, align=TA_CENTER),
        ])

    cw = [1*cm, 3.5*cm, 2.5*cm, 2*cm, 2.5*cm, 2.5*cm, 2.5*cm, 2.5*cm, 2*cm, 3*cm]
    tbl = Table(data, colWidths=cw, repeatRows=1)
    tbl.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,0), C_PRIMARY),
        ("ROWBACKGROUNDS",(0,1),(-1,-1), [C_WHITE, C_LIGHTER]),
        ("BOX",(0,0),(-1,-1), 0.5, C_BORDER),
        ("LINEBELOW",(0,0),(-1,-1), 0.3, C_BORDER),
        ("PADDING",(0,0),(-1,-1), 4),
        ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
    ]))
    elements.append(tbl)

    elements.append(Spacer(1, 8))
    elements.append(HRFlowable(width="100%", thickness=1, color=C_PRIMARY))
    elements.append(_p(f"SuperTag  |  Confidential  |  {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                       size=7, color=C_GRAY, align=TA_RIGHT))

    doc.build(elements)
    return buf.getvalue()
