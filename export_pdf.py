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

# ── 색상 팔레트 (모던 대시보드) ──
C_PRIMARY   = colors.HexColor("#1e3a8a")   # 딥 블루
C_ACCENT    = colors.HexColor("#3b82f6")   # 액센트 블루
C_PURPLE    = colors.HexColor("#6366f1")   # 브랜드 퍼플
C_DARK      = colors.HexColor("#0f172a")   # 텍스트
C_TEXT      = colors.HexColor("#334155")   # 본문
C_GRAY      = colors.HexColor("#64748b")   # 보조 텍스트
C_LIGHT     = colors.HexColor("#f1f5f9")   # 밝은 배경
C_LIGHTER   = colors.HexColor("#f8fafc")   # 더 밝은 배경
C_BORDER    = colors.HexColor("#e2e8f0")   # 테두리
C_GREEN     = colors.HexColor("#059669")   # 좋음
C_ORANGE    = colors.HexColor("#d97706")   # 주의
C_RED       = colors.HexColor("#dc2626")   # 낮음
C_WHITE     = colors.white


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
    """프로필 이미지: Supabase Storage -> CDN -> 로컬"""
    import requests as _req
    pk = inf.get("pk", "") if isinstance(inf, dict) else ""
    username = inf.get("username", "") if isinstance(inf, dict) else str(inf)
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
    """참여율에 따른 색상"""
    try:
        v = float(er or 0)
        if v >= 5: return C_GREEN
        if v >= 2: return C_ACCENT
        return C_ORANGE
    except: return C_GRAY


# ═══════════════════════════════════════════════════════════
# 스코어카드 (1인 상세 PDF)
# ═══════════════════════════════════════════════════════════

def _scorecard_elements(inf, manual, posts_summary=None):
    elements = []
    W = 25.7 * cm  # landscape A4 content width

    username = inf.get("username", "")
    full_name = inf.get("full_name", "")

    # ════════ 1. 헤더 바 (딥블루 그라데이션 효과) ════════
    pic = _get_profile_img(inf, (48, 48))
    pic_cell = pic if pic else _p(username[0].upper() if username else "?",
                                   size=22, bold=True, color=C_WHITE, align=TA_CENTER)

    # 이름 + 배지 영역
    name_block = []
    name_block.append(_p(f"@{username}", size=16, bold=True, color=C_WHITE))
    sub_parts = []
    if full_name: sub_parts.append(full_name)
    if inf.get("category") or manual.get("main_category"):
        sub_parts.append(inf.get("category") or manual.get("main_category"))
    if sub_parts:
        name_block.append(_p(" · ".join(sub_parts), size=9, color=colors.HexColor("#93c5fd")))

    badges = []
    if inf.get("is_verified"): badges.append("✓ 인증")
    if inf.get("is_business"): badges.append("비즈니스")
    if manual.get("can_live"): badges.append("라이브")
    if badges:
        name_block.append(_p("  ".join(badges), size=8, bold=True, color=colors.HexColor("#fbbf24")))

    # 오른쪽: 날짜 + 브랜드
    right_block = [
        _p("SUPERTAG", size=11, bold=True, color=C_WHITE, align=TA_RIGHT),
        _p("Influencer Scorecard", size=7, color=colors.HexColor("#93c5fd"), align=TA_RIGHT),
        _p(datetime.now().strftime("%Y.%m.%d"), size=8, color=colors.HexColor("#93c5fd"), align=TA_RIGHT),
    ]

    header = Table([[pic_cell, name_block, right_block]], colWidths=[60, W - 60 - 7*cm, 7*cm])
    header.setStyle(TableStyle([
        ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
        ("BACKGROUND", (0,0), (-1,-1), C_PRIMARY),
        ("LEFTPADDING", (0,0), (0,0), 10),
        ("LEFTPADDING", (1,0), (1,0), 12),
        ("RIGHTPADDING", (-1,0), (-1,0), 12),
        ("TOPPADDING", (0,0), (-1,-1), 10),
        ("BOTTOMPADDING", (0,0), (-1,-1), 10),
    ]))
    elements.append(header)
    elements.append(Spacer(1, 2))

    # ════════ 2. 핵심 지표 카드 (6칸) ════════
    def _metric_cell(label, value, val_color=C_PRIMARY):
        return [
            _p(value, size=14, bold=True, color=val_color, align=TA_CENTER),
            _p(label, size=7, color=C_GRAY, align=TA_CENTER),
        ]

    er = inf.get("engagement_rate", 0)
    metrics_row = [
        _metric_cell("팔로워", _fmt(inf.get("follower_count")), C_PRIMARY),
        _metric_cell("팔로잉", _fmt(inf.get("following_count")), C_TEXT),
        _metric_cell("총 게시물", _fmt(inf.get("media_count")), C_TEXT),
        _metric_cell("참여율", _fmt_rate(er), _er_color(er)),
        _metric_cell("릴스 비율", _fmt_rate(inf.get("reels_ratio")), C_ACCENT),
        _metric_cell("협찬 비율", _fmt_rate(inf.get("sponsored_ratio")), C_ORANGE),
    ]
    mt = Table([metrics_row], colWidths=[W/6]*6)
    mt.setStyle(TableStyle([
        ("ALIGN",(0,0),(-1,-1),"CENTER"), ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
        ("BACKGROUND",(0,0),(-1,-1), C_WHITE),
        ("BOX",(0,0),(-1,-1), 0.5, C_BORDER),
        ("LINEAFTER",(0,0),(-2,-1), 0.5, C_BORDER),
        ("TOPPADDING",(0,0),(-1,-1), 8),
        ("BOTTOMPADDING",(0,0),(-1,-1), 8),
    ]))
    elements.append(mt)
    elements.append(Spacer(1, 8))

    # ════════ 3. 피드/릴스 성과 비교 (컬러 테이블) ════════
    elements.append(_p("피드 / 릴스 성과 비교", size=11, bold=True, color=C_PRIMARY))
    elements.append(Spacer(1, 3))

    hdr = [_p("", size=8), _p("평균 좋아요", size=8, bold=True, color=C_GRAY, align=TA_CENTER),
           _p("평균 댓글", size=8, bold=True, color=C_GRAY, align=TA_CENTER),
           _p("평균 조회수", size=8, bold=True, color=C_GRAY, align=TA_CENTER)]
    feed = [
        _p("피드 (Feed)", size=9, bold=True, color=C_ACCENT),
        _p(_fmt(inf.get("avg_feed_likes")), size=10, bold=True, color=C_DARK, align=TA_CENTER),
        _p(_fmt(inf.get("avg_feed_comments")), size=10, bold=True, color=C_DARK, align=TA_CENTER),
        _p("-", size=9, color=C_GRAY, align=TA_CENTER),
    ]
    reel = [
        _p("릴스 (Reels)", size=9, bold=True, color=C_PURPLE),
        _p(_fmt(inf.get("avg_reel_likes")), size=10, bold=True, color=C_DARK, align=TA_CENTER),
        _p(_fmt(inf.get("avg_reel_comments")), size=10, bold=True, color=C_DARK, align=TA_CENTER),
        _p(_fmt(inf.get("avg_reel_views")), size=10, bold=True, color=C_PURPLE, align=TA_CENTER),
    ]
    cw = [4*cm, 6*cm, 6*cm, 6*cm]
    pt = Table([hdr, feed, reel], colWidths=cw)
    pt.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,0), C_LIGHT),
        ("BACKGROUND",(0,1),(-1,1), colors.HexColor("#eff6ff")),  # 피드: 연한 파랑
        ("BACKGROUND",(0,2),(-1,2), colors.HexColor("#f5f3ff")),  # 릴스: 연한 보라
        ("BOX",(0,0),(-1,-1), 0.5, C_BORDER),
        ("LINEBELOW",(0,0),(-1,0), 0.5, C_BORDER),
        ("LINEBELOW",(0,1),(-1,1), 0.5, C_BORDER),
        ("PADDING",(0,0),(-1,-1), 7),
        ("ALIGN",(1,0),(-1,-1),"CENTER"), ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
    ]))
    elements.append(pt)
    elements.append(Spacer(1, 8))

    # ════════ 4. 계정 정보 + 단가 (2단 레이아웃) ════════
    elements.append(_p("계정 정보", size=11, bold=True, color=C_PRIMARY))
    elements.append(Spacer(1, 3))

    def _info_table(rows, cw_l=3.5*cm, cw_r=8.5*cm):
        data = []
        for label, val in rows:
            data.append([_p(label, size=7.5, bold=True, color=C_GRAY),
                         _p(str(val), size=7.5, color=C_DARK)])
        t = Table(data, colWidths=[cw_l, cw_r])
        t.setStyle(TableStyle([
            ("ROWBACKGROUNDS",(0,0),(-1,-1), [C_WHITE, C_LIGHTER]),
            ("BOX",(0,0),(-1,-1), 0.5, C_BORDER),
            ("LINEBELOW",(0,0),(-1,-2), 0.3, C_BORDER),
            ("PADDING",(0,0),(-1,-1), 5),
            ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
        ]))
        return t

    left_rows = [
        ("업로드 빈도", inf.get("upload_frequency") or "-"),
        ("활성 시간", inf.get("active_hours") or "-"),
        ("마지막 게시", inf.get("last_post_date") or "-"),
        ("카테고리", inf.get("category") or manual.get("main_category") or "-"),
        ("바이오", (inf.get("biography") or "-")[:80]),
        ("프로필 링크", (inf.get("external_url") or "-")[:60]),
    ]
    right_rows = [
        ("담당자", manual.get("contact_name") or "-"),
        ("카카오", manual.get("contact_kakao") or "-"),
        ("이메일", manual.get("contact_email") or inf.get("public_email") or "-"),
        ("피드 단가", f"{manual.get('feed_price') or 0:,}만원" if manual.get("feed_price") else "-"),
        ("릴스 단가", f"{manual.get('reel_price') or 0:,}만원" if manual.get("reel_price") else "-"),
        ("협업 유형", manual.get("collab_types") or "-"),
    ]

    detail = Table([[_info_table(left_rows), _info_table(right_rows)]],
                   colWidths=[12.5*cm, 12.5*cm], spaceAfter=0)
    detail.setStyle(TableStyle([
        ("VALIGN",(0,0),(-1,-1),"TOP"),
        ("LEFTPADDING",(0,0),(0,0),0), ("RIGHTPADDING",(0,0),(0,0),4),
        ("LEFTPADDING",(1,0),(1,0),4), ("RIGHTPADDING",(1,0),(1,0),0),
        ("TOPPADDING",(0,0),(-1,-1),0), ("BOTTOMPADDING",(0,0),(-1,-1),0),
    ]))
    elements.append(detail)
    elements.append(Spacer(1, 8))

    # ════════ 5. 인기 게시물 TOP ════════
    top_likes = inf.get("top_posts_likes", [])
    top_reels = inf.get("top_reels_views", [])
    if top_likes or top_reels:
        elements.append(_p("인기 게시물 TOP", size=11, bold=True, color=C_PRIMARY))
        elements.append(Spacer(1, 3))

        post_hdr = [
            _p("유형", size=7.5, bold=True, color=C_WHITE, align=TA_CENTER),
            _p("게시물 코드", size=7.5, bold=True, color=C_WHITE, align=TA_CENTER),
            _p("좋아요", size=7.5, bold=True, color=C_WHITE, align=TA_CENTER),
            _p("댓글", size=7.5, bold=True, color=C_WHITE, align=TA_CENTER),
            _p("조회수", size=7.5, bold=True, color=C_WHITE, align=TA_CENTER),
        ]
        post_data = [post_hdr]

        for p in (top_likes or [])[:3]:
            if not isinstance(p, dict): continue
            url = p.get("url", "")
            code = url.split("/p/")[-1].rstrip("/")[:15] if "/p/" in url else url[-20:]
            post_data.append([
                _p("피드", size=8, bold=True, color=C_ACCENT, align=TA_CENTER),
                _p(code, size=8, color=C_TEXT, align=TA_CENTER),
                _p(_fmt(p.get("likes", 0)), size=8, color=C_DARK, align=TA_CENTER),
                _p(_fmt(p.get("comments", 0)), size=8, color=C_DARK, align=TA_CENTER),
                _p("-", size=8, color=C_GRAY, align=TA_CENTER),
            ])

        for p in (top_reels or [])[:3]:
            if not isinstance(p, dict): continue
            url = p.get("url", "")
            code = url.split("/p/")[-1].rstrip("/")[:15] if "/p/" in url else url[-20:]
            post_data.append([
                _p("릴스", size=8, bold=True, color=C_PURPLE, align=TA_CENTER),
                _p(code, size=8, color=C_TEXT, align=TA_CENTER),
                _p(_fmt(p.get("likes", 0)), size=8, color=C_DARK, align=TA_CENTER),
                _p("-", size=8, color=C_GRAY, align=TA_CENTER),
                _p(_fmt(p.get("views", 0)), size=8, bold=True, color=C_PURPLE, align=TA_CENTER),
            ])

        if len(post_data) > 1:
            pcw = [2.5*cm, 6*cm, 4*cm, 4*cm, 4*cm]
            ptbl = Table(post_data, colWidths=pcw)
            ptbl.setStyle(TableStyle([
                ("BACKGROUND",(0,0),(-1,0), C_PRIMARY),
                ("ROWBACKGROUNDS",(0,1),(-1,-1), [C_WHITE, C_LIGHTER]),
                ("BOX",(0,0),(-1,-1), 0.5, C_BORDER),
                ("LINEBELOW",(0,0),(-1,-1), 0.3, C_BORDER),
                ("PADDING",(0,0),(-1,-1), 5),
                ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
            ]))
            elements.append(ptbl)
            elements.append(Spacer(1, 8))

    # ════════ 6. 해시태그 ════════
    htags_raw = inf.get("top_hashtags", "[]")
    try:
        htags = json.loads(htags_raw) if isinstance(htags_raw, str) else (htags_raw or [])
    except: htags = []
    if htags:
        elements.append(_p("자주 사용하는 해시태그", size=11, bold=True, color=C_PRIMARY))
        elements.append(Spacer(1, 3))
        tags = []
        for h in htags[:20]:
            if isinstance(h, dict):
                tags.append(f'<font color="#6366f1">#{h["tag"]}</font>({h.get("count",1)})')
            elif isinstance(h, str):
                tags.append(f'<font color="#6366f1">#{h}</font>')
        tag_text = "  &nbsp;&nbsp;  ".join(tags)
        elements.append(_p(tag_text, size=8, color=C_TEXT))
        elements.append(Spacer(1, 6))

    # ════════ 7. 메모 / 협업 브랜드 ════════
    notes = manual.get("notes", "")
    brands = manual.get("past_brands", "")
    if notes or brands:
        elements.append(_p("메모 / 협업 이력", size=11, bold=True, color=C_PRIMARY))
        elements.append(Spacer(1, 2))
        if notes:
            elements.append(_p(f"메모: {notes}", size=8, color=C_TEXT))
        if brands:
            elements.append(_p(f"협업 브랜드: {brands}", size=8, color=C_TEXT))
        elements.append(Spacer(1, 4))

    # ════════ 8. 푸터 ════════
    elements.append(Spacer(1, 6))
    elements.append(HRFlowable(width="100%", thickness=1, color=C_PRIMARY))
    elements.append(Spacer(1, 3))
    footer_l = f"SuperTag  |  Confidential  |  {datetime.now().strftime('%Y-%m-%d %H:%M')}  |  v2.0"
    footer_r = f"통계 갱신: {inf.get('stats_updated_at', inf.get('last_post_date', '-'))}"
    ft = Table([
        [_p(footer_l, size=7, color=C_GRAY), _p(footer_r, size=7, color=C_GRAY, align=TA_RIGHT)]
    ], colWidths=[W/2, W/2])
    ft.setStyle(TableStyle([("PADDING",(0,0),(-1,-1),0)]))
    elements.append(ft)

    return elements


def export_single_pdf(inf: dict, manual: dict) -> bytes:
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=landscape(A4),
                             leftMargin=1.2*cm, rightMargin=1.2*cm,
                             topMargin=1*cm, bottomMargin=0.8*cm)
    doc.build(_scorecard_elements(inf, manual))
    return buf.getvalue()


def export_multi_pdf(inf_list: list) -> bytes:
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=landscape(A4),
                             leftMargin=1.2*cm, rightMargin=1.2*cm,
                             topMargin=1*cm, bottomMargin=0.8*cm)
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

    # 푸터
    elements.append(Spacer(1, 8))
    elements.append(HRFlowable(width="100%", thickness=1, color=C_PRIMARY))
    elements.append(_p(f"SuperTag  |  Confidential  |  {datetime.now().strftime('%Y-%m-%d %H:%M')}",
                       size=7, color=C_GRAY, align=TA_RIGHT))

    doc.build(elements)
    return buf.getvalue()
