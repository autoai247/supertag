"""PDF 생성 모듈 — 인플루언서 스코어카드 & 리스트"""
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

# 폰트 등록 (NanumGothic 사용)
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

C_PURPLE    = colors.HexColor("#6366f1")
C_GRAY      = colors.HexColor("#64748b")
C_LIGHTGRAY = colors.HexColor("#f8fafc")
C_BORDER    = colors.HexColor("#e2e8f0")
C_DARK      = colors.HexColor("#0f172a")
C_GREEN     = colors.HexColor("#10b981")
C_ORANGE    = colors.HexColor("#f59e0b")
C_BLUE      = colors.HexColor("#3b82f6")
C_RED       = colors.HexColor("#ef4444")
C_PINK      = colors.HexColor("#ec4899")

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

def _p(text, font=None, size=8, color=C_DARK, bold=False, align=TA_LEFT):
    fn = (FONT_BOLD if bold else FONT_NAME) if not font else font
    return Paragraph(str(text), ParagraphStyle("_p", fontName=fn, fontSize=size,
                                                textColor=color, alignment=align, leading=size*1.4))

def _get_profile_img(inf, size=(50, 50)):
    """프로필 이미지: Supabase Storage → CDN → 로컬 파일 순서로 시도"""
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
    # 로컬 파일 폴백
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

def _stat_box(label, value, color=C_PURPLE):
    """통계 박스 (값 + 라벨)"""
    return [
        _p(value, bold=True, size=12, color=color, align=TA_CENTER),
        _p(label, size=7, color=C_GRAY, align=TA_CENTER),
    ]

def _section_title(text):
    return _p(text, bold=True, size=10, color=C_PURPLE)


def _scorecard_elements(inf, manual, posts_summary=None):
    """인플루언서 1명 스코어카드"""
    elements = []
    W = 25*cm

    username = inf.get("username", "")
    full_name = inf.get("full_name", "")

    # ════════ 헤더 ════════
    pic = _get_profile_img(inf, (45, 45))
    pic_cell = pic if pic else _p(username[0].upper() if username else "?", bold=True, size=18,
                                   color=colors.white, align=TA_CENTER)
    name_parts = [_p(f"@{username}", bold=True, size=13, color=colors.white)]
    if full_name:
        name_parts.append(_p(full_name, size=8, color=colors.HexColor("#c7d2fe")))
    name_parts.append(_p(f"instagram.com/{username}", size=7, color=colors.HexColor("#a5b4fc")))

    badges = []
    if inf.get("is_verified"): badges.append("인증계정")
    if manual.get("can_live"): badges.append("라이브가능")
    if badges:
        name_parts.append(_p(" | ".join(badges), bold=True, size=7, color=colors.HexColor("#fbbf24")))

    header = Table([[pic_cell, name_parts]], colWidths=[55, W-55])
    header.setStyle(TableStyle([
        ("VALIGN", (0,0), (-1,-1), "MIDDLE"),
        ("BACKGROUND", (0,0), (-1,-1), C_PURPLE),
        ("PADDING", (0,0), (-1,-1), 8),
        ("ROUNDEDCORNERS", [6,6,0,0]),
    ]))
    elements.append(header)

    # ════════ 핵심 지표 6칸 ════════
    row1 = [
        _stat_box("팔로워", _fmt(inf.get("follower_count")), C_PURPLE),
        _stat_box("팔로잉", _fmt(inf.get("following_count")), C_GRAY),
        _stat_box("총 게시물", _fmt(inf.get("media_count")), C_DARK),
        _stat_box("참여율", _fmt_rate(inf.get("engagement_rate")), C_GREEN),
        _stat_box("릴스 비율", _fmt_rate(inf.get("reels_ratio")), C_BLUE),
        _stat_box("협찬 비율", _fmt_rate(inf.get("sponsored_ratio")), C_ORANGE),
    ]
    t = Table([row1], colWidths=[W/6]*6)
    t.setStyle(TableStyle([
        ("ALIGN",(0,0),(-1,-1),"CENTER"), ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
        ("GRID",(0,0),(-1,-1), 0.5, C_BORDER), ("PADDING",(0,0),(-1,-1), 6),
        ("BACKGROUND",(0,0),(-1,-1), colors.white),
    ]))
    elements.append(t)
    elements.append(Spacer(1, 4))

    # ════════ 피드/릴스 성과 비교 ════════
    elements.append(_section_title("피드 / 릴스 성과"))
    perf_headers = ["", "평균 좋아요", "평균 댓글", "평균 조회수"]
    feed_row = [
        _p("피드", bold=True, size=8, color=C_ORANGE),
        _p(_fmt(inf.get("avg_feed_likes")), size=8, align=TA_CENTER),
        _p(_fmt(inf.get("avg_feed_comments")), size=8, align=TA_CENTER),
        _p("-", size=8, color=C_GRAY, align=TA_CENTER),
    ]
    reel_row = [
        _p("릴스", bold=True, size=8, color=C_BLUE),
        _p(_fmt(inf.get("avg_reel_likes")), size=8, align=TA_CENTER),
        _p(_fmt(inf.get("avg_reel_comments")), size=8, align=TA_CENTER),
        _p(_fmt(inf.get("avg_reel_views")), size=8, color=C_BLUE, align=TA_CENTER),
    ]
    perf_t = Table([perf_headers, feed_row, reel_row], colWidths=[3*cm, 5.5*cm, 5.5*cm, 5.5*cm])
    perf_t.setStyle(TableStyle([
        ("FONTNAME",(0,0),(-1,0), FONT_BOLD), ("FONTSIZE",(0,0),(-1,0), 8),
        ("TEXTCOLOR",(0,0),(-1,0), C_GRAY),
        ("BACKGROUND",(0,0),(-1,0), colors.HexColor("#f1f5f9")),
        ("GRID",(0,0),(-1,-1), 0.3, C_BORDER), ("PADDING",(0,0),(-1,-1), 5),
        ("ALIGN",(1,0),(-1,-1),"CENTER"), ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
    ]))
    elements.append(perf_t)
    elements.append(Spacer(1, 6))

    # ════════ 상세 정보 2단 ════════
    elements.append(_section_title("계정 정보 / 단가"))

    def _detail_rows(rows):
        t = Table(rows, colWidths=[3.5*cm, 8*cm])
        t.setStyle(TableStyle([
            ("FONTNAME",(0,0),(0,-1), FONT_BOLD), ("FONTNAME",(1,0),(1,-1), FONT_NAME),
            ("FONTSIZE",(0,0),(-1,-1), 7.5),
            ("TEXTCOLOR",(0,0),(0,-1), C_GRAY), ("TEXTCOLOR",(1,0),(1,-1), C_DARK),
            ("ROWBACKGROUNDS",(0,0),(-1,-1), [colors.white, C_LIGHTGRAY]),
            ("GRID",(0,0),(-1,-1), 0.3, C_BORDER), ("PADDING",(0,0),(-1,-1), 4),
        ]))
        return t

    left = _detail_rows([
        ["업로드 빈도", inf.get("upload_frequency") or "-"],
        ["활성 시간", inf.get("active_hours") or "-"],
        ["마지막 게시", inf.get("last_post_date") or "-"],
        ["카테고리", inf.get("category") or manual.get("main_category") or "-"],
        ["바이오", (inf.get("biography") or "-")[:60]],
        ["프로필 링크", (inf.get("external_url") or "-")[:50]],
    ])
    right = _detail_rows([
        ["담당자", manual.get("contact_name") or "-"],
        ["카카오", manual.get("contact_kakao") or "-"],
        ["이메일", manual.get("contact_email") or inf.get("public_email") or "-"],
        ["피드 단가", f"{manual.get('feed_price') or 0:,}만원" if manual.get("feed_price") else "-"],
        ["릴스 단가", f"{manual.get('reel_price') or 0:,}만원" if manual.get("reel_price") else "-"],
        ["협업 유형", manual.get("collab_types") or "-"],
    ])
    detail = Table([[left, right]], colWidths=[12*cm, 12*cm])
    detail.setStyle(TableStyle([("VALIGN",(0,0),(-1,-1),"TOP"),("PADDING",(0,0),(-1,-1),0)]))
    elements.append(detail)
    elements.append(Spacer(1, 6))

    # ════════ 최근 게시물 (Top 3) ════════
    top_likes = inf.get("top_posts_likes", [])
    top_reels = inf.get("top_reels_views", [])
    if top_likes or top_reels:
        elements.append(_section_title("인기 게시물"))
        post_headers = ["유형", "URL", "좋아요", "댓글", "조회수"]
        post_rows = [post_headers]
        for p in (top_likes or [])[:3]:
            url = p.get("url", "") if isinstance(p, dict) else ""
            short = url.split("/p/")[-1].rstrip("/")[:12] if "/p/" in url else url[-20:]
            post_rows.append([
                "피드", short, _fmt(p.get("likes",0) if isinstance(p, dict) else 0),
                _fmt(p.get("comments",0) if isinstance(p, dict) else 0), "-"
            ])
        for p in (top_reels or [])[:3]:
            url = p.get("url", "") if isinstance(p, dict) else ""
            short = url.split("/p/")[-1].rstrip("/")[:12] if "/p/" in url else url[-20:]
            post_rows.append([
                "릴스", short, _fmt(p.get("likes",0) if isinstance(p, dict) else 0),
                "-", _fmt(p.get("views",0) if isinstance(p, dict) else 0)
            ])
        if len(post_rows) > 1:
            pt = Table(post_rows, colWidths=[2*cm, 7*cm, 3.5*cm, 3.5*cm, 3.5*cm])
            pt.setStyle(TableStyle([
                ("FONTNAME",(0,0),(-1,0), FONT_BOLD), ("FONTSIZE",(0,0),(-1,-1), 7.5),
                ("TEXTCOLOR",(0,0),(-1,0), colors.white),
                ("BACKGROUND",(0,0),(-1,0), C_PURPLE),
                ("GRID",(0,0),(-1,-1), 0.3, C_BORDER), ("PADDING",(0,0),(-1,-1), 4),
                ("ALIGN",(2,0),(-1,-1),"CENTER"), ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
                ("ROWBACKGROUNDS",(0,1),(-1,-1),[colors.white, C_LIGHTGRAY]),
            ]))
            elements.append(pt)
            elements.append(Spacer(1, 6))

    # ════════ 해시태그 ════════
    htags_raw = inf.get("top_hashtags", "[]")
    try:
        htags = json.loads(htags_raw) if isinstance(htags_raw, str) else (htags_raw or [])
    except: htags = []
    if htags:
        elements.append(_section_title("자주 사용하는 해시태그"))
        tag_text = "  ".join([f"#{h['tag']}({h['count']})" for h in htags[:15]])
        elements.append(_p(tag_text, size=7.5, color=C_PURPLE))
        elements.append(Spacer(1, 4))

    # ════════ 메모 / 브랜드 ════════
    if manual.get("notes"):
        elements.append(_p(f"메모: {manual['notes']}", size=7.5, color=C_GRAY))
    if manual.get("past_brands"):
        elements.append(_p(f"협업 브랜드: {manual['past_brands']}", size=7.5, color=C_GRAY))

    # ════════ 푸터 ════════
    elements.append(Spacer(1, 4))
    elements.append(HRFlowable(width="100%", thickness=0.5, color=C_BORDER))
    elements.append(_p(
        f"SuperTag  |  {datetime.now().strftime('%Y-%m-%d %H:%M')}  |  통계갱신: {inf.get('last_post_date') or '-'}",
        size=6.5, color=colors.HexColor("#94a3b8"), align=TA_RIGHT
    ))

    return elements


def export_single_pdf(inf: dict, manual: dict) -> bytes:
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=landscape(A4),
                             leftMargin=1.5*cm, rightMargin=1.5*cm,
                             topMargin=1.2*cm, bottomMargin=1*cm)
    doc.build(_scorecard_elements(inf, manual))
    return buf.getvalue()


def export_multi_pdf(inf_list: list) -> bytes:
    buf = io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=landscape(A4),
                             leftMargin=1.5*cm, rightMargin=1.5*cm,
                             topMargin=1.2*cm, bottomMargin=1*cm)
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
                             topMargin=1.2*cm, bottomMargin=0.8*cm)
    elements = []

    elements.append(_p("인플루언서 비교 리스트", bold=True, size=14, color=C_DARK))
    elements.append(_p(f"총 {len(inf_list)}명 | {datetime.now().strftime('%Y-%m-%d %H:%M')}", size=8, color=C_GRAY))
    elements.append(Spacer(1, 8))

    headers = ["#", "계정", "팔로워", "참여율", "릴스조회", "릴스좋아요", "피드좋아요", "게시물",
               "카테고리", "피드단가", "릴스단가"]
    data = [headers]

    for i, (inf, manual) in enumerate(inf_list):
        data.append([
            str(i+1),
            f"@{inf.get('username', '')}",
            _fmt(inf.get("follower_count")),
            _fmt_rate(inf.get("engagement_rate")),
            _fmt(inf.get("avg_reel_views")),
            _fmt(inf.get("avg_reel_likes")),
            _fmt(inf.get("avg_feed_likes")),
            _fmt(inf.get("media_count")),
            (inf.get("category") or manual.get("main_category") or "-")[:8],
            f"{manual.get('feed_price') or 0}만" if manual.get("feed_price") else "-",
            f"{manual.get('reel_price') or 0}만" if manual.get("reel_price") else "-",
        ])

    cw = [1*cm, 3.5*cm, 2.5*cm, 2*cm, 2.5*cm, 2.5*cm, 2.5*cm, 2*cm, 3*cm, 2*cm, 2*cm]
    tbl = Table(data, colWidths=cw, repeatRows=1)
    tbl.setStyle(TableStyle([
        ("BACKGROUND",(0,0),(-1,0), C_PURPLE),
        ("TEXTCOLOR",(0,0),(-1,0), colors.white),
        ("FONTNAME",(0,0),(-1,0), FONT_BOLD),
        ("FONTSIZE",(0,0),(-1,-1), 7.5),
        ("FONTNAME",(0,1),(-1,-1), FONT_NAME),
        ("ROWBACKGROUNDS",(0,1),(-1,-1), [colors.white, C_LIGHTGRAY]),
        ("GRID",(0,0),(-1,-1), 0.3, C_BORDER),
        ("ALIGN",(0,0),(-1,-1),"CENTER"),
        ("ALIGN",(1,1),(1,-1),"LEFT"),
        ("PADDING",(0,0),(-1,-1), 3.5),
        ("VALIGN",(0,0),(-1,-1),"MIDDLE"),
    ]))
    elements.append(tbl)
    doc.build(elements)
    return buf.getvalue()
