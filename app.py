# -*- coding: utf-8 -*-
"""
와플랫 공공 지표 - 지자체 운영 대시보드
Google Sheets 실데이터 기반 인터랙티브 Streamlit 대시보드
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots

# Plotly 전역 폰트: Pretendard (한국어 가독성 최적화)
pio.templates["waplat"] = go.layout.Template(
    layout=go.Layout(
        font=dict(family="Pretendard, Noto Sans KR, sans-serif", size=13, color="#1E293B"),
        title=dict(font=dict(size=15, color="#1E293B", family="Pretendard, Noto Sans KR")),
        paper_bgcolor="white",
        plot_bgcolor="white",
        xaxis=dict(gridcolor="#F1F5F9", linecolor="#E2E8F0"),
        yaxis=dict(gridcolor="#F1F5F9", linecolor="#E2E8F0"),
    )
)
pio.templates.default = "waplat"

from sheets_data import (
    fetch_all_sheets, fetch_sheet, SHEET_GIDS,
    build_dashboard_data, build_municipality_heatmap_data,
    get_week_summary, get_weekly_municipality_data,
    safe_numeric, find_municipality_columns, extract_municipality_name,
    REGION_MAP, MUNICIPALITY_KEYWORDS,
)
from unified_data import (
    load_unified_data, get_agency_master, save_agency,
    toggle_agency_active, get_agency_summary, get_data_source_info,
    get_db_data, seed_agencies_from_sheets, get_active_agencies,
    import_safety_check_from_sheets,
)
from data_input import DATA_TYPES, process_pasted_data, detect_data_type
from local_db import (
    save_safety_check, save_generic, get_safety_check_data,
    get_all_dates, get_data_stats, init_db,
    save_agency, deactivate_agency, activate_agency, delete_agency,
    get_all_agencies, get_agency_summary,
)

# ============================================================
# 페이지 설정
# ============================================================
st.set_page_config(
    page_title="와플랫 공공 지표 대시보드",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================
# 🔐 비밀번호 보호
# ============================================================
def _check_password() -> bool:
    """비밀번호 확인 — 통과 시 True 반환
    REQUIRE_AUTH=true 일 때만 비밀번호 요구 (로컬에서는 자동 통과)
    """
    # 로컬 실행 시 비밀번호 스킵 (secrets에 REQUIRE_AUTH=true 없으면 통과)
    if str(st.secrets.get("REQUIRE_AUTH", "false")).lower() != "true":
        return True

    if st.session_state.get("authenticated"):
        return True

    # 중앙 정렬된 로그인 카드
    st.markdown("""
    <style>
    #login-wrap {
        max-width: 400px; margin: 80px auto; padding: 2.5rem 2rem;
        background: white; border-radius: 20px;
        box-shadow: 0 8px 32px rgba(102,126,234,0.18);
        text-align: center;
    }
    #login-wrap h2 { color: #667eea; margin-bottom: 0.2rem; }
    #login-wrap p  { color: #888; font-size: 0.9rem; margin-bottom: 1.5rem; }
    </style>
    <div id="login-wrap">
      <h2>📊 와플랫 공공 대시보드</h2>
      <p>접근 권한이 필요합니다</p>
    </div>
    """, unsafe_allow_html=True)

    col = st.columns([1, 2, 1])[1]
    with col:
        pw = st.text_input("비밀번호", type="password", label_visibility="collapsed",
                           placeholder="비밀번호를 입력하세요")
        if st.button("로그인", use_container_width=True, type="primary"):
            correct = st.secrets.get("PASSWORD", "waflat2025!")
            if pw == correct:
                st.session_state["authenticated"] = True
                st.rerun()
            else:
                st.error("비밀번호가 틀렸습니다.")
    return False

if not _check_password():
    st.stop()

# ============================================================
# 커스텀 CSS
# ============================================================
st.markdown("""
<style>
@import url('https://cdn.jsdelivr.net/gh/orioncactus/pretendard@v1.3.9/dist/web/static/pretendard.min.css');
@import url('https://fonts.googleapis.com/css2?family=Noto+Sans+KR:wght@300;400;500;600;700;900&display=swap');

/* ── 기본 폰트: Pretendard (가독성 최적화) ── */
html, body, [class*="css"] {
    font-family: 'Pretendard', 'Noto Sans KR', -apple-system, BlinkMacSystemFont, sans-serif;
    -webkit-font-smoothing: antialiased;
    letter-spacing: -0.01em;
}

/* ── 전체 배경 ── */
.stApp { background-color: #F4F6FB; }

/* ══════════════════════════════════════
   KPI 카드 — 공통 베이스
══════════════════════════════════════ */
.metric-card, .metric-card-green, .metric-card-red, .metric-card-orange {
    position: relative;
    padding: 1.4rem 1.2rem 1.1rem;
    border-radius: 20px;
    color: white;
    text-align: center;
    margin-bottom: 0.6rem;
    overflow: hidden;
    transition: transform 0.18s ease, box-shadow 0.18s ease;
}
.metric-card:hover,
.metric-card-green:hover,
.metric-card-red:hover,
.metric-card-orange:hover {
    transform: translateY(-3px);
    box-shadow: 0 12px 32px rgba(0,0,0,0.18) !important;
}

/* 카드 안쪽 빛 효과 */
.metric-card::before, .metric-card-green::before,
.metric-card-red::before, .metric-card-orange::before {
    content: '';
    position: absolute;
    top: -30%; right: -10%;
    width: 160px; height: 160px;
    background: rgba(255,255,255,0.10);
    border-radius: 50%;
    pointer-events: none;
}

/* 라벨 (지표명) */
.metric-card h3, .metric-card-green h3,
.metric-card-red h3, .metric-card-orange h3 {
    margin: 0 0 0.5rem;
    font-size: 0.72rem;
    font-weight: 500;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    opacity: 0.80;
}

/* 숫자 값 — 크고 굵게 */
.metric-card h1, .metric-card-green h1,
.metric-card-red h1, .metric-card-orange h1 {
    margin: 0 0 0.4rem;
    font-size: 2.6rem;
    font-weight: 900;
    letter-spacing: -0.02em;
    line-height: 1;
}

/* 전주 대비 델타 */
.metric-card p, .metric-card-green p,
.metric-card-red p, .metric-card-orange p {
    margin: 0;
    font-size: 0.78rem;
    opacity: 0.85;
}
.metric-card .up   { color: #A5F3C0; font-weight: 700; }
.metric-card .down { color: #FFAB91; font-weight: 700; }
.metric-card .flat { color: rgba(255,255,255,0.6); }

/* ── 카드별 색상 ── */
.metric-card {
    background: linear-gradient(135deg, #5B73E8 0%, #7B52D3 100%);
    box-shadow: 0 6px 20px rgba(91,115,232,0.35);
}
.metric-card-green {
    background: linear-gradient(135deg, #11B981 0%, #059669 100%);
    box-shadow: 0 6px 20px rgba(17,185,129,0.35);
}
.metric-card-red {
    background: linear-gradient(135deg, #F87171 0%, #DC2626 100%);
    box-shadow: 0 6px 20px rgba(248,113,113,0.35);
}
.metric-card-orange {
    background: linear-gradient(135deg, #FB923C 0%, #EA580C 100%);
    box-shadow: 0 6px 20px rgba(251,146,60,0.35);
}

/* ══════════════════════════════════════
   섹션 헤더
══════════════════════════════════════ */
.section-header {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    padding: 0.65rem 1.1rem;
    margin: 1.8rem 0 1rem;
    background: white;
    border-radius: 12px;
    border-left: 5px solid #5B73E8;
    font-weight: 700;
    font-size: 1rem;
    color: #1E293B;
    box-shadow: 0 2px 8px rgba(0,0,0,0.06);
}

/* ══════════════════════════════════════
   인사이트 박스
══════════════════════════════════════ */
.insight-box {
    background: #EFF6FF;
    border-left: 4px solid #5B73E8;
    padding: 0.9rem 1.2rem;
    margin: 0.4rem 0;
    border-radius: 0 12px 12px 0;
    font-size: 0.88rem;
    line-height: 1.65;
    color: #1E293B;
}
.insight-box-danger {
    background: #FFF5F5;
    border-left: 4px solid #F87171;
    padding: 0.9rem 1.2rem;
    margin: 0.4rem 0;
    border-radius: 0 12px 12px 0;
    font-size: 0.88rem;
    line-height: 1.65;
}
.insight-box-success {
    background: #F0FDF4;
    border-left: 4px solid #11B981;
    padding: 0.9rem 1.2rem;
    margin: 0.4rem 0;
    border-radius: 0 12px 12px 0;
    font-size: 0.88rem;
    line-height: 1.65;
}

/* ══════════════════════════════════════
   상태 뱃지
══════════════════════════════════════ */
.status-danger   { background:#FEE2E2; color:#991B1B; padding:3px 11px; border-radius:999px; font-weight:700; font-size:0.78rem; display:inline-block; }
.status-caution  { background:#FEF9C3; color:#92400E; padding:3px 11px; border-radius:999px; font-weight:700; font-size:0.78rem; display:inline-block; }
.status-normal   { background:#F1F5F9; color:#475569; padding:3px 11px; border-radius:999px; font-size:0.78rem; display:inline-block; }
.status-excellent{ background:#DCFCE7; color:#166534; padding:3px 11px; border-radius:999px; font-weight:700; font-size:0.78rem; display:inline-block; }

/* ══════════════════════════════════════
   탭 스타일
══════════════════════════════════════ */
.stTabs [data-baseweb="tab-list"] {
    gap: 2px;
    background: #E8ECF4;
    border-radius: 12px;
    padding: 4px;
}
.stTabs [data-baseweb="tab"] {
    padding: 7px 18px;
    border-radius: 9px;
    font-weight: 500;
    font-size: 0.84rem;
    color: #64748B;
    background: transparent;
    border: none;
}
.stTabs [aria-selected="true"] {
    background: white !important;
    color: #5B73E8 !important;
    font-weight: 700 !important;
    box-shadow: 0 2px 8px rgba(0,0,0,0.08);
}

/* ══════════════════════════════════════
   사이드바
══════════════════════════════════════ */
section[data-testid="stSidebar"] {
    background: #1E293B;
}
section[data-testid="stSidebar"] * {
    color: #CBD5E1 !important;
}
section[data-testid="stSidebar"] .stRadio label {
    font-size: 0.85rem !important;
    padding: 4px 0;
}
section[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p {
    color: #94A3B8 !important;
    font-size: 0.78rem !important;
}

/* ══════════════════════════════════════
   데이터프레임
══════════════════════════════════════ */
div[data-testid="stMetricValue"] { font-size: 1.4rem; font-weight: 700; }

/* ── 전체 컨텐츠 영역 카드화 ── */
section.main > div { padding-top: 1rem; }
</style>
""", unsafe_allow_html=True)

# ============================================================
# 데이터 로드 — DB 우선, Google Sheets는 새로고침 시에만
# ============================================================
@st.cache_data(ttl=14400, show_spinner="Google Sheets 데이터 로딩 중... (최초 1회, 이후 4시간 캐시)")
def load_all_data():
    """전체 Google Sheets 로드 (4시간 캐시)"""
    sheets = fetch_all_sheets()
    data = build_dashboard_data(sheets)
    return sheets, data


@st.cache_data(ttl=14400, show_spinner=False)
def cached_heatmap(_data: dict, week: str) -> "pd.DataFrame":
    """지자체 히트맵 — 주차별 캐시 (페이지 재진입 시 즉시 반환)"""
    return build_municipality_heatmap_data(_data, week)


@st.cache_data(ttl=14400, show_spinner=False)
def cached_week_summary(_sheets: dict, _data: dict, week: str) -> dict:
    """주차 요약 — 주차별 캐시"""
    return get_week_summary(_sheets, _data, week)

try:
    sheets, data = load_all_data()
    DATA_LOADED = True
    # 최초 1회: Google Sheets에서 지자체 자동 등록 + 안부확인 raw 데이터 임포트
    if "agency_seeded" not in st.session_state:
        seeded = seed_agencies_from_sheets(sheets)
        if seeded > 0:
            st.toast(f"Google Sheets에서 {seeded}개 지자체 자동 등록됨")
        # 안부확인 raw 데이터 임포트
        imported = import_safety_check_from_sheets(sheets)
        if imported > 0:
            st.toast(f"안부확인 raw 데이터 {imported}건 임포트됨")
        st.session_state["agency_seeded"] = True
except Exception as e:
    st.error(f"데이터 로딩 실패: {e}")
    DATA_LOADED = False
    sheets, data = {}, {}

# ============================================================
# 상태 색상 / 배지
# ============================================================
STATUS_COLORS = {"집중관리": "#FF4B4B", "주의관리": "#FFA500", "정상": "#9E9E9E", "우수사례": "#00C853"}
DETAIL_STATUS_COLORS = {"위험": "#FF4B4B", "주의": "#FFA500", "보통": "#9E9E9E", "우수": "#00C853"}

def status_badge(status):
    cls = {"집중관리": "danger", "주의관리": "caution", "정상": "normal", "우수사례": "excellent"}
    return f'<span class="status-{cls.get(status, "normal")}">{status}</span>'

def delta_html(val, suffix="", invert=False, prev_val=0):
    """증감 표시 HTML (invert=True면 음수가 좋은 것)
    val: 현재값과 이전값의 차이 (delta)
    prev_val: 이전 값 (비율 계산용)
    """
    if val > 0:
        cls = "down" if invert else "up"
        arrow = "▲"
    elif val < 0:
        cls = "up" if invert else "down"
        arrow = "▼"
    else:
        cls, arrow = "flat", "→"

    # 비율 계산
    if prev_val and prev_val != 0:
        pct = abs(val) / abs(prev_val) * 100
        if suffix == "명":
            return f'<span class="{cls}">{arrow} {abs(val):,.0f}{suffix} ({pct:.1f}%)</span>'
        else:
            return f'<span class="{cls}">{arrow} {abs(val):.1f}{suffix} ({pct:.1f}%)</span>'
    else:
        if suffix == "명":
            return f'<span class="{cls}">{arrow} {abs(val):,.0f}{suffix}</span>'
        else:
            return f'<span class="{cls}">{arrow} {abs(val):.1f}{suffix}</span>'

# ============================================================
# 사이드바
# ============================================================
with st.sidebar:
    # ── 로고 ──────────────────────────────────────────────────
    import os, base64
    _logo_path = os.path.join(os.path.dirname(__file__), "assets", "logo.png")
    if os.path.exists(_logo_path):
        with open(_logo_path, "rb") as _f:
            _logo_b64 = base64.b64encode(_f.read()).decode()
        st.markdown(
            f"""<div style="background:white;border-radius:12px;
                            padding:10px 16px 8px;margin:4px 0 12px;
                            text-align:center;
                            box-shadow:0 2px 8px rgba(0,0,0,0.25)">
                  <img src="data:image/png;base64,{_logo_b64}"
                       style="max-width:150px;height:auto;display:block;margin:0 auto">
                  <div style="font-size:0.65rem;color:#64748B;margin-top:4px;
                              font-family:'Pretendard','Noto Sans KR',sans-serif">
                    공공 서비스 지표 대시보드
                  </div>
                </div>""",
            unsafe_allow_html=True,
        )
    else:
        st.markdown(
            """<div style="padding:0.6rem 0 0.4rem;text-align:left">
              <span style="font-size:1.6rem;font-weight:900;color:white;
                           font-family:'Pretendard','Noto Sans KR',sans-serif;
                           letter-spacing:-0.03em">waplat</span>
              <span style="font-size:0.7rem;color:rgba(255,255,255,0.55);
                           display:block;margin-top:2px">공공 서비스 지표 대시보드</span>
            </div>""",
            unsafe_allow_html=True,
        )

    weeks = data.get("주차목록", [])
    if weeks:
        selected_week = st.selectbox(
            "📅 주차 선택",
            options=list(reversed(weeks)),
            index=0,
            help="데이터를 확인할 주차를 선택하세요"
        )
    else:
        selected_week = None
        st.warning("주차 데이터 없음")

    st.divider()

    # 페이지 선택
    page = st.radio(
        "페이지 선택",
        [
            "📋 Summary",
            "👥 1.회원가입 & 이탈",
            "🖐 2.안부확인",
            "📊 3.안부체크율",
            "🔄 4.안부체크 변경",
            "❤ 5.심혈관체크",
            "😰 6.스트레스체크",
            "💊 7.복약관리",
            "🩺 8.건강상담",
            "💬 9.생활상담",
            "🃏 10.맞고(와플랫)",
            "🎮 11.맞고(와플랫+게스트)",
            "👤 12.맞고(게스트)",
            "🤖 AI 생활지원사",
            "📝 자동 보고서",
            "📥 데이터 입력",
            "🗄 DB 뷰어",
            "⚙ 지자체 설정",
        ],
        index=0,
    )

    st.divider()
    if st.button("🔄 데이터 새로고침"):
        st.cache_data.clear()
        st.rerun()
    st.caption("4시간마다 자동 새로고침")

    if DATA_LOADED:
        # 데이터 소스 상태 — 항상 최신 데이터 사용 (summary와 동기화)
        agency_stats = get_agency_summary(sheets)
        if agency_stats.get("total", 0) > 0:
            st.caption(f"🏛 지자체 {agency_stats.get('active', 0)}개 운영 중")
            st.caption(f"🛡 세이프 {agency_stats.get('safe_count', 0)} | 📋 베이직 {agency_stats.get('basic_count', 0)}")
        st.caption("✅ 데이터 연결됨")
    else:
        st.caption("⚠️ 데이터 로딩 실패")

# ============================================================
# Helper 함수
# ============================================================
def weekly_total(df, value_col="값", agg="sum"):
    """주차별 합계를 시작일 포함하여 계산"""
    if "시작일" in df.columns:
        # 주차+시작일 쌍의 첫 번째 시작일 유지
        date_map = df.groupby("주차")["시작일"].first()
        total = df.groupby("주차")[value_col].agg(agg).reset_index()
        total["시작일"] = total["주차"].map(date_map)
    else:
        total = df.groupby("주차")[value_col].agg(agg).reset_index()
    return total

def get_prev_week(week):
    """이전 주차 반환"""
    if week in weeks:
        idx = weeks.index(week)
        return weeks[idx - 1] if idx > 0 else None
    return None

def shorten_date(date_str):
    """날짜 문자열에서 '20' 접두사 제거: 2026-03-21 → 26-03-21"""
    s = str(date_str).strip()
    if s.startswith("20") and len(s) >= 10:
        return s[2:]
    return s

def shorten_dates_in_df(df, col):
    """DataFrame의 날짜 컬럼을 짧은 형식으로 변환"""
    df = df.copy()
    df[col] = df[col].apply(shorten_date)
    return df

def date_to_week_label(date_str):
    """날짜 문자열을 ISO 주차 형식으로 변환: 2026-04-05 → 26-15 (다른 주차 컬럼과 통일)"""
    from datetime import datetime
    s = str(date_str).strip()
    try:
        if len(s) >= 10 and s[4:5] == "-":
            dt = datetime.strptime(s[:10], "%Y-%m-%d")
            yr, wk, _ = dt.isocalendar()
            return f"{str(yr)[2:]}-{wk:02d}"
    except Exception:
        pass
    return s

def week_label_df(df, col):
    """DataFrame의 날짜 컬럼을 ISO 주차 레이블로 변환 (호출 전 시간순 정렬 권장)"""
    df = df.copy()
    df[col] = df[col].apply(date_to_week_label)
    return df

# 공통 범례 설정 (X축 겹침 방지)
LEGEND_BELOW = dict(orientation="h", yanchor="top", y=-0.22, xanchor="center", x=0.5, font=dict(size=9))
LEGEND_BELOW_LARGE = dict(orientation="h", yanchor="top", y=-0.28, xanchor="center", x=0.5, font=dict(size=8))

def plot_weekly_series(df, x_col, y_col, title, color="#2F5496", height=300):
    """주간 시계열 차트 — 최신 포인트 강조 + 전주 대비 변화 표시"""
    df = shorten_dates_in_df(df, x_col)
    fig = px.area(df, x=x_col, y=y_col, color_discrete_sequence=[color])

    # 최신 데이터 포인트 강조 + 전주 대비 어노테이션
    if len(df) >= 2:
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        latest_val = float(latest[y_col]) if not pd.isna(latest[y_col]) else 0
        prev_val = float(prev[y_col]) if not pd.isna(prev[y_col]) else 0
        delta = latest_val - prev_val
        pct = (delta / prev_val * 100) if prev_val != 0 else 0
        arrow = "▲" if delta > 0 else "▼" if delta < 0 else "→"
        color_d = "#00C853" if delta >= 0 else "#FF4B4B"

        fig.add_trace(go.Scatter(
            x=[latest[x_col]], y=[latest_val],
            mode="markers+text",
            marker=dict(size=13, color=color, line=dict(width=2, color="white")),
            text=[f"{arrow} {abs(delta):,.0f} ({abs(pct):.1f}%)"],
            textposition="top center",
            textfont=dict(size=13, color=color_d, family="Pretendard, Noto Sans KR"),
            showlegend=False,
            hoverinfo="skip",
        ))

    fig.update_layout(
        title=title, height=height,
        margin=dict(t=40, b=60, l=40, r=10),
        hovermode="x unified",
        xaxis=dict(type="category", title=""),
        yaxis_title="",
    )
    fig.update_traces(
        hovertemplate="<b>%{x}</b><br>값: %{y:,.0f}<extra></extra>",
        line=dict(width=2),
        fillcolor=f"rgba({int(color[1:3],16)},{int(color[3:5],16)},{int(color[5:7],16)},0.15)",
        selector=dict(type="scatter", fill="tozeroy"),
    )
    st.plotly_chart(fig, use_container_width=True)

def plot_bar_rate_dual(df, x_col, bar_col, bar_label, bar_color,
                       line_col, line_label, line_color,
                       title, bar_unit="명", line_unit="%", height=430):
    """이용자수(막대) + 이용률(꺾은선) 통합 듀얼 Y축 차트 — 모든 서비스 페이지 공통"""
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    valid = df[df[x_col].astype(str).str.strip() != "nan"].copy()
    bar_vals = valid[bar_col].apply(safe_numeric)
    # 막대: 반투명으로 배경 처리 → 꺾은선(이용비중)이 더 잘 보임
    fig.add_trace(go.Bar(
        x=valid[x_col], y=bar_vals, name=bar_label,
        marker_color=bar_color, opacity=0.55,
        text=bar_vals.apply(lambda v: f"{int(v):,}" if v == int(v) else f"{v:.1f}"),
        textposition="outside", textfont=dict(size=14, color="#222", family="Noto Sans KR"),
        hovertemplate=f"<b>%{{x}}</b><br>{bar_label}: %{{y:,}}{bar_unit}<extra></extra>"
    ), secondary_y=False)
    if line_col and line_col in valid.columns:
        line_vals = valid[line_col].apply(safe_numeric)
        # 꺾은선: 굵고 선명하게 — 이용비중이 핵심 지표
        fig.add_trace(go.Scatter(
            x=valid[x_col], y=line_vals, name=line_label,
            mode="lines+markers+text",
            line=dict(color=line_color, width=3),
            marker=dict(size=10, color=line_color,
                        line=dict(color="white", width=2)),
            text=line_vals.apply(lambda v: f"<b>{v:.1f}{line_unit}</b>"),
            textposition="top center",
            textfont=dict(size=15, color=line_color, family="Noto Sans KR"),
            hovertemplate=f"<b>%{{x}}</b><br>{line_label}: %{{y:.1f}}{line_unit}<extra></extra>"
        ), secondary_y=True)
    fig.update_layout(
        title=title, height=height, hovermode="x unified",
        xaxis=dict(type="category"),
        legend=LEGEND_BELOW, margin=dict(t=40, b=70), bargap=0.3,
    )
    fig.update_yaxes(title_text=bar_unit, secondary_y=False)
    fig.update_yaxes(title_text=line_unit, secondary_y=True, showgrid=False)
    st.plotly_chart(fig, use_container_width=True)


def extract_mun_ratio_trend(raw_df: pd.DataFrame) -> pd.DataFrame:
    """시트 원본에서 이용자비중 컬럼(AI~BK)을 (주차, 지자체명, 값) long format으로 변환"""
    if raw_df.empty:
        return pd.DataFrame()
    week_col = next((c for c in raw_df.columns if "주차" in str(c)), None)
    if week_col is None:
        return pd.DataFrame()
    ratio_cols = [c for c in raw_df.columns if "이용자비중" in str(c)]
    if not ratio_cols:
        return pd.DataFrame()

    sub = raw_df[[week_col] + ratio_cols].copy()
    sub = sub[~sub[week_col].astype(str).str.strip().isin(["", "nan"])]
    sub[week_col] = sub[week_col].astype(str).str.strip()
    for col in ratio_cols:
        sub[col] = sub[col].apply(safe_numeric)

    long = sub.melt(id_vars=[week_col], value_vars=ratio_cols,
                    var_name="_col", value_name="값")

    def _mun_name(s):
        s = str(s).strip()
        # "경남사회서비스원 이용자비중\n이용자비중" 등 중복 제거
        s = s.replace("\n이용자비중", "").replace(" 이용자비중", "")
        return s.strip()

    long["지자체명"] = long["_col"].apply(_mun_name)
    long = long.rename(columns={week_col: "주차"})
    return long[["주차", "지자체명", "값"]].reset_index(drop=True)


def plot_municipality_bar(df, value_col, title, color_map=None, height=400):
    """지자체별 바 차트 (내림차순 정렬)"""
    df_sorted = df.sort_values(value_col, ascending=True)
    fig = px.bar(df_sorted, y="지자체명", x=value_col, orientation="h",
                 color="권역" if "권역" in df_sorted.columns else None,
                 color_discrete_map={"수도권": "#2F5496", "비수도권": "#FF6F00", "기관": "#7B1FA2", "기타": "#9E9E9E"},
                 height=max(height, len(df_sorted) * 28))
    fig.update_layout(
        title=title, margin=dict(t=40, b=10, l=10, r=10),
        xaxis_title="", yaxis_title="",
        legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5),
    )
    fig.update_traces(
        hovertemplate="<b>%{y}</b><br>%{x:,.1f}<extra></extra>",
    )
    st.plotly_chart(fig, use_container_width=True)

def page_week_range_selector(key_prefix: str, all_weeks: list):
    """페이지 내 주차 범위 선택기 (시작~끝 주차)
    기본 시작: 25-52 주차 (없으면 최근 12주)
    Returns: (start_week, end_week) or (None, None)
    """
    if not all_weeks:
        return None, None

    # 기본 시작 인덱스: 25-52 주차
    default_start = "25-52"
    if default_start in all_weeks:
        default_idx = all_weeks.index(default_start)
    else:
        # 25-52가 없으면 26-01 시도
        if "26-01" in all_weeks:
            default_idx = all_weeks.index("26-01")
        else:
            default_idx = max(0, len(all_weeks) - 12)

    with st.expander("📅 기간 설정 (펼쳐서 변경)", expanded=False):
        col1, col2 = st.columns(2)
        with col1:
            start_week = st.selectbox(
                "시작 주차", all_weeks,
                index=default_idx,
                key=f"{key_prefix}_start"
            )
        with col2:
            start_idx = all_weeks.index(start_week) if start_week in all_weeks else 0
            end_options = all_weeks[start_idx:]
            end_week = st.selectbox(
                "종료 주차", end_options,
                index=len(end_options) - 1,  # 기본: 마지막 주차
                key=f"{key_prefix}_end"
            )
        st.caption(f"선택 기간: {start_week} ~ {end_week}")
    return start_week, end_week


def filter_by_week_range(df, week_col, start_week, end_week, all_weeks):
    """DataFrame을 주차 범위로 필터링"""
    if df.empty or not start_week or not end_week:
        return df
    if week_col not in df.columns:
        return df
    start_idx = all_weeks.index(start_week) if start_week in all_weeks else 0
    end_idx = all_weeks.index(end_week) if end_week in all_weeks else len(all_weeks) - 1
    valid_weeks = set(all_weeks[start_idx:end_idx + 1])
    return df[df[week_col].astype(str).str.strip().isin(valid_weeks)]


def get_active_agencies_for_week(week: str) -> list:
    """특정 주차 기준으로 계약 중인 지자체 반환"""
    agencies = get_agency_master()
    if agencies.empty:
        return []

    # 주차 → 시작일 매핑
    wu = data.get("weekly_users", pd.DataFrame())
    target_date = None
    if not wu.empty and "주차" in wu.columns and "시작일" in wu.columns:
        match = wu[wu["주차"].astype(str).str.strip() == week]
        if not match.empty:
            target_date = str(match.iloc[0].get("시작일", "")).strip()

    active = []
    for _, row in agencies.iterrows():
        if row.get("is_active", 0) != 1:
            continue
        # 계약 기간 체크
        if target_date:
            contract_start = str(row.get("contract_start", "")).strip()
            contract_end = str(row.get("contract_end", "")).strip()
            # 시작일 이전이면 제외
            if contract_start and target_date < contract_start:
                continue
            # 종료일 이후면 제외
            if contract_end and contract_end != "" and target_date > contract_end:
                continue
        active.append(row["agency_name"])
    return active


def plot_municipality_lines(df_long, title, height=350, metric_label="값", show_avg=True):
    """지자체별 주간 추이 라인 차트 — 10개 이상이면 Top/Bottom 5 포커스"""
    if df_long.empty:
        st.info("데이터 없음")
        return

    x_col = "주차"
    df_long = shorten_dates_in_df(df_long, x_col)
    mun_count = df_long["지자체명"].nunique()

    # 지자체가 10개 이상이면 포커스 모드
    if mun_count >= 10:
        latest_week = df_long[x_col].max()
        latest = df_long[df_long[x_col] == latest_week].copy()
        top5 = latest.nlargest(5, "값")["지자체명"].tolist()
        bot5 = latest.nsmallest(5, "값")["지자체명"].tolist()

        view_mode = st.radio(
            "표시 모드", ["전체", "Top 5", "Bottom 5", "Top 5 + Bottom 5"],
            index=3, horizontal=True, key=f"view_{hash(title) % 100000}"
        )
        if view_mode == "Top 5":
            df_long = df_long[df_long["지자체명"].isin(top5)]
        elif view_mode == "Bottom 5":
            df_long = df_long[df_long["지자체명"].isin(bot5)]
        elif view_mode == "Top 5 + Bottom 5":
            df_long = df_long[df_long["지자체명"].isin(top5 + bot5)]

    fig = px.line(df_long, x=x_col, y="값", color="지자체명", markers=True)

    # 전체 평균 참조선 (점선)
    if show_avg and not df_long.empty:
        avg_by_week = df_long.groupby(x_col)["값"].mean().reset_index()
        fig.add_trace(go.Scatter(
            x=avg_by_week[x_col], y=avg_by_week["값"],
            mode="lines", name="── 전체 평균",
            line=dict(color="#333", width=3, dash="dash"),
            hovertemplate="평균: %{y:,.1f}<extra></extra>"
        ))

    fig.update_layout(
        title=title, height=height,
        margin=dict(t=40, b=60, l=40, r=10),
        hovermode="x unified",
        legend=LEGEND_BELOW,
        xaxis=dict(type="category", title=""),
        yaxis_title=metric_label,
    )
    fig.update_traces(hovertemplate="%{y:,.0f}<extra>%{fullData.name}</extra>")
    st.plotly_chart(fig, use_container_width=True)


# ============================================================
# 📋 Summary 페이지
# ============================================================
if page == "📋 Summary":
    if selected_week:
        summary = cached_week_summary(sheets, data, selected_week)
        prev_week = get_prev_week(selected_week)
        prev_summary = cached_week_summary(sheets, data, prev_week) if prev_week else {}

        st.markdown(f'<div class="section-header">📅 {selected_week}주차 ({summary.get("시작일", "")}) 운영 현황</div>', unsafe_allow_html=True)

        # 회원가입 현황 (이용자현황 시트에서)
        reg = data.get("registration", pd.DataFrame())
        total_contract = 0
        total_registered = 0
        total_incomplete = 0
        if not reg.empty:
            if "협약인원" in reg.columns:
                total_contract = int(reg["협약인원"].sum())
            if "가입완료" in reg.columns:
                total_registered = int(reg["가입완료"].sum())
            if "가입미완료" in reg.columns:
                total_incomplete = int(reg["가입미완료"].sum())
        total_reg_rate = round(total_registered / total_contract * 100, 1) if total_contract > 0 else 0

        # 주간 데이터에서 전주 가입완료 수 가져오기
        wu = data.get("weekly_users", pd.DataFrame())
        cur_registered = total_registered
        prev_registered = total_registered  # 기본값
        if not wu.empty and "주차" in wu.columns and "가입완료합계" in wu.columns:
            wu_cur = wu[wu["주차"].astype(str).str.strip() == selected_week]
            if not wu_cur.empty:
                cur_registered = safe_numeric(wu_cur.iloc[0].get("가입완료합계", total_registered))
            if prev_week:
                wu_prev = wu[wu["주차"].astype(str).str.strip() == prev_week]
                if not wu_prev.empty:
                    prev_registered = safe_numeric(wu_prev.iloc[0].get("가입완료합계", total_registered))

        delta_registered = cur_registered - prev_registered

        # KPI 카드
        cols = st.columns(4)
        kpi_data = [
            ("총 협약인원", total_contract, 0, "명", "metric-card", False),
            ("총 가입완료", cur_registered, prev_registered, "명", "metric-card-green", False),
            ("전체 가입률", total_reg_rate, 0, "%", "metric-card", False),
            ("안부확인율", summary.get("안부확인율", summary.get("안부체크율", 0)), prev_summary.get("안부확인율", prev_summary.get("안부체크율", 0)), "%", "metric-card-orange", False),
        ]
        for col, (label, val, prev_val, suffix, card_cls, invert) in zip(cols, kpi_data):
            delta = float(val) - float(prev_val) if prev_val else 0
            with col:
                st.markdown(f"""
                <div class="{card_cls}">
                    <h3>{label}</h3>
                    <h1>{val:,.0f}{suffix}</h1>
                    <p>{delta_html(delta, suffix, invert, prev_val=float(prev_val) if prev_val else 0)}</p>
                </div>
                """, unsafe_allow_html=True)

        st.markdown("")

        # 지자체 계약 현황 (베이직/세이프) + 계약 시작/종료 알림
        agency_sum = get_agency_summary(sheets)
        if agency_sum["total"] > 0:
            st.markdown('<div class="section-header">지자체 계약 현황</div>', unsafe_allow_html=True)

            # 상단 3개 카드: 활성 / 세이프 / 베이직
            acols = st.columns(3)
            with acols[0]:
                st.markdown(f"""
                <div style="text-align:center; padding:10px; border-radius:12px;
                            background:#E3F2FD; border:2px solid #1565C0;">
                    <span style="font-size:1.8rem; font-weight:700; color:#1565C0;">{agency_sum['active']}</span>
                    <br><span style="font-size:0.85rem; color:#555;">활성 지자체</span>
                </div>
                """, unsafe_allow_html=True)
            with acols[1]:
                st.markdown(f"""
                <div style="text-align:center; padding:10px; border-radius:12px;
                            background:#E8F5E9; border:2px solid #2E7D32;">
                    <span style="font-size:1.8rem; font-weight:700; color:#2E7D32;">{agency_sum['safe']}</span>
                    <br><span style="font-size:0.85rem; color:#555;">세이프 (관제)</span>
                </div>
                """, unsafe_allow_html=True)
            with acols[2]:
                st.markdown(f"""
                <div style="text-align:center; padding:10px; border-radius:12px;
                            background:#FFF3E0; border:2px solid #E65100;">
                    <span style="font-size:1.8rem; font-weight:700; color:#E65100;">{agency_sum['basic']}</span>
                    <br><span style="font-size:0.85rem; color:#555;">베이직</span>
                </div>
                """, unsafe_allow_html=True)

            st.markdown("")

            # 계약 시작/종료 알림 게시판 (최근 4주)
            from datetime import datetime, timedelta
            today = datetime.now().strftime("%Y-%m-%d")
            four_weeks_later = (datetime.now() + timedelta(days=28)).strftime("%Y-%m-%d")
            four_weeks_ago = (datetime.now() - timedelta(days=28)).strftime("%Y-%m-%d")

            MODEL_KR = {"safe": "세이프", "safe_plus": "세이프 플러스", "basic": "베이직", "basic_plus": "베이직 플러스"}

            agencies_df = get_agency_master()
            if not agencies_df.empty:
                # 4주 내 계약 시작 예정
                starting_soon = agencies_df[
                    (agencies_df["contract_start"] >= today) &
                    (agencies_df["contract_start"] <= four_weeks_later) &
                    (agencies_df["contract_start"] != "")
                ].copy()

                # 4주 내 계약 종료 예정
                ending_soon = agencies_df[
                    (agencies_df["contract_end"] >= today) &
                    (agencies_df["contract_end"] <= four_weeks_later) &
                    (agencies_df["contract_end"] != "")
                ].copy()

                # 최근 4주 내 계약 시작됨
                recently_started = agencies_df[
                    (agencies_df["contract_start"] >= four_weeks_ago) &
                    (agencies_df["contract_start"] <= today) &
                    (agencies_df["contract_start"] != "")
                ].copy()

                # 최근 4주 내 계약 종료됨
                recently_ended = agencies_df[
                    (agencies_df["contract_end"] >= four_weeks_ago) &
                    (agencies_df["contract_end"] <= today) &
                    (agencies_df["contract_end"] != "")
                ].copy()

                alert_col1, alert_col2 = st.columns(2)

                with alert_col1:
                    st.markdown("**🟢 계약 시작 (최근 4주)**")
                    if not recently_started.empty:
                        for _, r in recently_started.iterrows():
                            model = MODEL_KR.get(r.get("service_model", ""), r.get("service_model", ""))
                            st.markdown(f'<div class="insight-box-success">{r["contract_start"]} {r["agency_name"]} {model} 계약 시작 ({int(r.get("target_users", 0))}명)</div>', unsafe_allow_html=True)
                    if not starting_soon.empty:
                        for _, r in starting_soon.iterrows():
                            model = MODEL_KR.get(r.get("service_model", ""), r.get("service_model", ""))
                            st.markdown(f'<div class="insight-box">{r["contract_start"]} {r["agency_name"]} {model} 계약 시작 예정 ({int(r.get("target_users", 0))}명)</div>', unsafe_allow_html=True)
                    if recently_started.empty and starting_soon.empty:
                        st.markdown('<div class="insight-box">최근 4주 내 계약 시작 없음</div>', unsafe_allow_html=True)

                with alert_col2:
                    st.markdown("**🔴 계약 종료 (최근 4주)**")
                    if not ending_soon.empty:
                        for _, r in ending_soon.iterrows():
                            try:
                                days_left = (datetime.strptime(r["contract_end"], "%Y-%m-%d") - datetime.now()).days
                            except:
                                days_left = 0
                            model = MODEL_KR.get(r.get("service_model", ""), r.get("service_model", ""))
                            st.markdown(f'<div class="insight-box-danger">{r["contract_end"]} {r["agency_name"]} {model} 계약 종료 예정 ({days_left}일 남음)</div>', unsafe_allow_html=True)
                    if not recently_ended.empty:
                        for _, r in recently_ended.iterrows():
                            model = MODEL_KR.get(r.get("service_model", ""), r.get("service_model", ""))
                            st.markdown(f'<div class="insight-box-danger">{r["contract_end"]} {r["agency_name"]} {model} 계약 종료</div>', unsafe_allow_html=True)
                    if ending_soon.empty and recently_ended.empty:
                        st.markdown('<div class="insight-box">최근 4주 내 계약 종료 없음</div>', unsafe_allow_html=True)

            st.markdown("")

            # 세이프 대상 지자체 현황
            try:
                from local_db import get_connection as _gc
                _conn = _gc()
                safe_status = pd.read_sql_query("SELECT * FROM safe_agency_status ORDER BY monitoring_start_date", _conn)
                _conn.close()
            except:
                safe_status = pd.DataFrame()

            st.markdown('<div class="section-header">🛡 세이프 대상 지자체 현황</div>', unsafe_allow_html=True)

            if not safe_status.empty:
                # 요약 카드
                total_safe_target = int(safe_status["contract_users"].sum())
                total_monitoring = int(safe_status["registered_users"].sum())
                total_call = int(safe_status["joined_users"].sum())
                avg_m_rate = round(total_monitoring / total_safe_target * 100, 1) if total_safe_target > 0 else 0
                avg_c_rate = round(total_call / total_safe_target * 100, 1) if total_safe_target > 0 else 0

                sc1, sc2, sc3, sc4 = st.columns(4)
                with sc1:
                    st.markdown(f'<div style="text-align:center; padding:6px; border-radius:10px; background:#E8F5E9; border:1px solid #2E7D32;"><b style="font-size:1.2rem; color:#2E7D32;">{total_safe_target:,}명</b><br><span style="font-size:0.75rem;">세이프 대상 합계</span></div>', unsafe_allow_html=True)
                with sc2:
                    st.markdown(f'<div style="text-align:center; padding:6px; border-radius:10px; background:#E3F2FD; border:1px solid #1565C0;"><b style="font-size:1.2rem; color:#1565C0;">{total_monitoring:,}명</b><br><span style="font-size:0.75rem;">등록 이용자</span></div>', unsafe_allow_html=True)
                with sc3:
                    st.markdown(f'<div style="text-align:center; padding:6px; border-radius:10px; background:#FFF3E0; border:1px solid #E65100;"><b style="font-size:1.2rem; color:#E65100;">{avg_m_rate}%</b><br><span style="font-size:0.75rem;">등록 이용률</span></div>', unsafe_allow_html=True)
                with sc4:
                    st.markdown(f'<div style="text-align:center; padding:6px; border-radius:10px; background:#FCE4EC; border:1px solid #C62828;"><b style="font-size:1.2rem; color:#C62828;">{avg_c_rate}%</b><br><span style="font-size:0.75rem;">가입 이용률</span></div>', unsafe_allow_html=True)

                st.markdown("")

                # 데이터 테이블
                safe_display = safe_status[["monitoring_start_date", "memo", "agency_name", "contract_users",
                                             "registered_users", "joined_users", "registered_rate", "joined_rate"]].copy()
                safe_display.columns = ["관제시작일", "비고", "지자체명", "계약인원", "등록이용자", "가입이용자", "등록이용률(%)", "가입이용률(%)"]
                st.dataframe(safe_display, use_container_width=True, hide_index=True)

            # 파일 업로드 + 수동 편집
            with st.expander("📤 세이프 현황 업데이트 (엑셀 업로드 또는 수동 편집)"):
                upload_tab, manual_tab = st.tabs(["📁 엑셀 파일 업로드", "✏️ 수동 편집"])

                with upload_tab:
                    st.caption("계산용.xlsx 같은 엑셀 파일을 업로드하면 자동으로 반영됩니다.")
                    st.caption("필수 컬럼: 관제 등록 날짜, 구분, 지자체명, 계약 인원, 전체 등록 이용자, 전체 가입한 이용자")
                    uploaded_file = st.file_uploader("엑셀 파일 업로드 (.xlsx)", type=["xlsx"], key="safe_upload")

                    if uploaded_file is not None:
                        try:
                            import openpyxl
                            wb = openpyxl.load_workbook(uploaded_file, data_only=True)
                            # Sheet2 (정리된 데이터) 우선, 없으면 Sheet1
                            ws = wb["Sheet2"] if "Sheet2" in wb.sheetnames else wb[wb.sheetnames[0]]

                            # 헤더 찾기 (첫 번째 비어있지 않은 행)
                            header_row = None
                            for row in ws.iter_rows(min_row=1, max_row=5, values_only=False):
                                vals = [cell.value for cell in row]
                                if any(v is not None and str(v).strip() for v in vals):
                                    header_row = row
                                    break

                            if header_row:
                                headers = [str(cell.value).strip() if cell.value else f"col_{i}" for i, cell in enumerate(header_row)]
                                start_row = header_row[0].row + 1

                                rows_data = []
                                for row in ws.iter_rows(min_row=start_row, max_row=ws.max_row, values_only=True):
                                    vals = list(row)
                                    if len(vals) >= len(headers):
                                        vals = vals[:len(headers)]
                                    else:
                                        vals.extend([None] * (len(headers) - len(vals)))
                                    row_dict = dict(zip(headers, vals))
                                    # 지자체명이 있는 행만
                                    agency = None
                                    for k, v in row_dict.items():
                                        if v and str(v).strip() and any(kw in str(k) for kw in ["지자체", "기관"]):
                                            agency = str(v).strip()
                                            break
                                    if agency and agency != "합계" and agency != "총합":
                                        rows_data.append(row_dict)

                                if rows_data:
                                    preview_df = pd.DataFrame(rows_data)
                                    st.success(f"✅ {len(rows_data)}개 지자체 데이터 감지!")
                                    st.dataframe(preview_df, use_container_width=True, hide_index=True)

                                    if st.button("💾 이 데이터로 세이프 현황 업데이트", key="upload_safe_save"):
                                        try:
                                            from local_db import get_connection as _gc2
                                            _conn2 = _gc2()
                                            _conn2.execute("DELETE FROM safe_agency_status")

                                            for rd in rows_data:
                                                # 컬럼명 유연 매칭
                                                agency_name = ""
                                                start_date = ""
                                                memo = ""
                                                contract = 0
                                                registered = 0
                                                joined = 0

                                                for k, v in rd.items():
                                                    kl = str(k).replace(" ", "")
                                                    if "지자체" in kl or "기관" in kl:
                                                        agency_name = str(v).strip() if v else ""
                                                    elif "날짜" in kl or "시작" in kl or "등록날짜" in kl:
                                                        start_date = str(v).strip() if v else ""
                                                    elif "구분" in kl or "비고" in kl:
                                                        memo = str(v).strip() if v else ""
                                                    elif "계약" in kl and "인원" in kl:
                                                        contract = int(float(v)) if v else 0
                                                    elif "등록" in kl and "이용" in kl:
                                                        registered = int(float(v)) if v else 0
                                                    elif "가입" in kl and "이용" in kl:
                                                        joined = int(float(v)) if v else 0

                                                if not agency_name:
                                                    continue

                                                r_rate = round(registered / contract * 100, 1) if contract > 0 else 0
                                                j_rate = round(joined / contract * 100, 1) if contract > 0 else 0

                                                _conn2.execute("""
                                                    INSERT INTO safe_agency_status
                                                    (monitoring_start_date, memo, agency_name, contract_users, registered_users, joined_users, registered_rate, joined_rate)
                                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                                """, (start_date, memo, agency_name, contract, registered, joined, r_rate, j_rate))

                                            _conn2.commit()
                                            _conn2.close()
                                            st.success("✅ 세이프 현황이 업데이트되었습니다!")
                                            st.rerun()
                                        except Exception as e:
                                            st.error(f"저장 실패: {e}")
                                else:
                                    st.warning("지자체 데이터를 찾을 수 없습니다. 컬럼명을 확인해주세요.")
                        except Exception as e:
                            st.error(f"파일 읽기 실패: {e}")

                with manual_tab:
                    if not safe_status.empty:
                        safe_edit = safe_status[["monitoring_start_date", "memo", "agency_name", "contract_users",
                                                  "registered_users", "joined_users"]].copy()
                        safe_edit.columns = ["관제시작일", "비고", "지자체명", "계약인원", "등록이용자", "가입이용자"]
                    else:
                        safe_edit = pd.DataFrame(columns=["관제시작일", "비고", "지자체명", "계약인원", "등록이용자", "가입이용자"])

                    edited_safe = st.data_editor(
                        safe_edit,
                        use_container_width=True,
                        num_rows="dynamic",
                        key="safe_editor",
                    )

                    if st.button("💾 세이프 현황 저장", key="save_safe_status"):
                        try:
                            from local_db import get_connection as _gc2
                            _conn2 = _gc2()
                            _conn2.execute("DELETE FROM safe_agency_status")
                            for _, r in edited_safe.iterrows():
                                agency = str(r.get("지자체명", "")).strip()
                                if not agency:
                                    continue
                                target = int(r.get("계약인원", 0))
                                mon = int(r.get("등록이용자", 0))
                                call = int(r.get("가입이용자", 0))
                                m_rate = round(mon / target * 100, 1) if target > 0 else 0
                                c_rate = round(call / target * 100, 1) if target > 0 else 0
                                _conn2.execute("""
                                    INSERT INTO safe_agency_status
                                    (monitoring_start_date, memo, agency_name, contract_users, registered_users, joined_users, registered_rate, joined_rate)
                                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                                """, (str(r.get("관제시작일", "")), str(r.get("비고", "")), agency, target, mon, call, m_rate, c_rate))
                            _conn2.commit()
                            _conn2.close()
                            st.success("세이프 현황이 저장되었습니다!")
                            st.rerun()
                        except Exception as e:
                            st.error(f"저장 실패: {e}")

            st.markdown("")

        # 히트맵
        heatmap_df = cached_heatmap(data, selected_week)
        # 해당 주차 기준 계약 중인 지자체만 필터링 (부분 매칭 지원)
        active_list = get_active_agencies_for_week(selected_week)
        if active_list and not heatmap_df.empty:
            def _is_active_fuzzy(name):
                if name in active_list:
                    return True
                for a in active_list:
                    if name in a or a in name:
                        return True
                return False
            heatmap_df = heatmap_df[heatmap_df["지자체명"].apply(_is_active_fuzzy)]
    else:
        st.info("사이드바에서 주차를 선택해주세요.")


# ============================================================
# 👥 회원가입 & 이탈
# ============================================================
elif page == "👥 1.회원가입 & 이탈":
    st.markdown('<div class="section-header">👥 회원가입 및 앱 삭제자 현황</div>', unsafe_allow_html=True)
    p_start, p_end = page_week_range_selector("member", weeks)

    # 지자체별 회원가입 현황
    reg = data.get("registration", pd.DataFrame())
    wu = data.get("weekly_users", pd.DataFrame())

    if not reg.empty and "지자체명" in reg.columns:
        reg = reg.copy()

        # 가입률: 항상 이용자현황 시트 기반으로 계산 (Summary와 동일 기준)
        total_contract = int(reg["협약인원"].apply(safe_numeric).sum()) if "협약인원" in reg.columns else 0
        total_registered = int(reg["가입완료"].apply(safe_numeric).sum()) if "가입완료" in reg.columns else 0
        total_rate = round(total_registered / total_contract * 100, 1) if total_contract > 0 else 0

        total_incomplete = total_contract - total_registered

        kcols = st.columns(4)
        with kcols[0]:
            st.markdown(f'<div class="metric-card"><h3>총 협약인원</h3><h1>{total_contract:,}명</h1></div>', unsafe_allow_html=True)
        with kcols[1]:
            st.markdown(f'<div class="metric-card-green"><h3>가입완료</h3><h1>{total_registered:,}명</h1></div>', unsafe_allow_html=True)
        with kcols[2]:
            st.markdown(f'<div class="metric-card-red"><h3>미완료</h3><h1>{total_incomplete:,}명</h1></div>', unsafe_allow_html=True)
        with kcols[3]:
            st.markdown(f'<div class="metric-card-orange"><h3>전체 가입률</h3><h1>{total_rate}%</h1></div>', unsafe_allow_html=True)

        st.markdown("")

        # 주차별 대상자 수 대비 가입완료 비중 추이
        wu = data.get("weekly_users", pd.DataFrame())
        if not wu.empty and "주차" in wu.columns:
            wu_chart = wu.copy()
            # 기간 필터 적용
            if p_start:
                wu_chart = filter_by_week_range(wu_chart, "주차", p_start, p_end, weeks)
            else:
                wu_chart = wu_chart[wu_chart["주차"].astype(str).str.strip() >= "25-52"]
            # 필요한 컬럼 찾기 및 숫자 변환
            target_col = None
            reg_col = None
            for c in wu_chart.columns:
                cl = str(c).replace("\n", "").strip()
                if cl == "대상자수" or ("대상자" in cl and "수" in cl):
                    target_col = c
                elif c == "가입완료합계" or ("회원가입" in cl and "완료" in cl and "주간" not in cl and "비중" not in cl and "율" not in cl):
                    reg_col = c

            if target_col and reg_col:
                wu_chart[target_col] = wu_chart[target_col].apply(safe_numeric)
                wu_chart[reg_col] = wu_chart[reg_col].apply(safe_numeric)
                # 전체가입률: 항상 가입완료/대상자 직접 계산 (이용자현황 시트 기준과 통일)
                if False:  # Google Sheets 전체가입률 컬럼은 사용하지 않음
                    wu_chart["_rate"] = wu_chart["전체가입률"].apply(safe_numeric)
                    # 0인 행은 직접 계산으로 보충
                    mask = wu_chart["_rate"] == 0
                    wu_chart.loc[mask, "_rate"] = (wu_chart.loc[mask, reg_col] / wu_chart.loc[mask, target_col].replace(0, float("nan")) * 100).round(1).fillna(0)
                elif "대상자수" in wu_chart.columns:
                    wu_chart["대상자수"] = wu_chart["대상자수"].apply(safe_numeric)
                    wu_chart["_rate"] = (wu_chart[reg_col] / wu_chart["대상자수"].replace(0, float("nan")) * 100).round(1).fillna(0)
                else:
                    wu_chart["_rate"] = (wu_chart[reg_col] / wu_chart[target_col].replace(0, float("nan")) * 100).round(1).fillna(0)

                fig = make_subplots(specs=[[{"secondary_y": True}]])
                fig.add_trace(
                    go.Bar(x=wu_chart["주차"], y=wu_chart[target_col], name="대상자 수",
                           marker_color="#B0BEC5",
                           hovertemplate="<b>%{x}</b><br>대상자: %{y:,}명<extra></extra>"),
                    secondary_y=False,
                )
                fig.add_trace(
                    go.Bar(x=wu_chart["주차"], y=wu_chart[reg_col], name="가입완료",
                           marker_color="#00C853",
                           hovertemplate="<b>%{x}</b><br>가입완료: %{y:,}명<extra></extra>"),
                    secondary_y=False,
                )
                fig.add_trace(
                    go.Scatter(x=wu_chart["주차"], y=wu_chart["_rate"], name="가입률(%)",
                               mode="lines+markers", line=dict(color="#FF6F00", width=3),
                               marker=dict(size=5),
                               hovertemplate="<b>%{x}</b><br>가입률: %{y:.1f}%<extra></extra>"),
                    secondary_y=True,
                )

                fig.update_layout(
                    title="주차별 대상자 수 대비 회원가입 완료 비중",
                    height=420, margin=dict(t=40, b=30),
                    hovermode="x unified", barmode="group",
                    legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5),
                    xaxis=dict(type="category"),  # 주차를 카테고리로 (날짜 자동해석 방지)
                )
                fig.update_yaxes(title_text="명", secondary_y=False)
                fig.update_yaxes(title_text="가입률 (%)", secondary_y=True, range=[0, 110])
                st.plotly_chart(fig, use_container_width=True)

        tab1, tab2 = st.tabs(["지자체별 비중", "지자체별 가입률"])

        with tab1:
            # 지자체별 전체 회원 중 비중 (%)
            if "가입완료" in reg.columns:
                reg["전체비중"] = (reg["가입완료"] / total_registered * 100).round(1) if total_registered > 0 else 0
                reg["권역"] = reg["지자체명"].map(REGION_MAP).fillna("기타")

                fig = px.pie(reg, values="가입완료", names="지자체명",
                             title="지자체별 회원 비중",
                             color_discrete_sequence=px.colors.qualitative.Set3)
                fig.update_traces(
                    textposition="inside",
                    textinfo="label+percent",
                    hovertemplate="<b>%{label}</b><br>가입완료: %{value:,}명<br>비중: %{percent}<extra></extra>"
                )
                fig.update_layout(height=450, margin=dict(t=40, b=10))
                st.plotly_chart(fig, use_container_width=True)

        with tab2:
            # 지자체별 가입 완료율 바 차트
            if "완료율" in reg.columns:
                reg["권역"] = reg["지자체명"].map(REGION_MAP).fillna("기타")
                plot_municipality_bar(reg, "완료율", "지자체별 회원가입 완료율 (협약 대비 %)")

    else:
        st.info("이용자 현황 데이터가 없습니다.")

    # 주차별 앱 삭제의심자 + 전체삭제비중 추이
    deletion_raw = sheets.get("심혈관현황", pd.DataFrame())  # gid=981210016 (앱삭제 데이터 포함)
    if not deletion_raw.empty:
        st.markdown('<div class="section-header">앱 삭제 현황</div>', unsafe_allow_html=True)

        del_trend = deletion_raw.copy()
        week_col_d = None
        delete_col = None
        ratio_col = None
        for c in del_trend.columns:
            cl = str(c).replace("\n", "").strip()
            if "주차" in cl:
                week_col_d = c
            elif "앱" in cl and "삭제" in cl and "의심" in cl:
                delete_col = c
            elif "전체삭제비중" in cl or "전체" in cl and "삭제" in cl and "비중" in cl:
                ratio_col = c

        if week_col_d and (delete_col or ratio_col):
            # 기간 필터 적용
            if p_start:
                del_trend = filter_by_week_range(del_trend, week_col_d, p_start, p_end, weeks)
            if delete_col:
                del_trend[delete_col] = del_trend[delete_col].apply(safe_numeric)
            if ratio_col:
                del_trend[ratio_col] = del_trend[ratio_col].apply(safe_numeric)

            del_chart = shorten_dates_in_df(del_trend, week_col_d)

            fig = make_subplots(specs=[[{"secondary_y": True}]])
            if delete_col:
                fig.add_trace(go.Bar(
                    x=del_chart[week_col_d], y=del_chart[delete_col],
                    name="앱 삭제의심자", marker_color="#EF5350",
                    hovertemplate="%{y:,}명<extra>삭제의심자</extra>"
                ), secondary_y=False)
            if ratio_col:
                fig.add_trace(go.Scatter(
                    x=del_chart[week_col_d], y=del_chart[ratio_col],
                    name="전체삭제비중(%)", mode="lines+markers",
                    line=dict(color="#D32F2F", width=2),
                    hovertemplate="%{y:.1f}%<extra>삭제비중</extra>"
                ), secondary_y=True)
            fig.update_layout(
                title="주차별 앱 삭제의심자 및 전체삭제비중",
                height=380, hovermode="x unified",
                xaxis=dict(type="category"),
                legend=LEGEND_BELOW, margin=dict(t=40, b=70),
            )
            fig.update_yaxes(title_text="삭제의심자 (명)", secondary_y=False)
            fig.update_yaxes(title_text="전체삭제비중 (%)", secondary_y=True, range=[0, max(20, del_chart[ratio_col].max() * 1.3) if ratio_col else 20])
            st.plotly_chart(fig, use_container_width=True)

    # 지자체별 앱삭제율
    del_df = data.get("app_deletion", pd.DataFrame())
    if not del_df.empty and selected_week:
        st.markdown('<div class="section-header">지자체별 앱 삭제율</div>', unsafe_allow_html=True)
        week_del = del_df[del_df["주차"].astype(str).str.strip() == selected_week].copy()
        if not week_del.empty:
            # 앱삭제율이 0이고 다른 지표도 없는 지자체 제외 (계약 종료)
            # 히트맵에 있는 활성 지자체만 표시
            active_list = get_active_agencies()
            heatmap_muns = set()
            hm_df = cached_heatmap(data, selected_week)
            if not hm_df.empty:
                heatmap_muns = set(hm_df["지자체명"].tolist())
            if heatmap_muns:
                week_del = week_del[week_del["지자체명"].isin(heatmap_muns)]
            week_del["권역"] = week_del["지자체명"].map(REGION_MAP).fillna("기타")
            if not week_del.empty:
                plot_municipality_bar(week_del, "앱삭제율", f"{selected_week} 주차 지자체별 앱삭제율 (%)")


# ============================================================
# 🖐 안부확인
# ============================================================
elif page == "🖐 2.안부확인":
    st.markdown('<div class="section-header">🖐 안부확인 현황</div>', unsafe_allow_html=True)

    # DB에서 안부확인 raw 데이터 조회
    safety_db = data.get("dashboard_data", {}).get("db_safety_check", pd.DataFrame()) if isinstance(data.get("dashboard_data"), dict) else pd.DataFrame()
    if safety_db.empty:
        try:
            safety_db = get_db_data("raw_safety_check")
        except:
            safety_db = pd.DataFrame()

    # DB 데이터가 있으면 일별 지표 계산
    if not safety_db.empty and "date" in safety_db.columns:
        safety_db = safety_db.copy()

        # 📅 날짜 기간 선택기
        all_dates = sorted(safety_db["date"].unique())
        # 기본 시작: 2025-12-22 (25-52주차 시작일 근사) 또는 최근 60일
        default_start_date = "2026-01-01"
        if default_start_date in all_dates:
            default_start_idx = all_dates.index(default_start_date)
        else:
            # 가장 가까운 날짜 찾기
            default_start_idx = 0
            for i, d in enumerate(all_dates):
                if d >= default_start_date:
                    default_start_idx = i
                    break

        with st.expander("📅 기간 설정 (펼쳐서 변경)", expanded=False):
            dc1, dc2 = st.columns(2)
            with dc1:
                date_start = st.selectbox("시작일", all_dates, index=default_start_idx, key="safety_date_start")
            with dc2:
                start_idx = all_dates.index(date_start) if date_start in all_dates else 0
                end_options = all_dates[start_idx:]
                date_end = st.selectbox("종료일", end_options, index=len(end_options)-1, key="safety_date_end")
            st.caption(f"선택 기간: {date_start} ~ {date_end}")

        # 선택 기간으로 필터링
        safety_db = safety_db[(safety_db["date"] >= date_start) & (safety_db["date"] <= date_end)]

        # 일별 전체 합산
        agg_cols = {
            "alarm_send_count": "sum", "confirm_count": "sum",
            "target_user_count": "sum", "complete_user_count": "sum",
            "impossible_user_count": "sum", "detect_motion_count": "sum",
            "ai_care_generate_count": "sum", "ai_care_response_count": "sum",
            "call_generate_count": "sum", "call_response_count": "sum",
            "uncheck_48hr_user_count": "sum", "uncheck_48hr_target_count": "sum",
        }
        valid_agg = {k: v for k, v in agg_cols.items() if k in safety_db.columns}
        daily = safety_db.groupby("date").agg(valid_agg).reset_index().sort_values("date")
        # X축 날짜 짧게
        daily = shorten_dates_in_df(daily, "date")

        # 비율 지표: Google Sheets 수식 결과를 우선 사용 (DB 자체 계산보다 정확)
        cd_sheets = data.get("checkin_daily", pd.DataFrame())
        rate_cols = ["안부미확인률", "48시간미확인률", "안부체크응답률", "콜응답률", "AI케어응답률",
                     "안부체크비중", "동작감지비중", "AI케어비중", "안부확인콜비중", "안부체크율"]

        if not cd_sheets.empty and "날짜" in cd_sheets.columns:
            # Google Sheets 날짜를 짧은 형식으로 변환해서 매칭
            cd_match = cd_sheets.copy()
            cd_match = shorten_dates_in_df(cd_match, "날짜")
            # daily의 date와 매칭
            for rc in rate_cols:
                if rc in cd_match.columns:
                    rate_map = dict(zip(cd_match["날짜"], cd_match[rc]))
                    mapped_col = rc if rc != "콜응답률" else "안부확인콜응답률"
                    daily[mapped_col] = daily["date"].map(rate_map).fillna(0)

        # Google Sheets에 없는 컬럼은 DB에서 fallback 계산
        t = daily["target_user_count"].replace(0, float("nan"))
        if "안부미확인률" not in daily.columns or daily["안부미확인률"].sum() == 0:
            daily["안부미확인률"] = (daily.get("impossible_user_count", 0) / t * 100).round(1).fillna(0)
        if "48시간미확인률" not in daily.columns or daily["48시간미확인률"].sum() == 0:
            daily["48시간미확인률"] = (daily.get("uncheck_48hr_user_count", 0) / t * 100).round(1).fillna(0)
        if "안부체크비중" not in daily.columns or daily["안부체크비중"].sum() == 0:
            daily["안부체크비중"] = (daily["confirm_count"] / daily["complete_user_count"].replace(0, float("nan")) * 100).round(1).fillna(0)
        if "동작감지비중" not in daily.columns or daily["동작감지비중"].sum() == 0:
            daily["동작감지비중"] = (daily.get("detect_motion_count", 0) / daily["complete_user_count"].replace(0, float("nan")) * 100).round(1).fillna(0)
        if "AI케어비중" not in daily.columns or daily["AI케어비중"].sum() == 0:
            daily["AI케어비중"] = (daily["ai_care_response_count"] / daily["complete_user_count"].replace(0, float("nan")) * 100).round(1).fillna(0)
        if "안부확인콜비중" not in daily.columns or daily["안부확인콜비중"].sum() == 0:
            daily["안부확인콜비중"] = (daily["call_response_count"] / daily["complete_user_count"].replace(0, float("nan")) * 100).round(1).fillna(0)
        # 안부체크응답률: off 대상자 반영 (2025-11-15 이후)
        # off 대상자 = 안부체크 발송을 끈 이용자 (분모에서 제외)
        from sheets_data import get_check_off_users as _get_off
        _off_total = sum(_get_off(sheets).values()) if sheets else 0
        if "안부체크응답률" not in daily.columns or daily["안부체크응답률"].sum() == 0:
            _send = daily["alarm_send_count"].copy()
            # 2025-11-15 이후 데이터만 off 대상자 차감
            _off_mask = daily["date"] >= "2025-11-15"
            _adjusted_send = _send.copy()
            _adjusted_send[_off_mask] = _send[_off_mask] - _off_total
            _adjusted_send = _adjusted_send.replace(0, float("nan")).clip(lower=1)
            daily["안부체크응답률"] = (daily["confirm_count"] / _adjusted_send * 100).round(1).fillna(0)
        else:
            # Google Sheets 값이 있어도 off 반영으로 재계산
            _send = daily.get("alarm_send_count", None)
            if _send is not None and _send.sum() > 0:
                _off_mask = daily["date"] >= "2025-11-15"
                _adjusted_send = _send.copy()
                _adjusted_send[_off_mask] = _send[_off_mask] - _off_total
                _adjusted_send = _adjusted_send.replace(0, float("nan")).clip(lower=1)
                daily["안부체크응답률"] = (daily["confirm_count"] / _adjusted_send * 100).round(1).fillna(0)
        if "AI케어응답률" not in daily.columns or daily["AI케어응답률"].sum() == 0:
            daily["AI케어응답률"] = (daily["ai_care_response_count"] / daily["ai_care_generate_count"].replace(0, float("nan")) * 100).round(1).fillna(0)
        if "안부확인콜응답률" not in daily.columns or daily["안부확인콜응답률"].sum() == 0:
            daily["안부확인콜응답률"] = (daily["call_response_count"] / daily["call_generate_count"].replace(0, float("nan")) * 100).round(1).fillna(0)

        # 4/19, 4/20 데이터 제외 (미완료 데이터)
        daily = daily[~daily["date"].isin(["26-04-19", "26-04-20"])]

        st.caption(f"데이터 기간: {daily['date'].min()} ~ {daily['date'].max()}")

        tab1, tab2, tab3, tab4, tab5, tab7, tab8 = st.tabs([
            "완료현황", "안부확인 비중", "안부확인율",
            "AI케어 알람", "안부확인콜",
            "📊 일자별 데이터", "📊 지자체별 데이터"
        ])

        # ── Tab 1: 일별 안부확인 완료현황 (안부미확인률, 48시간미확인률)
        with tab1:
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(go.Bar(x=daily["date"], y=daily["complete_user_count"], name="완료자",
                                 marker_color="#00C853",
                                 hovertemplate="%{y:,}명<extra>완료자</extra>"), secondary_y=False)
            fig.add_trace(go.Bar(x=daily["date"], y=daily.get("impossible_user_count", 0), name="미확인자",
                                 marker_color="#FF8A80",
                                 hovertemplate="%{y:,}명<extra>미확인자</extra>"), secondary_y=False)
            fig.add_trace(go.Scatter(x=daily["date"], y=daily["안부미확인률"], name="안부미확인률(%)",
                                     mode="lines", line=dict(color="#FF4B4B", width=2, dash="dot"),
                                     hovertemplate="%{y:.1f}%<extra>안부미확인률</extra>"), secondary_y=True)
            fig.add_trace(go.Scatter(x=daily["date"], y=daily["48시간미확인률"], name="48시간미확인률(%)",
                                     mode="lines", line=dict(color="#D32F2F", width=2),
                                     hovertemplate="%{y:.1f}%<extra>48시간미확인률</extra>"), secondary_y=True)
            fig.update_layout(title="일별 안부확인 완료현황", height=420, hovermode="x unified",
                              barmode="stack", xaxis=dict(type="category"),
                              legend=LEGEND_BELOW)
            fig.update_yaxes(title_text="명", secondary_y=False)
            fig.update_yaxes(title_text="%", secondary_y=True, range=[0, max(30, daily["안부미확인률"].max() * 1.3)])
            st.plotly_chart(fig, use_container_width=True)

        # ── Tab 2: 일별 안부확인 비중 (4개 비중 Area chart)
        with tab2:
            fig = go.Figure()
            fig.add_trace(go.Scatter(x=daily["date"], y=daily["안부체크비중"], name="안부체크",
                                     fill="tozeroy", mode="lines", line=dict(color="#2F5496"),
                                     hovertemplate="%{y:.1f}%<extra>안부체크</extra>"))
            fig.add_trace(go.Scatter(x=daily["date"], y=daily["동작감지비중"], name="동작감지/서비스이용",
                                     fill="tozeroy", mode="lines", line=dict(color="#00897B"),
                                     hovertemplate="%{y:.1f}%<extra>동작감지</extra>"))
            fig.add_trace(go.Scatter(x=daily["date"], y=daily["AI케어비중"], name="AI케어 알람",
                                     fill="tozeroy", mode="lines", line=dict(color="#7B1FA2"),
                                     hovertemplate="%{y:.1f}%<extra>AI케어</extra>"))
            fig.add_trace(go.Scatter(x=daily["date"], y=daily["안부확인콜비중"], name="안부확인콜",
                                     fill="tozeroy", mode="lines", line=dict(color="#E91E63"),
                                     hovertemplate="%{y:.1f}%<extra>안부확인콜</extra>"))
            fig.update_layout(title="일별 안부확인 비중 (완료자 대비 %)", height=400, hovermode="x unified",
                              xaxis=dict(type="category"),
                              legend=LEGEND_BELOW)
            st.plotly_chart(fig, use_container_width=True)

        # ── Tab 3: 일별 안부확인율 + 지자체별 안부확인율 (주간)
        with tab3:
            # 일별 안부확인율 = complete_user_count / target_user_count × 100
            if "complete_user_count" in daily.columns and "target_user_count" in daily.columns:
                daily["안부확인율"] = (
                    daily["complete_user_count"]
                    / daily["target_user_count"].replace(0, float("nan")) * 100
                ).round(1).fillna(0)
                fig_cr = go.Figure()
                fig_cr.add_trace(go.Scatter(
                    x=daily["date"], y=daily["안부확인율"],
                    mode="lines+markers", name="안부확인율",
                    line=dict(color="#2F5496", width=2.5),
                    fill="tozeroy", fillcolor="rgba(47,84,150,0.08)",
                    hovertemplate="<b>%{x}</b><br>안부확인율: %{y:.1f}%<extra></extra>"
                ))
                # x축 틱 수 제한 (최대 24개)
                _cr_dates = daily["date"].tolist()
                _step = max(1, len(_cr_dates) // 24)
                _tick_vals = _cr_dates[::_step]
                fig_cr.update_layout(
                    title="일별 안부확인율 (완료자 / 대상자)",
                    height=350, hovermode="x unified",
                    xaxis=dict(
                        type="category", title="",
                        tickmode="array", tickvals=_tick_vals,
                        tickangle=-45, tickfont=dict(size=11),
                    ),
                    yaxis=dict(title="안부확인율 (%)", range=[0, 100]),
                    margin=dict(t=40, b=80),
                )
                st.plotly_chart(fig_cr, use_container_width=True)

            # 지자체별 안부확인율 (주간 Google Sheets 데이터) — 바로 아래 연결
            cr_mun = data.get("checkin_municipality_rate", pd.DataFrame())
            if not cr_mun.empty and "안부확인율" in cr_mun.columns:
                cr_show = cr_mun[cr_mun["안부확인율"].notna() & (cr_mun["안부확인율"] > 0)].copy()
                cr_show = cr_show.sort_values("시작일")
                # 최근 16주만 기본 표시
                recent_dates = sorted(cr_show["시작일"].unique())[-16:]
                cr_show = cr_show[cr_show["시작일"].isin(recent_dates)]
                cr_show = week_label_df(cr_show, "시작일")
                if not cr_show.empty:
                    fig_mun = px.line(
                        cr_show, x="시작일", y="안부확인율", color="지자체명",
                        markers=True,
                        color_discrete_sequence=px.colors.qualitative.Set2,
                    )
                    fig_mun.update_layout(
                        title="지자체별 안부확인율 주간 추이 (최근 16주)",
                        height=420, hovermode="x unified",
                        xaxis=dict(
                            type="category", title="",
                            tickangle=-45, tickfont=dict(size=11),
                        ),
                        yaxis=dict(title="안부확인율 (%)", range=[0, 100]),
                        legend=LEGEND_BELOW, margin=dict(t=40, b=100),
                    )
                    fig_mun.update_traces(
                        hovertemplate="<b>%{x}</b><br>%{y:.1f}%<extra>%{fullData.name}</extra>"
                    )
                    st.plotly_chart(fig_mun, use_container_width=True)
                else:
                    st.info("안부확인율 데이터가 없습니다.")
            else:
                st.info("지자체별 안부확인율 데이터가 없습니다. (안부확인지자체 시트 확인 필요)")

        # ── Tab 4: 일별 AI케어 알람 응답 (발송수, 응답자수, 응답률)
        with tab4:
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(go.Bar(x=daily["date"], y=daily["ai_care_generate_count"], name="AI케어 발송수",
                                 marker_color="#CE93D8",
                                 hovertemplate="%{y:,}건<extra>발송수</extra>"), secondary_y=False)
            fig.add_trace(go.Bar(x=daily["date"], y=daily["ai_care_response_count"], name="AI케어 응답자수",
                                 marker_color="#7B1FA2",
                                 hovertemplate="%{y:,}명<extra>응답자</extra>"), secondary_y=False)
            fig.add_trace(go.Scatter(x=daily["date"], y=daily["AI케어응답률"], name="AI케어 응답률(%)",
                                     mode="lines+markers", line=dict(color="#FF6F00", width=2),
                                     hovertemplate="%{y:.1f}%<extra>응답률</extra>"), secondary_y=True)
            fig.update_layout(title="일별 AI케어 알람 응답", height=420, hovermode="x unified",
                              barmode="group", xaxis=dict(type="category"),
                              legend=LEGEND_BELOW)
            fig.update_yaxes(title_text="건/명", secondary_y=False)
            fig.update_yaxes(title_text="응답률(%)", secondary_y=True, range=[0, 110])
            st.plotly_chart(fig, use_container_width=True)

        # ── Tab 5: 일별 안부확인콜 (발송수, 응답자수, 응답률)
        with tab5:
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(go.Bar(x=daily["date"], y=daily["call_generate_count"], name="콜 발송수",
                                 marker_color="#F48FB1",
                                 hovertemplate="%{y:,}건<extra>발송수</extra>"), secondary_y=False)
            fig.add_trace(go.Bar(x=daily["date"], y=daily["call_response_count"], name="콜 응답자수",
                                 marker_color="#E91E63",
                                 hovertemplate="%{y:,}명<extra>응답자</extra>"), secondary_y=False)
            fig.add_trace(go.Scatter(x=daily["date"], y=daily["안부확인콜응답률"], name="콜 응답률(%)",
                                     mode="lines+markers", line=dict(color="#FF6F00", width=2),
                                     hovertemplate="%{y:.1f}%<extra>응답률</extra>"), secondary_y=True)
            fig.update_layout(title="일별 안부확인콜", height=420, hovermode="x unified",
                              barmode="group", xaxis=dict(type="category"),
                              legend=LEGEND_BELOW)
            fig.update_yaxes(title_text="건/명", secondary_y=False)
            fig.update_yaxes(title_text="응답률(%)", secondary_y=True, range=[0, 110])
            st.plotly_chart(fig, use_container_width=True)

        # ── Tab 7: 일자별 전체 데이터 테이블 (Google Sheets gid=261480368 형태)
        with tab7:
            st.markdown("**일자별 안부확인 전체 데이터** (Google Sheets `복약확인알림(전체)` 시트와 동일)")

            # 일자별 기간 필터
            date_list = sorted(daily["date"].unique())
            if len(date_list) > 1:
                d_col1, d_col2 = st.columns(2)
                with d_col1:
                    date_from = st.selectbox("시작일", date_list, index=max(0, len(date_list)-14), key="daily_from")
                with d_col2:
                    date_to = st.selectbox("종료일", date_list, index=len(date_list)-1, key="daily_to")
                daily_filtered = daily[(daily["date"] >= date_from) & (daily["date"] <= date_to)].copy()
            else:
                daily_filtered = daily.copy()

            # 표시 컬럼 정리
            display_cols = {
                "date": "날짜",
                "target_user_count": "전체 대상자",
                "complete_user_count": "안부확인 완료자",
                "impossible_user_count": "안부미확인자",
                "uncheck_48hr_user_count": "48시간 미확인",
                "confirm_count": "①안부체크 응답자",
                "detect_motion_count": "②동작감지 이용자",
                "ai_care_response_count": "③AI케어 응답자",
                "call_response_count": "④안부확인콜 응답자",
                "alarm_send_count": "안부체크 발송수",
                "ai_care_generate_count": "AI케어 발송수",
                "call_generate_count": "안부확인콜 발송수",
                "안부미확인률": "안부미확인률(%)",
                "48시간미확인률": "48시간미확인률(%)",
                "안부체크응답률": "안부체크응답률(%)",
                "AI케어응답률": "AI케어응답률(%)",
                "안부확인콜응답률": "안부확인콜응답률(%)",
            }
            available_display = {k: v for k, v in display_cols.items() if k in daily_filtered.columns}
            daily_display = daily_filtered[list(available_display.keys())].rename(columns=available_display)
            daily_display = daily_display.sort_values("날짜", ascending=False)

            st.dataframe(
                daily_display,
                use_container_width=True,
                height=min(600, len(daily_display) * 35 + 50),
                column_config={
                    "안부미확인률(%)": st.column_config.NumberColumn(format="%.1f%%"),
                    "48시간미확인률(%)": st.column_config.NumberColumn(format="%.1f%%"),
                    "안부체크응답률(%)": st.column_config.NumberColumn(format="%.1f%%"),
                    "AI케어응답률(%)": st.column_config.NumberColumn(format="%.1f%%"),
                    "안부확인콜응답률(%)": st.column_config.NumberColumn(format="%.1f%%"),
                },
            )
            st.caption(f"총 {len(daily_display)}일 데이터")

        # ── Tab 8: 지자체별 데이터 테이블
        with tab8:
            st.markdown("**지자체별 안부확인 데이터** (Google Sheets `복약확인알림(전체지자체)` 시트와 동일)")

            # 날짜 선택
            all_dates = sorted(safety_db["date"].unique())
            if all_dates:
                selected_date = st.selectbox("날짜 선택", list(reversed(all_dates)), index=0, key="mun_date")
                mun_day = safety_db[safety_db["date"] == selected_date].copy()

                if not mun_day.empty:
                    # 지표 계산
                    t = mun_day["target_user_count"].replace(0, float("nan"))
                    mun_day["안부체크율(%)"] = (mun_day["confirm_count"] / mun_day["alarm_send_count"].replace(0, float("nan")) * 100).round(1).fillna(0)
                    mun_day["안부미확인률(%)"] = (mun_day["impossible_user_count"] / t * 100).round(1).fillna(0)
                    mun_day["48시간미확인률(%)"] = (mun_day.get("uncheck_48hr_user_count", 0) / t * 100).round(1).fillna(0)

                    # 표시용 정리
                    mun_display_cols = {
                        "agency_name": "지자체명",
                        "target_user_count": "대상자수",
                        "complete_user_count": "완료자수",
                        "impossible_user_count": "미확인자",
                        "confirm_count": "안부체크응답",
                        "detect_motion_count": "동작감지",
                        "ai_care_response_count": "AI케어응답",
                        "call_response_count": "콜응답",
                        "alarm_send_count": "체크발송수",
                        "ai_care_generate_count": "AI케어발송",
                        "call_generate_count": "콜발송",
                        "uncheck_48hr_user_count": "48시간미확인",
                        "안부체크율(%)": "안부체크율(%)",
                        "안부미확인률(%)": "안부미확인률(%)",
                        "48시간미확인률(%)": "48시간미확인률(%)",
                    }
                    available_mun = {k: v for k, v in mun_display_cols.items() if k in mun_day.columns}
                    mun_display = mun_day[list(available_mun.keys())].rename(columns=available_mun)
                    # 발송수 0인 지자체 제외
                    if "체크발송수" in mun_display.columns:
                        mun_display = mun_display[mun_display["체크발송수"] > 0]
                    mun_display = mun_display.sort_values("대상자수", ascending=False)

                    st.dataframe(
                        mun_display,
                        use_container_width=True,
                        height=min(600, len(mun_display) * 35 + 50),
                        column_config={
                            "안부체크율(%)": st.column_config.NumberColumn(format="%.1f%%"),
                            "안부미확인률(%)": st.column_config.NumberColumn(format="%.1f%%"),
                            "48시간미확인률(%)": st.column_config.NumberColumn(format="%.1f%%"),
                        },
                    )
                    st.caption(f"{selected_date} 기준 {len(mun_display)}개 지자체")

    else:
        # DB 데이터 없으면 Google Sheets 데이터 사용 (기존 방식)
        cr_base = data.get("checkin_municipality_rate", pd.DataFrame())
        if not cr_base.empty and "안부확인율" in cr_base.columns:
            cr_all = cr_base[cr_base["안부확인율"].notna() & (cr_base["안부확인율"] > 0)].copy()
            cr_all = cr_all.sort_values("시작일")
            cr_all = week_label_df(cr_all, "시작일")
            if not cr_all.empty:
                # ① 전체 평균 추이
                avg_cr = cr_all.groupby("시작일")["안부확인율"].mean().reset_index()
                fig = px.line(avg_cr, x="시작일", y="안부확인율", markers=True,
                              color_discrete_sequence=["#2F5496"])
                fig.update_layout(title="안부확인율 추이 (전체 평균)", height=350,
                                  hovermode="x unified", xaxis=dict(type="category"),
                                  yaxis=dict(title="안부확인율 (%)", range=[0, 100]),
                                  margin=dict(t=40, b=30))
                st.plotly_chart(fig, use_container_width=True)

                # ② 지자체별 추이 (바로 아래)
                fig2 = px.line(cr_all, x="시작일", y="안부확인율", color="지자체명",
                               markers=True,
                               color_discrete_sequence=px.colors.qualitative.Set2)
                fig2.update_layout(title="지자체별 안부확인율 추이", height=420,
                                   hovermode="x unified", xaxis=dict(type="category"),
                                   yaxis=dict(title="안부확인율 (%)", range=[0, 100]),
                                   legend=LEGEND_BELOW, margin=dict(t=40, b=80))
                fig2.update_traces(
                    hovertemplate="<b>%{x}</b><br>%{y:.1f}%<extra>%{fullData.name}</extra>"
                )
                st.plotly_chart(fig2, use_container_width=True)
            else:
                st.info("안부확인율 데이터가 없습니다.")
        else:
            st.info("안부확인 데이터가 없습니다. '📥 데이터 입력'에서 safetyCheck 데이터를 붙여넣어주세요.")


# ============================================================
# ❤ 심혈관체크
# ============================================================
elif page == "❤ 5.심혈관체크":
    st.markdown('<div class="section-header">❤ 심혈관체크</div>', unsafe_allow_html=True)

    p_start, p_end = page_week_range_selector("cardio", weeks)

    tab1, tab2 = st.tabs(["이용자수 추이", "검사횟수 추이"])

    with tab1:
        cardio_users = data.get("weekly_심혈관이용자", pd.DataFrame())
        cardio_user_raw = sheets.get("심혈관이용자", pd.DataFrame())
        if not cardio_user_raw.empty:
            cu = cardio_user_raw.copy()
            _wc, _sum_col, _rc = None, None, None
            for c in cu.columns:
                cl = str(c).replace("\n", "").strip()
                if "주차" in cl and _wc is None: _wc = c
                elif ("이용자합계" in cl or ("합계" in cl and "이용자" in cl)) and _sum_col is None: _sum_col = c
                elif ("전체이용비중" in cl or "이용비중" in cl) and _rc is None: _rc = c
            # C열 합계 우선
            cardio_total_c = data.get("total_심혈관이용자", pd.DataFrame())
            if _wc:
                cu = filter_by_week_range(cu, _wc, p_start, p_end, weeks)
                cu = shorten_dates_in_df(cu, _wc)
                if not cardio_total_c.empty:
                    ct = filter_by_week_range(cardio_total_c, "주차", p_start, p_end, weeks)
                    ct = shorten_dates_in_df(ct, "주차")
                    cu = cu.copy()
                    ct_map = dict(zip(ct["주차"], ct["값"].apply(safe_numeric)))
                    cu["_bar"] = cu[_wc].map(ct_map).fillna(cu[_sum_col].apply(safe_numeric) if _sum_col else 0)
                    bar_col_use = "_bar"
                else:
                    bar_col_use = _sum_col
                if bar_col_use:
                    plot_bar_rate_dual(cu, _wc, bar_col_use, "이용자수", "#EF5350",
                                       _rc, "전체이용비중", "#FF6F00",
                                       "심혈관체크 이용자수 + 전체이용비중")
            cf = filter_by_week_range(cardio_users, "주차", p_start, p_end, weeks) if not cardio_users.empty else pd.DataFrame()
            if not cf.empty:
                # 이용자수 → 이용률 (주차별 지자체 가입완료 회원 대비 %)
                _wrm = data.get("weekly_registered_by_mun", pd.DataFrame())
                if not _wrm.empty:
                    _reg_week_map = {(str(r["주차"]).strip(), str(r["지자체명"]).strip()): safe_numeric(r["가입완료"])
                                     for _, r in _wrm.iterrows()}
                    cf = cf.copy()
                    def _cardio_rate(r):
                        denom = _reg_week_map.get((str(r["주차"]).strip(), str(r["지자체명"]).strip()), 0)
                        return round(r["값"] / denom * 100, 1) if denom > 0 else 0
                    cf["값"] = cf.apply(_cardio_rate, axis=1)
                    plot_municipality_lines(cf, "지자체별 심혈관체크 이용률 추이 (가입회원 대비 %)", metric_label="이용률(%)")
                else:
                    plot_municipality_lines(cf, "지자체별 심혈관체크 이용자 추이", metric_label="이용자수")
            # ── 지자체별 이용자비중 추이 (AI~BK열) ──────────────────────────
            st.markdown("---")
            mrt_cardio = extract_mun_ratio_trend(cardio_user_raw)
            if not mrt_cardio.empty:
                mrt_cardio = filter_by_week_range(mrt_cardio, "주차", p_start, p_end, weeks)
                _active_c = mrt_cardio.groupby("지자체명")["값"].sum()
                _active_c = _active_c[_active_c > 0].index.tolist()
                mrt_cardio = mrt_cardio[mrt_cardio["지자체명"].isin(_active_c)]
                if not mrt_cardio.empty:
                    plot_municipality_lines(mrt_cardio, "지자체별 심혈관체크 이용자비중 추이 (%)", metric_label="이용자비중(%)")
        else:
            st.info("심혈관 이용자 데이터가 없습니다.")

    with tab2:
        cardio_exam = data.get("weekly_심혈관검사", pd.DataFrame())
        cardio_exam_raw = sheets.get("심혈관검사횟수", pd.DataFrame())
        if not cardio_exam_raw.empty:
            ce = cardio_exam_raw.copy()
            _wc, _sum_col, _awc = None, None, None
            for c in ce.columns:
                cl = str(c).replace("\n", "").strip()
                if "주차" in cl and _wc is None: _wc = c
                elif "합계" in cl and _sum_col is None: _sum_col = c
                elif "1인" in cl and "주평균" in cl and _awc is None: _awc = c
            cardio_exam_total_c = data.get("total_심혈관검사", pd.DataFrame())
            if _wc:
                ce = filter_by_week_range(ce, _wc, p_start, p_end, weeks)
                ce = shorten_dates_in_df(ce, _wc)
                if not cardio_exam_total_c.empty:
                    cet = filter_by_week_range(cardio_exam_total_c, "주차", p_start, p_end, weeks)
                    cet = shorten_dates_in_df(cet, "주차")
                    ct_map = dict(zip(cet["주차"], cet["값"].apply(safe_numeric)))
                    ce["_bar"] = ce[_wc].map(ct_map).fillna(ce[_sum_col].apply(safe_numeric) if _sum_col else 0)
                    bar_col_use = "_bar"
                else:
                    bar_col_use = _sum_col
                if bar_col_use:
                    plot_bar_rate_dual(ce, _wc, bar_col_use, "검사횟수", "#EF5350",
                                       _awc, "1인 주평균", "#455A64",
                                       "심혈관 검사횟수 + 1인 주평균", bar_unit="회", line_unit="회")
            cf = filter_by_week_range(cardio_exam, "주차", p_start, p_end, weeks) if not cardio_exam.empty else pd.DataFrame()
            if not cf.empty:
                plot_municipality_lines(cf, "지자체별 심혈관 검사횟수 추이", metric_label="검사횟수")
        else:
            st.info("심혈관 검사횟수 데이터가 없습니다.")


# ============================================================
# 💊 복약관리
# ============================================================
elif page == "💊 7.복약관리":
    st.markdown('<div class="section-header">💊 복약관리</div>', unsafe_allow_html=True)
    p_start, p_end = page_week_range_selector("med", weeks)

    # 데이터 준비
    med_users = data.get("weekly_복약등록회원", pd.DataFrame())
    med_count = data.get("weekly_복약등록건수", pd.DataFrame())
    reg = data.get("registration", pd.DataFrame())

    # 전체 가입완료자 수 (비율 계산용)
    total_registered = 0
    if not reg.empty and "가입완료" in reg.columns:
        total_registered = int(reg["가입완료"].apply(safe_numeric).sum())

    tab1, tab2, tab3, tab4 = st.tabs(["활성 이용자 복약 이용자수", "활성 이용자 복약 등록건수", "지자체별 비중", "상세 데이터"])

    with tab1:
        # 활성 이용자 복약 이용자수 — Google Sheets 원본 시트에서 직접 (합계+비율 컬럼 활용)
        med_raw = sheets.get("복약등록회원", pd.DataFrame())
        if not med_raw.empty:
            mr = med_raw.copy()
            # 컬럼 찾기
            _wc, _sum_col, _ratio_col = None, None, None
            for c in mr.columns:
                cl = str(c).replace("\n", "").strip()
                if "주차" in cl: _wc = c
                elif "이용자" in cl and "합계" in cl: _sum_col = c
                elif cl == "비율" or ("비율" in cl and "WoW" not in cl and "1인" not in cl): _ratio_col = c

            if _wc and _sum_col:
                mr[_sum_col] = mr[_sum_col].apply(safe_numeric)
                if _ratio_col:
                    mr[_ratio_col] = mr[_ratio_col].apply(safe_numeric)

                # 기간 필터
                mr = filter_by_week_range(mr, _wc, p_start, p_end, weeks)
                mr = shorten_dates_in_df(mr, _wc)
                mr = mr[mr[_wc].astype(str).str.strip() != ""]

                fig = make_subplots(specs=[[{"secondary_y": True}]])
                fig.add_trace(go.Bar(
                    x=mr[_wc], y=mr[_sum_col],
                    name="이용자 수 합계",
                    marker_color="#424242",
                    text=mr[_sum_col].apply(lambda x: f"{x:,.0f}"),
                    textposition="outside", textfont=dict(size=13),
                    hovertemplate="<b>%{x}</b><br>이용자수: %{y:,}명<extra></extra>"
                ), secondary_y=False)
                if _ratio_col:
                    fig.add_trace(go.Scatter(
                        x=mr[_wc], y=mr[_ratio_col],
                        name="비율",
                        mode="lines+markers+text",
                        line=dict(color="#FF6F00", width=2),
                        text=mr[_ratio_col].apply(lambda x: f"{x:.0f}%"),
                        textposition="top center", textfont=dict(size=13, color="#FF6F00"),
                        hovertemplate="<b>%{x}</b><br>비율: %{y:.1f}%<extra></extra>"
                    ), secondary_y=True)
                fig.update_layout(
                    title="활성 이용자 복약 이용자수",
                    height=450, hovermode="x unified",
                    xaxis=dict(type="category"),
                    legend=LEGEND_BELOW, margin=dict(t=40, b=70),
                    bargap=0.3,
                )
                fig.update_yaxes(title_text="이용자수 (명)", secondary_y=False)
                if _ratio_col:
                    max_ratio = mr[_ratio_col].max()
                    fig.update_yaxes(title_text="비율 (%)", secondary_y=True,
                                     range=[0, max(25, max_ratio * 1.3) if max_ratio > 0 else 25])
                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("복약 등록 회원수 시트에서 합계/비율 컬럼을 찾을 수 없습니다.")
        else:
            st.info("복약 등록 회원수 데이터가 없습니다.")

    with tab2:
        med_count_raw = sheets.get("복약등록건수", pd.DataFrame())
        med_count_c = data.get("total_복약등록건수", pd.DataFrame())
        if not med_count_raw.empty:
            mc_raw = med_count_raw.copy()
            _wc, _sum_col, _rc = None, None, None
            for c in mc_raw.columns:
                cl = str(c).replace("\n", "").strip()
                if "주차" in cl and _wc is None: _wc = c
                elif "합계" in cl and _sum_col is None: _sum_col = c
                elif ("전체이용비중" in cl or "이용비중" in cl) and _rc is None: _rc = c
            if _wc:
                mc_raw = filter_by_week_range(mc_raw, _wc, p_start, p_end, weeks)
                mc_raw = shorten_dates_in_df(mc_raw, _wc)
                if not med_count_c.empty:
                    mct = filter_by_week_range(med_count_c, "주차", p_start, p_end, weeks)
                    mct = shorten_dates_in_df(mct, "주차")
                    ct_map = dict(zip(mct["주차"], mct["값"].apply(safe_numeric)))
                    mc_raw["_bar"] = mc_raw[_wc].map(ct_map).fillna(mc_raw[_sum_col].apply(safe_numeric) if _sum_col else 0)
                    bar_col_use = "_bar"
                else:
                    bar_col_use = _sum_col
                if bar_col_use:
                    plot_bar_rate_dual(mc_raw, _wc, bar_col_use, "등록건수", "#66BB6A",
                                       _rc, "전체이용비중", "#FF6F00",
                                       "활성 이용자 복약 등록건수 + 전체이용비중", bar_unit="건")
        elif not med_count.empty:
            mf = filter_by_week_range(med_count, "주차", p_start, p_end, weeks)
            total = mf.groupby("주차")["값"].sum().reset_index()
            total.columns = ["주차", "등록건수합계"]
            total = shorten_dates_in_df(total, "주차")
            plot_bar_rate_dual(total, "주차", "등록건수합계", "등록건수", "#66BB6A",
                               None, None, None,
                               "활성 이용자 복약 등록건수", bar_unit="건")
        else:
            st.info("복약 등록건수 데이터가 없습니다.")

    with tab3:
        # 지자체별 비중 추이
        if not med_users.empty:
            mf = filter_by_week_range(med_users, "주차", p_start, p_end, weeks)
            reg_dict = {}
            if not reg.empty and "지자체명" in reg.columns and "가입완료" in reg.columns:
                for _, r in reg.iterrows():
                    reg_dict[str(r["지자체명"]).strip()] = safe_numeric(r.get("가입완료", 0))
            if reg_dict:
                mf_ratio = mf.copy()
                def _fuzzy(d, key, default=0):
                    if key in d: return d[key]
                    for k, v in d.items():
                        if key in k or k in key: return v
                    return default
                mf_ratio["가입완료"] = mf_ratio["지자체명"].map(lambda x: _fuzzy(reg_dict, x, 0))
                mf_ratio["비중"] = (mf_ratio["값"] / mf_ratio["가입완료"].replace(0, float("nan")) * 100).round(1).fillna(0)
                mf_ratio["값"] = mf_ratio["비중"]
                plot_municipality_lines(mf_ratio, "지자체별 복약 등록 비중 (가입자 대비 %)", metric_label="비중(%)")

    with tab4:
        if not med_users.empty:
            st.markdown("**복약 등록 회원수 원본**")
            st.dataframe(med_users, use_container_width=True, height=300)
        if not med_count.empty:
            st.markdown("**복약 등록건수 원본**")
            st.dataframe(med_count, use_container_width=True, height=300)


# ============================================================
# 🎮 맞고 & 게임
# ============================================================
elif page == "📊 3.안부체크율":
    st.markdown('<div class="section-header">📊 안부체크율</div>', unsafe_allow_html=True)

    # 권역 분류 (세분화)
    DETAIL_REGION = {
        "서초구청": "서울권", "강북구청": "서울권", "마포구청": "서울권", "광진구청": "서울권",
        "경기도청": "경기권", "용인시청": "경기권", "포천시청": "경기권", "광명시청": "경기권", "양평군청": "경기권",
        "청주시청": "충청권", "진천군청": "충청권", "음성군청": "충청권", "괴산군청": "충청권",
        "증평군청": "충청권", "충북사회서비스원": "충청권", "충남사회서비스원": "충청권",
        "강릉시청": "강원권", "강원사회서비스원": "강원권", "홍천군청": "강원권", "삼척시청": "강원권",
        "양양군청": "강원권", "정선군청": "강원권",
        "제주시청": "제주권", "서귀포시청": "제주권",
        "금정구청": "영남권", "경남사회서비스원": "영남권",
        "독거노인지원종합센터": "독거노인지원종합센터", "독거노인종합지원센터": "독거노인지원종합센터",
        "희망나래": "기관", "희망나래장애인복지관": "기관",
    }
    REGION_COLORS = {
        "서울권": "#2F5496", "경기권": "#00897B", "충청권": "#E65100",
        "강원권": "#7B1FA2", "영남권": "#C62828", "제주권": "#0277BD",
        "독거노인지원종합센터": "#1B5E20", "기관": "#795548",
    }

    # ── 전체 안부체크율(OFF 제외) 추이 — gid=261480368 AB열 ───────────────
    cd_all = data.get("checkin_daily", pd.DataFrame())
    if not cd_all.empty and "안부체크율" in cd_all.columns and "날짜" in cd_all.columns:
        cd_plot = cd_all[cd_all["안부체크율"].apply(safe_numeric) > 0].copy()
        # 최근 26주(182일)만 표시
        cd_plot = cd_plot.sort_values("날짜").tail(182)
        if not cd_plot.empty:
            # x축 틱 수 제한: 최대 24개만 표시
            n_pts = len(cd_plot)
            tick_step = max(1, n_pts // 24)
            tick_vals = cd_plot["날짜"].tolist()[::tick_step]

            fig_ab = go.Figure()
            fig_ab.add_trace(go.Scatter(
                x=cd_plot["날짜"], y=cd_plot["안부체크율"].apply(safe_numeric),
                mode="lines+markers", name="안부체크율(OFF 제외)",
                line=dict(color="#2F5496", width=2.5),
                marker=dict(size=5),
                fill="tozeroy", fillcolor="rgba(47,84,150,0.07)",
                hovertemplate="<b>%{x}</b><br>안부체크율: %{y:.1f}%<extra>OFF 제외</extra>",
            ))
            fig_ab.update_layout(
                title="안부체크율 전체 추이 (OFF 제외, AB열 기준)",
                height=360, hovermode="x unified",
                xaxis=dict(
                    type="category", title="",
                    tickmode="array", tickvals=tick_vals,
                    tickangle=-45, tickfont=dict(size=11),
                ),
                yaxis=dict(title="안부체크율 (%)", range=[0, 100]),
                margin=dict(t=45, b=90),
            )
            st.plotly_chart(fig_ab, use_container_width=True)
            st.markdown("---")

    checkin_rate = data.get("checkin_municipality_rate", pd.DataFrame())

    if not checkin_rate.empty and "안부체크율" in checkin_rate.columns:
        cr = checkin_rate[checkin_rate["안부체크율"].notna()].copy()
        cr["권역"] = cr["지자체명"].map(DETAIL_REGION).fillna("기타")

        # 📅 날짜 기간 선택기 — 원본 날짜 전체 기준 (dedup 이전)
        all_dates = sorted(cr["시작일"].unique())
        default_idx = max(0, len(all_dates) - 16)  # 최근 16주 기본 표시

        with st.expander("📅 기간 설정 (펼쳐서 변경)", expanded=False):
            dc1, dc2 = st.columns(2)
            with dc1:
                cr_date_start = st.selectbox("시작일", all_dates, index=default_idx, key="cr_date_start")
            with dc2:
                start_idx = all_dates.index(cr_date_start) if cr_date_start in all_dates else 0
                end_options = all_dates[start_idx:]
                cr_date_end = st.selectbox("종료일", end_options, index=len(end_options)-1, key="cr_date_end")
            st.caption(f"선택 기간: {cr_date_start} ~ {cr_date_end}")

        # 기간 필터 적용 (날짜 원본으로 필터링 후, 시간순 정렬 → 주차 레이블 변환)
        cr = cr[(cr["시작일"] >= cr_date_start) & (cr["시작일"] <= cr_date_end)]
        cr = cr.sort_values("시작일")
        # ── 일별 → 주차 단위 중복 제거 (같은 주 같은 지자체는 마지막 행만 유지) ──
        cr["_week_key"] = cr["시작일"].apply(date_to_week_label)
        cr = cr.drop_duplicates(subset=["_week_key", "지자체명"], keep="last")
        cr = cr.drop(columns=["_week_key"])
        cr = week_label_df(cr, "시작일")

        # 현재 활성 지자체만 필터링 (계약 종료된 마포구청 등 제외)
        active_list = get_active_agencies_for_week(selected_week) if selected_week else []
        if active_list:
            def _match_active(name):
                if name in active_list:
                    return True
                for a in active_list:
                    if name in a or a in name:
                        return True
                return False
            cr = cr[cr["지자체명"].apply(_match_active)]

        regions = sorted(cr["권역"].unique())
        tab_labels = ["전체 비교"] + regions
        tabs = st.tabs(tab_labels)

        # 전체 비교 탭
        with tabs[0]:
            if selected_week:
                dates = list(dict.fromkeys(cr["시작일"].tolist()))  # 시간순 정렬 보존
                if dates:
                    latest_date = dates[-1]
                    latest_data = cr[cr["시작일"] == latest_date].copy()
                    # 같은 주차 내 동일 지자체 중복 제거 (가장 최근 원본 날짜 기준 마지막 행 유지)
                    latest_data = latest_data.drop_duplicates(subset="지자체명", keep="last")
                    latest_data = latest_data.sort_values("안부체크율", ascending=True)

                    st.markdown(f"**{latest_date} 기준**")

                    fig = px.bar(latest_data, y="지자체명", x="안부체크율", orientation="h",
                                 color="권역", color_discrete_map=REGION_COLORS,
                                 height=min(520, max(320, len(latest_data) * 22)))
                    fig.update_layout(
                        title=f"지자체별 안부체크율 ({latest_date})",
                        legend=LEGEND_BELOW, margin=dict(t=40, b=70),
                        xaxis=dict(range=[0, 105]),
                        yaxis=dict(tickfont=dict(size=11)),
                    )
                    fig.update_traces(
                        texttemplate="%{x:.1f}%", textposition="outside",
                        textfont=dict(size=12),
                    )
                    st.plotly_chart(fig, use_container_width=True)

        # 권역별 탭
        for i, region in enumerate(regions):
            with tabs[i + 1]:
                region_data = cr[cr["권역"] == region]
                mun_list = region_data["지자체명"].unique().tolist()
                st.markdown(f"**{region}** — {', '.join(mun_list)}")

                fig = px.line(region_data, x="시작일", y="안부체크율", color="지자체명",
                              markers=True, color_discrete_sequence=px.colors.qualitative.Set2)
                fig.update_layout(
                    title=f"{region} 안부체크율 추이", height=400,
                    hovermode="x unified",
                    xaxis=dict(
                        type="category", title="",
                        tickangle=-45, tickfont=dict(size=11),
                    ),
                    yaxis=dict(title="안부체크율 (%)", range=[0, 100]),
                    legend=LEGEND_BELOW, margin=dict(t=40, b=90),
                )
                fig.update_traces(hovertemplate="<b>%{x}</b><br>%{y:.1f}%<extra>%{fullData.name}</extra>")
                st.plotly_chart(fig, use_container_width=True)

                # 해당 권역 최신 바 차트
                dates = list(dict.fromkeys(region_data["시작일"].tolist()))  # 시간순 정렬 보존
                if dates:
                    latest = (region_data[region_data["시작일"] == dates[-1]]
                              .drop_duplicates(subset="지자체명", keep="last")
                              .sort_values("안부체크율", ascending=True))
                    fig2 = px.bar(latest, y="지자체명", x="안부체크율", orientation="h",
                                  color_discrete_sequence=[REGION_COLORS.get(region, "#666")],
                                  height=min(400, max(200, len(latest) * 26)))
                    fig2.update_layout(
                        title=f"{region} 최신 안부체크율",
                        margin=dict(t=40, b=10),
                        xaxis=dict(range=[0, 105]),
                        yaxis=dict(tickfont=dict(size=11)),
                    )
                    fig2.update_traces(
                        texttemplate="%{x:.1f}%", textposition="outside",
                        textfont=dict(size=12),
                    )
                    st.plotly_chart(fig2, use_container_width=True)

        # 추가 지표
        extra_metrics = [m for m in ["안부확인율", "48미확인율", "안부콜응답률"] if m in checkin_rate.columns]
        if extra_metrics:
            st.markdown("---")
            selected_extra = st.selectbox("추가 지표 보기", extra_metrics)
            er = checkin_rate[checkin_rate[selected_extra].notna()].copy()
            er = er.sort_values("시작일")
            er = week_label_df(er, "시작일")
            if not er.empty:
                er["권역"] = er["지자체명"].map(DETAIL_REGION).fillna("기타")
                sel_region = st.radio("권역 선택", ["전체"] + sorted(er["권역"].unique().tolist()), horizontal=True, key="extra_region")
                if sel_region != "전체":
                    er = er[er["권역"] == sel_region]
                fig3 = px.line(er, x="시작일", y=selected_extra, color="지자체명", markers=True)
                fig3.update_layout(
                    title=f"지자체별 {selected_extra} 추이", height=400,
                    hovermode="x unified", xaxis=dict(type="category"),
                    yaxis=dict(title="%"),
                    legend=LEGEND_BELOW_LARGE,
                )
                st.plotly_chart(fig3, use_container_width=True)
    else:
        # DB 폴백
        try:
            safety_db = get_db_data("raw_safety_check")
        except:
            safety_db = pd.DataFrame()
        if not safety_db.empty and "complete_user_count" in safety_db.columns:
            mun_rate = safety_db.copy()
            mun_rate["안부체크율"] = (mun_rate["complete_user_count"] / mun_rate["target_user_count"].replace(0, float("nan")) * 100).round(1).fillna(0)
            st.dataframe(mun_rate[["date", "agency_name", "안부체크율"]].sort_values("date", ascending=False), use_container_width=True)
        else:
            st.info("데이터가 없습니다.")


# ============================================================
# 🔄 4.안부체크 변경
# ============================================================
elif page == "🔄 4.안부체크 변경":
    st.markdown('<div class="section-header">🔄 안부체크 변경건</div>', unsafe_allow_html=True)
    st.markdown("""
    <div class="insight-box">
    개인정보가 포함된 데이터입니다.<br>
    데이터 소스: <a href="https://docs.google.com/spreadsheets/d/15UZ9dZjYdD24PdWoSvrFWpQCM-T0vhc_yy9wrMunSNc/edit?gid=851523453" target="_blank">지자체별 안부체크 변경건 시트</a>
    </div>
    """, unsafe_allow_html=True)

    # Google Sheets에서 안부체크횟수 시트 (gid=851523453) 직접 가져오기
    checkin_change_raw = sheets.get("안부체크횟수", pd.DataFrame())

    if not checkin_change_raw.empty:
        df = checkin_change_raw.copy()

        # 시작일 컬럼 찾기
        date_col = None
        for c in df.columns:
            cl = str(c).replace("\n", "").strip()
            if "시작일" in cl:
                date_col = c
                break
        if date_col is None:
            date_col = df.columns[0]

        # 날짜 목록 추출
        all_change_dates = sorted([str(d).strip() for d in df[date_col].dropna().unique() if str(d).strip() and str(d).strip() != "nan"])

        tab_basic, tab_safe, tab_input = st.tabs(["📊 베이직 (안부상태 변경)", "🛡 세이프 (KT 관제)", "✏ 데이터 입력"])

        with tab_basic:
            # === 베이직: 안부상태 변경건 ===

            # 📅 기간 선택 (기본: 25-52주차 = 2025-12-28 근사)
            basic_default = "2025-12-28"
            basic_idx = 0
            for i, d in enumerate(all_change_dates):
                if d >= basic_default:
                    basic_idx = i
                    break
            with st.expander("📅 베이직 기간 설정 (펼쳐서 변경)", expanded=False):
                bc1, bc2 = st.columns(2)
                with bc1:
                    basic_start = st.selectbox("시작일", all_change_dates, index=basic_idx, key="basic_change_start")
                with bc2:
                    bs_idx = all_change_dates.index(basic_start) if basic_start in all_change_dates else 0
                    basic_end_opts = all_change_dates[bs_idx:]
                    basic_end = st.selectbox("종료일", basic_end_opts, index=len(basic_end_opts)-1, key="basic_change_end")
                st.caption(f"베이직 기간: {basic_start} ~ {basic_end}")

            # 기간 필터 적용
            df_basic = df[(df[date_col].astype(str) >= basic_start) & (df[date_col].astype(str) <= basic_end)].copy()
            # datetime 파싱으로 정확한 날짜 정렬
            df_basic["_sort"] = pd.to_datetime(df_basic[date_col].astype(str), errors="coerce")
            df_basic = df_basic.sort_values("_sort").drop(columns=["_sort"])
            df_basic = shorten_dates_in_df(df_basic, date_col)

            # 총합, 총 안부상태변경률 컬럼 찾기
            total_col = None
            total_rate_col = None
            mun_change_cols = []  # 지자체별 변경건 컬럼
            mun_rate_cols = []    # 지자체별 변경률 컬럼

            for c in df_basic.columns:
                cl = str(c).replace("\n", "").strip()
                if cl == "총합":
                    total_col = c
                elif "총 안부상태변경률" in cl or cl == "총 안부상태변경률":
                    total_rate_col = c
                elif "안부상태변경률" in cl and "총" not in cl:
                    mun_rate_cols.append(c)
                elif any(kw in cl for kw in MUNICIPALITY_KEYWORDS) and "KTT" not in cl and "관제" not in cl and "발송" not in cl and "변경률" not in cl:
                    mun_change_cols.append(c)

            # 1. 총 안부상태 변경건 + 변경률 추이
            if total_col:
                df_basic[total_col] = df_basic[total_col].apply(safe_numeric)
                fig = make_subplots(specs=[[{"secondary_y": True}]])
                fig.add_trace(go.Bar(
                    x=df_basic[date_col], y=df_basic[total_col], name="총 변경건",
                    marker_color="#FF6F00", opacity=0.7,
                    hovertemplate="%{y:,.0f}건<extra>총 변경건</extra>"
                ), secondary_y=False)

                if total_rate_col:
                    df_basic[total_rate_col] = df_basic[total_rate_col].apply(safe_numeric)
                    fig.add_trace(go.Scatter(
                        x=df_basic[date_col], y=df_basic[total_rate_col], name="변경률",
                        mode="lines+markers", line=dict(color="#D32F2F", width=2),
                        hovertemplate="%{y:.1f}%<extra>변경률</extra>"
                    ), secondary_y=True)

                fig.update_layout(
                    title="총 안부상태 변경건 및 변경률", height=400,
                    xaxis=dict(type="category"), hovermode="x unified",
                    margin=dict(t=40, b=60, l=40, r=40),
                    legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5),
                )
                fig.update_yaxes(title_text="변경건", secondary_y=False)
                fig.update_yaxes(title_text="%", secondary_y=True)
                st.plotly_chart(fig, use_container_width=True)

            # 2. 지자체별 안부상태 변경건 추이
            if mun_change_cols:
                rows = []
                for _, row in df_basic.iterrows():
                    d = str(row.get(date_col, "")).strip()
                    if not d or d == "nan":
                        continue
                    for mc in mun_change_cols:
                        mun = extract_municipality_name(mc)
                        val = safe_numeric(row.get(mc, 0))
                        if val > 0:
                            rows.append({"날짜": d, "지자체명": mun, "변경건": val})
                if rows:
                    mun_df = pd.DataFrame(rows)
                    # 날짜를 datetime으로 파싱하여 정확히 정렬
                    mun_df["_sort"] = pd.to_datetime("20" + mun_df["날짜"], errors="coerce")
                    mun_df = mun_df.sort_values("_sort").drop(columns=["_sort"])
                    sorted_dates = mun_df["날짜"].unique().tolist()
                    fig2 = px.line(mun_df, x="날짜", y="변경건", color="지자체명", markers=True)
                    fig2.update_layout(
                        title="지자체별 안부상태 변경건 추이", height=400,
                        xaxis=dict(type="category", categoryorder="array", categoryarray=sorted_dates),
                        hovermode="x unified",
                        margin=dict(t=40, b=60, l=40, r=10),
                        legend=LEGEND_BELOW_LARGE,
                    )
                    st.plotly_chart(fig2, use_container_width=True)

        with tab_safe:
            # === 세이프: KT 관제 ===

            # 📅 기간 선택 (기본: 2026-03-01)
            safe_default = "2026-03-01"
            safe_idx = 0
            for i, d in enumerate(all_change_dates):
                if d >= safe_default:
                    safe_idx = i
                    break
            with st.expander("📅 세이프 기간 설정 (펼쳐서 변경)", expanded=False):
                sc1, sc2 = st.columns(2)
                with sc1:
                    safe_start = st.selectbox("시작일", all_change_dates, index=safe_idx, key="safe_change_start")
                with sc2:
                    ss_idx = all_change_dates.index(safe_start) if safe_start in all_change_dates else 0
                    safe_end_opts = all_change_dates[ss_idx:]
                    safe_end = st.selectbox("종료일", safe_end_opts, index=len(safe_end_opts)-1, key="safe_change_end")
                st.caption(f"세이프 기간: {safe_start} ~ {safe_end}")

            # 기간 필터 적용
            df_safe = df[(df[date_col].astype(str) >= safe_start) & (df[date_col].astype(str) <= safe_end)].copy()
            df_safe = shorten_dates_in_df(df_safe, date_col)

            kt_total_col = None
            kt_send_col = None
            kt_rate_col = None
            kt_mun_rate_cols = []

            for c in df_safe.columns:
                cl = str(c).replace("\n", "").strip()
                if cl == "KT 관제 수":
                    kt_total_col = c
                elif cl == "전체 발송수":
                    kt_send_col = c
                elif cl == "KT 관제 대응률":
                    kt_rate_col = c
                elif "KT관제 대응률" in cl and cl != "KT 관제 대응률":
                    kt_mun_rate_cols.append(c)

            # 1. KT 관제 수 + 전체 발송수 + 대응률
            if kt_total_col and kt_send_col:
                df_safe[kt_total_col] = df_safe[kt_total_col].apply(safe_numeric)
                df_safe[kt_send_col] = df_safe[kt_send_col].apply(safe_numeric)

                fig3 = make_subplots(specs=[[{"secondary_y": True}]])
                fig3.add_trace(go.Bar(
                    x=df_safe[date_col], y=df_safe[kt_send_col], name="전체 발송수",
                    marker_color="#B0BEC5", opacity=0.6,
                    hovertemplate="%{y:,.0f}건<extra>전체 발송수</extra>"
                ), secondary_y=False)
                fig3.add_trace(go.Bar(
                    x=df_safe[date_col], y=df_safe[kt_total_col], name="KT 관제 수",
                    marker_color="#1565C0",
                    hovertemplate="%{y:,.0f}건<extra>KT 관제 수</extra>"
                ), secondary_y=False)

                if kt_rate_col:
                    df_safe[kt_rate_col] = df_safe[kt_rate_col].apply(safe_numeric)
                    fig3.add_trace(go.Scatter(
                        x=df_safe[date_col], y=df_safe[kt_rate_col], name="KT 관제 대응률",
                        mode="lines+markers", line=dict(color="#D32F2F", width=2),
                        hovertemplate="%{y:.1f}%<extra>KT 관제 대응률</extra>"
                    ), secondary_y=True)

                fig3.update_layout(
                    title="세이프 - KT 관제 현황", height=400,
                    xaxis=dict(type="category"), hovermode="x unified",
                    barmode="group",
                    margin=dict(t=40, b=60, l=40, r=40),
                    legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5),
                )
                fig3.update_yaxes(title_text="건수", secondary_y=False)
                fig3.update_yaxes(title_text="%", secondary_y=True)
                st.plotly_chart(fig3, use_container_width=True)

            # 2. 지자체별 관제 수 + 출동 수
            ktt_control_cols = []
            ktt_dispatch_cols = []
            for c in df_safe.columns:
                cl = str(c).replace("\n", "").strip()
                if cl.startswith("KTT_관제_"):
                    ktt_control_cols.append(c)
                elif cl.startswith("KTT_출동_"):
                    ktt_dispatch_cols.append(c)

            if ktt_control_cols:
                # 관제 수 데이터
                ctrl_rows = []
                disp_rows = []
                for _, row in df_safe.iterrows():
                    d = str(row.get(date_col, "")).strip()
                    if not d or d == "nan":
                        continue
                    for cc in ktt_control_cols:
                        mun = str(cc).replace("KTT_관제_", "").strip()
                        val = safe_numeric(row.get(cc, 0))
                        if val > 0 or mun not in ["마포구청", "광진구청"]:  # 종료 지자체 제외
                            ctrl_rows.append({"날짜": d, "지자체명": mun, "관제수": val})
                    for dc in ktt_dispatch_cols:
                        mun = str(dc).replace("KTT_출동_", "").strip()
                        val = safe_numeric(row.get(dc, 0))
                        if val > 0 or mun not in ["마포구청", "광진구청"]:
                            disp_rows.append({"날짜": d, "지자체명": mun, "출동수": val})

                st.markdown('<div class="section-header">지자체별 KT 관제 수</div>', unsafe_allow_html=True)
                if ctrl_rows:
                    ctrl_df = pd.DataFrame(ctrl_rows)
                    # 활성 지자체만 (데이터 있는 것만)
                    active_muns = ctrl_df.groupby("지자체명")["관제수"].sum()
                    active_muns = active_muns[active_muns > 0].index.tolist()
                    ctrl_df = ctrl_df[ctrl_df["지자체명"].isin(active_muns)]

                    fig4 = px.bar(ctrl_df, x="날짜", y="관제수", color="지자체명",
                                  barmode="group", height=400)
                    fig4.update_layout(
                        title="지자체별 주차별 KT 관제 수",
                        xaxis=dict(type="category"), hovermode="x unified",
                        legend=LEGEND_BELOW, margin=dict(t=40, b=70),
                    )
                    fig4.update_traces(hovertemplate="<b>%{x}</b><br>%{y:,.0f}건<extra>%{fullData.name}</extra>")
                    st.plotly_chart(fig4, use_container_width=True)

                st.markdown('<div class="section-header">지자체별 KT 출동 수</div>', unsafe_allow_html=True)
                if disp_rows:
                    disp_df = pd.DataFrame(disp_rows)
                    active_muns_d = disp_df.groupby("지자체명")["출동수"].sum()
                    active_muns_d = active_muns_d[active_muns_d > 0].index.tolist()
                    if active_muns_d:
                        disp_df = disp_df[disp_df["지자체명"].isin(active_muns_d)]
                        fig5 = px.bar(disp_df, x="날짜", y="출동수", color="지자체명",
                                      barmode="group", height=400)
                        fig5.update_layout(
                            title="지자체별 주차별 KT 출동 수",
                            xaxis=dict(type="category"), hovermode="x unified",
                            legend=LEGEND_BELOW, margin=dict(t=40, b=70),
                        )
                        fig5.update_traces(hovertemplate="<b>%{x}</b><br>%{y:,.0f}건<extra>%{fullData.name}</extra>")
                        st.plotly_chart(fig5, use_container_width=True)
                    else:
                        st.info("출동 데이터가 없습니다.")

        with tab_input:
            # 수동 입력 폼 (개인정보 데이터)
            st.markdown('<div class="section-header">데이터 직접 입력</div>', unsafe_allow_html=True)
            st.caption("개인정보가 포함된 안부상태 변경 데이터를 직접 입력합니다.")

            with st.form("checkin_change_form", clear_on_submit=True):
                fc1, fc2 = st.columns(2)
                with fc1:
                    change_date = st.date_input("날짜")
                    change_agency = st.selectbox("지자체", [
                        "경기도청", "서초구청", "진천군청", "음성군청", "강북구청",
                        "금정구청", "증평군청", "포천시청", "경남사회서비스원",
                        "강릉시청", "강원사회서비스원", "충북사회서비스원",
                        "독거노인지원종합센터", "희망나래장애인복지관", "홍천군청",
                        "충남사회서비스원", "삼척시청", "마포구청", "광진구청",
                    ])
                with fc2:
                    change_count = st.number_input("변경건수", min_value=0, value=0)
                    change_memo = st.text_input("메모", placeholder="상태 변경 사유")

                if st.form_submit_button("💾 저장", type="primary", use_container_width=True):
                    from local_db import get_connection as _gc
                    conn = _gc()
                    try:
                        conn.execute("""
                        INSERT INTO raw_generic (data_type, date, agency_name, raw_json)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT(data_type, date, agency_name) DO UPDATE SET raw_json=excluded.raw_json
                        """, ("안부체크변경", str(change_date), change_agency,
                              f'{{"count": {change_count}, "memo": "{change_memo}"}}'))
                        conn.commit()
                        st.success(f"✅ {change_agency} {change_date} 변경건 {change_count}건 저장!")
                    except Exception as e:
                        st.error(f"저장 실패: {e}")
                    conn.close()

    else:
        st.info("안부체크 변경건 데이터가 없습니다.")


# ============================================================
# 🎮 11.맞고(와플랫+게스트)
# ============================================================
elif page == "🎮 11.맞고(와플랫+게스트)":
    st.markdown('<div class="section-header">🎮 맞고 (와플랫+게스트)</div>', unsafe_allow_html=True)
    p_start, p_end = page_week_range_selector("matgo_all", weeks)

    # 집계형 시트에서 직접 가져오기
    matgo_all_raw = sheets.get("맞고와플게스트", pd.DataFrame())
    if not matgo_all_raw.empty:
        matgo_all = matgo_all_raw.copy()
        # 주차 컬럼 찾기
        week_col = None
        for c in matgo_all.columns:
            if "주차" in str(c):
                week_col = c
                break
        num_cols = [c for c in matgo_all.columns if c not in [week_col, "시작일"] and c is not None]
        for c in num_cols:
            matgo_all[c] = matgo_all[c].apply(safe_numeric)

        if week_col:
            matgo_all = filter_by_week_range(matgo_all, week_col, p_start, p_end, weeks)
            matgo_all = shorten_dates_in_df(matgo_all, week_col)

        tab1, tab2, tab3 = st.tabs(["이용자수·플레이판수", "플레이시간", "상세 데이터"])
        with tab1:
            if week_col:
                # 이용자수 + 1인당 플레이판수 듀얼 차트
                user_col = next((c for c in num_cols if "이용자" in str(c)), None)
                play_col = next((c for c in num_cols if "플레이판수" in str(c) and "1인당" not in str(c)), None)
                per_play_col = next((c for c in num_cols if "1인당" in str(c) and "플레이판" in str(c)), None)

                if user_col and play_col:
                    # 이용자수(막대, 좌) + 플레이판수(꺾은선, 우) 듀얼 Y축
                    fig = make_subplots(specs=[[{"secondary_y": True}]])
                    fig.add_trace(go.Bar(
                        x=matgo_all[week_col], y=matgo_all[user_col], name="이용자수",
                        marker_color="#FF6F00", opacity=0.85,
                        text=matgo_all[user_col].apply(lambda v: f"{int(v):,}" if pd.notna(v) else ""),
                        textposition="outside", textfont=dict(size=9),
                        hovertemplate="%{y:,}명<extra>이용자수</extra>"
                    ), secondary_y=False)
                    fig.add_trace(go.Scatter(
                        x=matgo_all[week_col], y=matgo_all[play_col], name="플레이판수",
                        mode="lines+markers+text",
                        line=dict(color="#FFB74D", width=2),
                        text=matgo_all[play_col].apply(lambda v: f"{int(v):,}" if pd.notna(v) else ""),
                        textposition="top center", textfont=dict(size=9, color="#E65100"),
                        hovertemplate="%{y:,}판<extra>플레이판수</extra>"
                    ), secondary_y=True)
                    if per_play_col:
                        fig.add_trace(go.Scatter(
                            x=matgo_all[week_col], y=matgo_all[per_play_col],
                            name="1인당 플레이판수", mode="lines+markers",
                            line=dict(color="#D32F2F", width=2, dash="dot"),
                            hovertemplate="%{y:.1f}판<extra>1인당</extra>"
                        ), secondary_y=True)
                    fig.update_layout(title="맞고(와플랫+게스트) 이용 현황", height=420,
                                      hovermode="x unified",
                                      xaxis=dict(type="category"),
                                      legend=LEGEND_BELOW, margin=dict(t=40, b=70), bargap=0.3)
                    fig.update_yaxes(title_text="이용자수 (명)", secondary_y=False)
                    fig.update_yaxes(title_text="플레이판수 (판)", secondary_y=True, showgrid=False)
                    st.plotly_chart(fig, use_container_width=True)

        with tab2:
            if week_col:
                # "수정" 버전 우선 사용 (시:분:초 형식이 아닌 숫자 형태)
                time_col = next((c for c in num_cols if "수정 플레이시간" in str(c) and "1인당" not in str(c)), None)
                if time_col is None:
                    time_col = next((c for c in num_cols if "플레이시간" in str(c) and "게스트" not in str(c) and "합" not in str(c) and "1인당" not in str(c) and "수정" not in str(c)), None)
                per_time_col = next((c for c in num_cols if "수정 1인당" in str(c) and "플레이시간" in str(c)), None)
                if per_time_col is None:
                    per_time_col = next((c for c in num_cols if "1인당" in str(c) and "플레이시간" in str(c) and "수정" not in str(c)), None)

                if time_col:
                    fig = make_subplots(specs=[[{"secondary_y": True}]])
                    fig.add_trace(go.Bar(x=matgo_all[week_col], y=matgo_all[time_col], name="플레이시간",
                                         marker_color="#4E342E",
                                         hovertemplate="%{y:,.0f}시간<extra>전체</extra>"), secondary_y=False)
                    if per_time_col:
                        fig.add_trace(go.Scatter(x=matgo_all[week_col], y=matgo_all[per_time_col],
                                                  name="1인당 플레이시간", mode="lines+markers",
                                                  line=dict(color="#E91E63", width=2),
                                                  hovertemplate="%{y:.1f}시간<extra>1인당</extra>"), secondary_y=True)
                    fig.update_layout(title="맞고(와플랫+게스트) 플레이 시간", height=400,
                                      hovermode="x unified", xaxis=dict(type="category"),
                                      legend=LEGEND_BELOW, margin=dict(t=40, b=70))
                    st.plotly_chart(fig, use_container_width=True)

        with tab3:
            st.dataframe(matgo_all, use_container_width=True, height=400)
    else:
        st.info("맞고(와플랫+게스트) 데이터가 없습니다.")


# ============================================================
# 🃏 10.맞고(와플랫)
# ============================================================
elif page == "🃏 10.맞고(와플랫)":
    st.markdown('<div class="section-header">🃏 맞고 (와플랫)</div>', unsafe_allow_html=True)
    p_start, p_end = page_week_range_selector("matgo_waflat", weeks)

    tab1, tab2, tab3 = st.tabs(["이용자수", "플레이 판수", "플레이 시간"])

    with tab1:
        matgo_user_raw = sheets.get("맞고이용자", pd.DataFrame())
        df = data.get("weekly_맞고이용자", pd.DataFrame())
        if not matgo_user_raw.empty:
            mu = matgo_user_raw.copy()
            _wc, _sum_col, _rc = None, None, None
            for c in mu.columns:
                cl = str(c).replace("\n", "").strip()
                if "주차" in cl and _wc is None: _wc = c
                elif ("이용자합계" in cl or ("합계" in cl and "이용자" in cl)) and _sum_col is None: _sum_col = c
                elif ("전체이용비중" in cl or "이용비중" in cl) and _rc is None: _rc = c
            if _wc:
                mu = filter_by_week_range(mu, _wc, p_start, p_end, weeks)
                mu = shorten_dates_in_df(mu, _wc)
                if _sum_col:
                    plot_bar_rate_dual(mu, _wc, _sum_col, "이용자수", "#42A5F5",
                                       _rc, "전체이용비중", "#FF6F00",
                                       "맞고(와플랫) 이용자수 + 전체이용비중")
            if not df.empty:
                dff = filter_by_week_range(df, "주차", p_start, p_end, weeks)
                plot_municipality_lines(dff, "지자체별 맞고 이용자수", metric_label="이용자수")
            # ── 지자체별 이용자비중 추이 (AI~BK열) ──────────────────────────
            st.markdown("---")
            mrt_matgo = extract_mun_ratio_trend(matgo_user_raw)
            if not mrt_matgo.empty:
                mrt_matgo = filter_by_week_range(mrt_matgo, "주차", p_start, p_end, weeks)
                _active_m = mrt_matgo.groupby("지자체명")["값"].sum()
                _active_m = _active_m[_active_m > 0].index.tolist()
                mrt_matgo = mrt_matgo[mrt_matgo["지자체명"].isin(_active_m)]
                if not mrt_matgo.empty:
                    plot_municipality_lines(mrt_matgo, "지자체별 맞고(와플랫) 이용자비중 추이 (%)", metric_label="이용자비중(%)")
        else:
            st.info("데이터 없음")

    with tab2:
        matgo_play_raw = sheets.get("맞고플레이판수", pd.DataFrame())
        df = data.get("weekly_맞고플레이판수", pd.DataFrame())
        if not matgo_play_raw.empty:
            mp = matgo_play_raw.copy()
            _wc, _sum_col, _awc = None, None, None
            for c in mp.columns:
                cl = str(c).replace("\n", "").strip()
                if "주차" in cl and _wc is None: _wc = c
                elif "합계" in cl and _sum_col is None: _sum_col = c
                elif "1인" in cl and "주평균" in cl and _awc is None: _awc = c
            if _wc:
                mp = filter_by_week_range(mp, _wc, p_start, p_end, weeks)
                mp = shorten_dates_in_df(mp, _wc)
                if _sum_col:
                    plot_bar_rate_dual(mp, _wc, _sum_col, "플레이판수", "#42A5F5",
                                       _awc, "1인 주평균", "#455A64",
                                       "맞고(와플랫) 플레이판수 + 1인 주평균", bar_unit="판", line_unit="판")
            if not df.empty:
                dff = filter_by_week_range(df, "주차", p_start, p_end, weeks)
                plot_municipality_lines(dff, "지자체별 플레이 판수", metric_label="판수")
        else:
            st.info("데이터 없음")

    with tab3:
        df = data.get("weekly_맞고플레이시간", pd.DataFrame())
        if not df.empty:
            df = filter_by_week_range(df, "주차", p_start, p_end, weeks)
            total = df.pipe(weekly_total)
            total = total.rename(columns={"값": "플레이시간합계"})
            plot_weekly_series(total, "주차", "플레이시간합계", "맞고(와플랫) 플레이 시간 추이", "#BF360C")
            plot_municipality_lines(df, "지자체별 플레이 시간", metric_label="시간")
        else:
            st.info("데이터 없음")


# ============================================================
# 👤 12.맞고(게스트)
# ============================================================
elif page == "👤 12.맞고(게스트)":
    st.markdown('<div class="section-header">👤 맞고 (게스트)</div>', unsafe_allow_html=True)
    p_start, p_end = page_week_range_selector("matgo_guest", weeks)

    # 집계형 시트에서 직접 가져오기
    guest_raw = sheets.get("맞고게스트", pd.DataFrame())
    if not guest_raw.empty:
        guest_df = guest_raw.copy()
        week_col = None
        for c in guest_df.columns:
            if "주차" in str(c):
                week_col = c
                break
        num_cols = [c for c in guest_df.columns if c not in [week_col, "시작일"] and c is not None]
        for c in num_cols:
            guest_df[c] = guest_df[c].apply(safe_numeric)

        if week_col:
            guest_df = filter_by_week_range(guest_df, week_col, p_start, p_end, weeks)
            guest_df = shorten_dates_in_df(guest_df, week_col)

        tab1, tab2 = st.tabs(["추이 차트", "상세 데이터"])
        with tab1:
            if week_col:
                user_col = next((c for c in num_cols if "이용자" in str(c) and "WoW" not in str(c) and "1인" not in str(c)), None)
                play_col = next((c for c in num_cols if "플레이 판수" in str(c) or "플레이판수" in str(c)), None)
                time_col = next((c for c in num_cols if "플레이 시간" in str(c) and "1인" not in str(c) and "초" not in str(c)), None)
                per_play_col = next((c for c in num_cols if "1인당" in str(c) and "플레이 판" in str(c)), None)

                if user_col:
                    fig = make_subplots(specs=[[{"secondary_y": True}]])
                    fig.add_trace(go.Bar(x=guest_df[week_col], y=guest_df[user_col], name="이용자수",
                                         marker_color="#795548",
                                         hovertemplate="%{y:,}명<extra>이용자수</extra>"), secondary_y=False)
                    if play_col:
                        fig.add_trace(go.Bar(x=guest_df[week_col], y=guest_df[play_col], name="플레이판수",
                                             marker_color="#BCAAA4",
                                             hovertemplate="%{y:,}판<extra>플레이판수</extra>"), secondary_y=False)
                    if per_play_col:
                        fig.add_trace(go.Scatter(x=guest_df[week_col], y=guest_df[per_play_col],
                                                  name="1인당 플레이판수", mode="lines+markers",
                                                  line=dict(color="#D32F2F", width=2),
                                                  hovertemplate="%{y:.1f}판<extra>1인당</extra>"), secondary_y=True)
                    fig.update_layout(title="맞고(게스트) 이용 현황", height=420,
                                      hovermode="x unified", barmode="group",
                                      xaxis=dict(type="category"),
                                      legend=LEGEND_BELOW, margin=dict(t=40, b=70))
                    fig.update_yaxes(title_text="명/판", secondary_y=False)
                    if per_play_col:
                        fig.update_yaxes(title_text="1인당 판수", secondary_y=True)
                    st.plotly_chart(fig, use_container_width=True)

        with tab2:
            st.dataframe(guest_df, use_container_width=True, height=400)
    else:
        st.info("맞고(게스트) 데이터가 없습니다.")


# ============================================================
# 😰 스트레스체크
# ============================================================
elif page == "😰 6.스트레스체크":
    st.markdown('<div class="section-header">😰 스트레스체크</div>', unsafe_allow_html=True)
    p_start, p_end = page_week_range_selector("stress", weeks)

    tab1, tab2 = st.tabs(["이용자수 추이", "수행횟수 추이"])

    with tab1:
        stress_users = data.get("weekly_스트레스이용자", pd.DataFrame())
        stress_user_raw = sheets.get("스트레스이용자", pd.DataFrame())
        if not stress_user_raw.empty:
            su = stress_user_raw.copy()
            _wc, _sum_col, _rc = None, None, None
            for c in su.columns:
                cl = str(c).replace("\n", "").strip()
                if "주차" in cl and _wc is None: _wc = c
                elif ("이용자합계" in cl or ("합계" in cl and "이용자" in cl)) and _sum_col is None: _sum_col = c
                elif ("전체이용비중" in cl or "이용비중" in cl) and _rc is None: _rc = c
            stress_total_c = data.get("total_스트레스이용자", pd.DataFrame())
            if _wc:
                su = filter_by_week_range(su, _wc, p_start, p_end, weeks)
                su = shorten_dates_in_df(su, _wc)
                if not stress_total_c.empty:
                    stt = filter_by_week_range(stress_total_c, "주차", p_start, p_end, weeks)
                    stt = shorten_dates_in_df(stt, "주차")
                    ct_map = dict(zip(stt["주차"], stt["값"].apply(safe_numeric)))
                    su["_bar"] = su[_wc].map(ct_map).fillna(su[_sum_col].apply(safe_numeric) if _sum_col else 0)
                    bar_col_use = "_bar"
                else:
                    bar_col_use = _sum_col
                if bar_col_use:
                    plot_bar_rate_dual(su, _wc, bar_col_use, "이용자수", "#AB47BC",
                                       _rc, "전체이용비중", "#FF6F00",
                                       "스트레스체크 이용자수 + 전체이용비중")
            sf = filter_by_week_range(stress_users, "주차", p_start, p_end, weeks) if not stress_users.empty else pd.DataFrame()
            if not sf.empty:
                # 이용자수 → 이용률 (주차별 지자체 가입완료 회원 대비 %)
                _wrm_s = data.get("weekly_registered_by_mun", pd.DataFrame())
                if not _wrm_s.empty:
                    _reg_week_map_s = {(str(r["주차"]).strip(), str(r["지자체명"]).strip()): safe_numeric(r["가입완료"])
                                       for _, r in _wrm_s.iterrows()}
                    sf = sf.copy()
                    def _stress_rate(r):
                        denom = _reg_week_map_s.get((str(r["주차"]).strip(), str(r["지자체명"]).strip()), 0)
                        return round(r["값"] / denom * 100, 1) if denom > 0 else 0
                    sf["값"] = sf.apply(_stress_rate, axis=1)
                    plot_municipality_lines(sf, "지자체별 스트레스체크 이용률 추이 (가입회원 대비 %)", metric_label="이용률(%)")
                else:
                    plot_municipality_lines(sf, "지자체별 스트레스체크 이용자 추이", metric_label="이용자수")
            # ── 지자체별 이용자비중 추이 (AI~BK열) ──────────────────────────
            st.markdown("---")
            mrt_stress = extract_mun_ratio_trend(stress_user_raw)
            if not mrt_stress.empty:
                mrt_stress = filter_by_week_range(mrt_stress, "주차", p_start, p_end, weeks)
                _active_s = mrt_stress.groupby("지자체명")["값"].sum()
                _active_s = _active_s[_active_s > 0].index.tolist()
                mrt_stress = mrt_stress[mrt_stress["지자체명"].isin(_active_s)]
                if not mrt_stress.empty:
                    plot_municipality_lines(mrt_stress, "지자체별 스트레스체크 이용자비중 추이 (%)", metric_label="이용자비중(%)")
        else:
            st.info("스트레스체크 이용자 데이터가 없습니다.")

    with tab2:
        stress_count = data.get("weekly_스트레스수행횟수", pd.DataFrame())
        stress_exam_raw = sheets.get("스트레스수행횟수", pd.DataFrame())
        if not stress_exam_raw.empty:
            se = stress_exam_raw.copy()
            _wc, _sum_col, _awc = None, None, None
            for c in se.columns:
                cl = str(c).replace("\n", "").strip()
                if "주차" in cl and _wc is None: _wc = c
                elif "합계" in cl and _sum_col is None: _sum_col = c
                elif "1인" in cl and "주평균" in cl and _awc is None: _awc = c
            stress_exam_total_c = data.get("total_스트레스수행횟수", pd.DataFrame())
            if _wc:
                se = filter_by_week_range(se, _wc, p_start, p_end, weeks)
                se = shorten_dates_in_df(se, _wc)
                if not stress_exam_total_c.empty:
                    sett = filter_by_week_range(stress_exam_total_c, "주차", p_start, p_end, weeks)
                    sett = shorten_dates_in_df(sett, "주차")
                    ct_map = dict(zip(sett["주차"], sett["값"].apply(safe_numeric)))
                    se["_bar"] = se[_wc].map(ct_map).fillna(se[_sum_col].apply(safe_numeric) if _sum_col else 0)
                    bar_col_use = "_bar"
                else:
                    bar_col_use = _sum_col
                if bar_col_use:
                    plot_bar_rate_dual(se, _wc, bar_col_use, "수행횟수", "#AB47BC",
                                       _awc, "1인 주평균", "#455A64",
                                       "스트레스체크 수행횟수 + 1인 주평균", bar_unit="회", line_unit="회")
            sf = filter_by_week_range(stress_count, "주차", p_start, p_end, weeks) if not stress_count.empty else pd.DataFrame()
            if not sf.empty:
                plot_municipality_lines(sf, "지자체별 스트레스체크 수행횟수 추이", metric_label="수행횟수")
        else:
            st.info("스트레스체크 수행횟수 데이터가 없습니다.")



# ============================================================
# 🤖 AI 생활지원사
# ============================================================
elif page == "🤖 AI 생활지원사":
    st.markdown('<div class="section-header">🤖 AI 생활지원사</div>', unsafe_allow_html=True)

    ai_df = data.get("ai_funnel", pd.DataFrame())
    if not ai_df.empty and "주차" in ai_df.columns:
        ai_df = ai_df.copy()

        # 컬럼 찾기 (줄바꿈 제거 후 매칭)
        cols_map = {}
        for c in ai_df.columns:
            cl = str(c).replace("\n", "").replace(" ", "").strip()
            if cl == "회원수": cols_map["회원수"] = c
            elif cl == "인트로": cols_map["인트로율"] = c
            elif "인트로" in cl and "단계" in cl and "회원" in cl and "건수" not in cl: cols_map["인트로수"] = c
            elif cl == "프로그램완료" and "회원" not in cl: cols_map["완료율"] = c
            elif "프로그램" in cl and "완료" in cl and "회원" in cl and "건수" not in cl: cols_map["완료수"] = c
            elif cl == "서비스": cols_map["서비스율"] = c
            elif "서비스" in cl and "제안" in cl and "단계" in cl and "회원" in cl and "건수" not in cl: cols_map["서비스수"] = c

        # 숫자 변환 + NaN → 0
        for k, c in cols_map.items():
            if c in ai_df.columns:
                ai_df[c] = ai_df[c].apply(safe_numeric).fillna(0)

        # 이상 데이터 제거 (주차 형식이 아닌 행)
        ai_df = ai_df[ai_df["주차"].astype(str).str.match(r"^\d{2}-\d{2}$", na=False)]
        # 주차 정리 + X축 짧게
        ai_chart = shorten_dates_in_df(ai_df, "주차")

        # 26-08~10 데이터 없는 주차 표시를 위한 처리
        existing_weeks = set(ai_chart["주차"].tolist())
        missing_weeks = [w for w in ["26-08", "26-09", "26-10"] if w not in existing_weeks]

        tab5, tab1, tab2, tab3, tab4 = st.tabs(["🏛 지자체별 비교", "참여율 추이 (%)", "참여 인원 (명)", "월별 추이", "상세 데이터"])

        with tab1:
            # 인트로율 + 서비스율 + 프로그램 완료율
            fig = go.Figure()

            if "인트로율" in cols_map:
                fig.add_trace(go.Scatter(
                    x=ai_chart["주차"], y=ai_chart[cols_map["인트로율"]],
                    name="인트로 참여율 (전화 수신)",
                    mode="lines+markers",
                    line=dict(color="#7B1FA2", width=2.5),
                    hovertemplate="<b>%{x}</b><br>인트로: %{y:.1f}%<extra></extra>"
                ))
            if "서비스율" in cols_map:
                fig.add_trace(go.Scatter(
                    x=ai_chart["주차"], y=ai_chart[cols_map["서비스율"]],
                    name="서비스 이용율",
                    mode="lines+markers",
                    line=dict(color="#00897B", width=2.5),
                    hovertemplate="<b>%{x}</b><br>서비스: %{y:.1f}%<extra></extra>"
                ))
            if "완료율" in cols_map:
                fig.add_trace(go.Scatter(
                    x=ai_chart["주차"], y=ai_chart[cols_map["완료율"]],
                    name="프로그램 완료율",
                    mode="lines+markers",
                    line=dict(color="#E91E63", width=2.5),
                    hovertemplate="<b>%{x}</b><br>완료: %{y:.1f}%<extra></extra>"
                ))

            # 지자체 구분 annotation
            fig.add_annotation(x="26-07", y=55, text="← 독거노인지원종합센터 | 데이터 없음 (26-08~10) | 삼척시청 →",
                               showarrow=False, font=dict(size=9, color="#999"),
                               bgcolor="rgba(255,255,255,0.8)", bordercolor="#ccc", borderwidth=1)

            # 각 주차별 회원수 표시 (X축 위에 annotation)
            if "회원수" in cols_map:
                for _, row in ai_chart.iterrows():
                    w = row["주차"]
                    m = int(row[cols_map["회원수"]])
                    if m > 0:
                        fig.add_annotation(
                            x=w, y=0, yref="y", yshift=-20,
                            text=f"{m}명", showarrow=False,
                            font=dict(size=8, color="#666"),
                        )

            fig.update_layout(
                title="AI 생활지원사 참여율 추이 (인트로 → 서비스 → 프로그램 완료)",
                height=480, hovermode="x unified",
                xaxis=dict(type="category", title=""),
                yaxis=dict(title="참여율 (%)", range=[-5, 60]),
                legend=LEGEND_BELOW, margin=dict(t=40, b=80),
            )
            st.plotly_chart(fig, use_container_width=True)

        with tab2:
            # 인원수 바 차트
            fig2 = make_subplots(specs=[[{"secondary_y": True}]])

            if "회원수" in cols_map:
                fig2.add_trace(go.Scatter(
                    x=ai_chart["주차"], y=ai_chart[cols_map["회원수"]],
                    name="전체 회원수", mode="lines+markers",
                    line=dict(color="#9E9E9E", width=1, dash="dot"),
                    hovertemplate="%{y:,}명<extra>회원수</extra>"
                ), secondary_y=True)

            if "인트로수" in cols_map:
                fig2.add_trace(go.Bar(
                    x=ai_chart["주차"], y=ai_chart[cols_map["인트로수"]],
                    name="인트로 (전화 수신)", marker_color="#7B1FA2",
                    text=ai_chart[cols_map["인트로수"]].apply(lambda x: f"{int(x)}"),
                    textposition="outside", textfont=dict(size=8),
                    hovertemplate="%{y:,.0f}명<extra>인트로</extra>"
                ), secondary_y=False)

            if "서비스수" in cols_map:
                fig2.add_trace(go.Bar(
                    x=ai_chart["주차"], y=ai_chart[cols_map["서비스수"]],
                    name="서비스 이용", marker_color="#00897B",
                    hovertemplate="%{y:,.0f}명<extra>서비스</extra>"
                ), secondary_y=False)

            if "완료수" in cols_map:
                fig2.add_trace(go.Bar(
                    x=ai_chart["주차"], y=ai_chart[cols_map["완료수"]],
                    name="프로그램 완료", marker_color="#E91E63",
                    hovertemplate="%{y:,.0f}명<extra>완료</extra>"
                ), secondary_y=False)

            # 데이터 없는 구간 표시
            fig2.add_annotation(x="26-07", y=0, yref="paper", yshift=10,
                               text="← 독거노인 | 데이터 없음 (08~10) | 삼척시청 →",
                               showarrow=False, font=dict(size=9, color="#999"),
                               bgcolor="rgba(255,255,255,0.8)")

            fig2.update_layout(
                title="AI 생활지원사 참여 인원 추이",
                height=450, hovermode="x unified",
                barmode="group", xaxis=dict(type="category"),
                legend=LEGEND_BELOW, margin=dict(t=40, b=80),
                bargap=0.3,
            )
            fig2.update_yaxes(title_text="참여 인원 (명)", secondary_y=False)
            fig2.update_yaxes(title_text="전체 회원수", secondary_y=True)
            st.plotly_chart(fig2, use_container_width=True)

        with tab3:
            ai_monthly_df = data.get("ai_monthly", pd.DataFrame())
            if not ai_monthly_df.empty and "월" in ai_monthly_df.columns:
                am = ai_monthly_df.copy()
                fig_m = go.Figure()
                color_map = {"인트로참여율": "#7B1FA2", "서비스이용률": "#00897B", "프로그램완료율": "#E91E63"}
                label_map = {"인트로참여율": "인트로 참여율", "서비스이용률": "서비스 이용률", "프로그램완료율": "프로그램 완료율"}
                for col, color in color_map.items():
                    if col in am.columns:
                        fig_m.add_trace(go.Scatter(
                            x=am["월"], y=am[col], name=label_map[col],
                            mode="lines+markers+text",
                            line=dict(color=color, width=2.5),
                            text=am[col].apply(lambda v: f"{v:.1f}%" if v > 0 else ""),
                            textposition="top center",
                            textfont=dict(size=9),
                            hovertemplate=f"<b>%{{x}}</b><br>{label_map[col]}: %{{y:.1f}}%<extra></extra>"
                        ))
                fig_m.update_layout(
                    title="AI 생활지원사 월별 참여율 추이",
                    height=420, hovermode="x unified",
                    xaxis=dict(type="category", title=""),
                    yaxis=dict(title="참여율 (%)", range=[0, 100]),
                    legend=LEGEND_BELOW, margin=dict(t=40, b=80),
                )
                st.plotly_chart(fig_m, use_container_width=True)
                st.dataframe(am, use_container_width=True, height=300)
            else:
                st.info("월별 추이 데이터가 없습니다.")

        with tab4:
            st.dataframe(ai_df, use_container_width=True, height=400)

        with tab5:
            # ── 지자체별 비교 (신규 시트: gid=887906400) ──────────────────
            MUN_COLORS = {
                "삼척시청": "#1565C0",   # 파랑
                "양양군청": "#2E7D32",   # 초록
                "정선군청": "#E65100",   # 주황
            }

            ai_mun = data.get("ai_municipality", pd.DataFrame())

            if ai_mun.empty:
                st.info("지자체별 데이터가 없습니다. (gid=887906400)")
            else:
                mun_df    = ai_mun.copy()
                agg_df    = mun_df[mun_df["지자체"] == "통합"].copy()   # 통합 행
                mun_only  = mun_df[mun_df["지자체"] != "통합"].copy()   # 지자체별 행
                periods   = mun_only["기간"].unique().tolist()
                muns      = mun_only["지자체"].unique().tolist()

                # ── 지자체 계약 시작 정보 ─────────────────────────────────
                mun_info = (
                    mun_only.groupby("지자체", sort=False)
                    .agg(계약시작주차=("기간", "first"), 알람요일=("알람요일", "first"))
                    .reset_index()
                )
                # ── 지자체별 현황 카드 (계약정보 + 최신 실적 통합) ───────────────────
                # 계약 시작주차 lookup: mun_info에서 추출
                mun_start_map = {
                    row["지자체"]: (row["계약시작주차"] or "-")
                    for _, row in mun_info.iterrows()
                }
                latest_period = periods[-1] if periods else None
                if latest_period:
                    latest = mun_only[mun_only["기간"] == latest_period].copy()
                    st.markdown(
                        f"<div style='font-weight:700;font-size:0.95rem;"
                        f"color:#1E293B;margin:0.3rem 0 0.8rem'>"
                        f"📌 {latest_period} 기준 지자체별 현황</div>",
                        unsafe_allow_html=True,
                    )
                    cols_kpi = st.columns(len(latest))
                    for i, (_, row) in enumerate(latest.iterrows()):
                        mun = row["지자체"]
                        color = MUN_COLORS.get(mun, "#607D8B")
                        alarm = row.get("알람요일") or "미정"
                        intro_pct  = safe_numeric(row.get("intro(%)", 0))
                        svc_pct    = safe_numeric(row.get("service proposal(%)", 0))
                        prog_pct   = safe_numeric(row.get("program(%)", 0))
                        alarm_user = int(safe_numeric(row.get("receiveAlarmUserCount", 0)))
                        raw_period = mun_start_map.get(mun, "-")
                        start_date = raw_period.split("~")[0].strip() if "~" in raw_period else raw_period
                        with cols_kpi[i]:
                            st.markdown(
                                f"""<div style="background:{color};border-radius:14px;
                                    padding:1.2rem 1rem;color:white;text-align:center;
                                    box-shadow:0 4px 12px rgba(0,0,0,0.15)">
                                  <b style="font-size:1.05rem">{mun}</b><br>
                                  <div style="font-size:0.78rem;opacity:0.9;margin:0.3rem 0 0.5rem">
                                    📅 {start_date} &nbsp;|&nbsp; 🔔 {alarm} 알람
                                  </div>
                                  <hr style="border:none;border-top:1px solid rgba(255,255,255,0.35);margin:0 0 0.6rem">
                                  <div style="font-size:0.78rem;opacity:0.85;margin-bottom:0.2rem">
                                    📨 알람도달 {alarm_user}명
                                  </div>
                                  <div style="font-size:1.9rem;font-weight:900;line-height:1.1">{intro_pct:.0f}%</div>
                                  <div style="font-size:0.72rem;opacity:0.85;margin-bottom:0.5rem">📊 인트로 참여율</div>
                                  <div style="font-size:0.82rem">
                                    🛎️ 서비스 {svc_pct:.0f}% &nbsp;|&nbsp; ✅ 프로그램 {prog_pct:.0f}%
                                  </div>
                                </div>""",
                                unsafe_allow_html=True,
                            )

                st.markdown("---")

                # ── 주차별 참여율 추이: 지자체×지표 명도별 그룹 막대 (단일 차트) ──
                # 지자체별 3단계 명도: 인트로(진) / 서비스(중) / 프로그램(연)
                MUN_SHADES = {
                    "삼척시청": {
                        "intro(%)":            "#1565C0",   # 진파랑
                        "service proposal(%)": "#42A5F5",   # 중파랑
                        "program(%)":          "#90CAF9",   # 연파랑
                    },
                    "양양군청": {
                        "intro(%)":            "#1B5E20",   # 진초록
                        "service proposal(%)": "#43A047",   # 중초록
                        "program(%)":          "#A5D6A7",   # 연초록
                    },
                    "정선군청": {
                        "intro(%)":            "#BF360C",   # 진주황
                        "service proposal(%)": "#F4511E",   # 중주황
                        "program(%)":          "#FFAB91",   # 연주황
                    },
                }
                DEFAULT_SHADES = {
                    "intro(%)": "#607D8B",
                    "service proposal(%)": "#90A4AE",
                    "program(%)": "#CFD8DC",
                }
                METRIC_DEF = [
                    ("intro(%)",            "인트로 참여율"),
                    ("service proposal(%)", "서비스 제안율"),
                    ("program(%)",          "프로그램 완료율"),
                ]

                # ── 지자체별 개별 그래프 (x축 기간 공통 고정) ──────────────
                # 전체 기간 목록을 원래 순서대로 확정 → 모든 차트가 같은 x 눈금 사용
                all_periods = list(dict.fromkeys(mun_only["기간"].tolist()))  # 순서 보존 중복 제거

                for mun in muns:
                    sub = mun_only[mun_only["지자체"] == mun].copy()
                    if sub.empty:
                        continue
                    # 전체 기간 기준으로 reindex (없는 주차는 0)
                    sub = sub.set_index("기간").reindex(all_periods).reset_index()
                    sub["기간"] = pd.Categorical(sub["기간"], categories=all_periods, ordered=True)

                    alarm_day = mun_only[mun_only["지자체"] == mun]["알람요일"].iloc[0] \
                                if "알람요일" in mun_only.columns else ""
                    mun_label = f"{mun} ({alarm_day})" if alarm_day else mun
                    color_base = MUN_COLORS.get(mun, "#607D8B")
                    shades = MUN_SHADES.get(mun, DEFAULT_SHADES)

                    fig_m = go.Figure()
                    for m_col, m_label in METRIC_DEF:
                        vals = sub[m_col].apply(safe_numeric) if m_col in sub.columns else pd.Series([0]*len(sub))
                        color = shades.get(m_col, "#607D8B")
                        fig_m.add_trace(go.Bar(
                            x=sub["기간"],
                            y=vals,
                            name=m_label,
                            marker_color=color,
                            text=vals.apply(lambda v: f"{v:.0f}%" if pd.notna(v) and v > 0 else ""),
                            textposition="outside",
                            textfont=dict(size=15),
                            hovertemplate=f"<b>%{{x}}</b><br>{m_label}: <b>%{{y:.1f}}%</b><extra></extra>",
                        ))
                    fig_m.update_layout(
                        title=dict(text=mun_label, font=dict(size=16, color=color_base)),
                        height=380,
                        barmode="group",
                        bargap=0.2,
                        hovermode="x unified",
                        xaxis=dict(
                            type="category",
                            categoryorder="array",
                            categoryarray=all_periods,  # 모든 차트 동일 x축
                            title="",
                        ),
                        yaxis=dict(title="참여율 (%)", range=[0, 115]),
                        legend=dict(orientation="h", yanchor="top", y=-0.18,
                                    xanchor="center", x=0.5),
                        margin=dict(t=50, b=80),
                    )
                    st.plotly_chart(fig_m, use_container_width=True)

                # ── 상세 데이터 표 ─────────────────────────────────────────
                with st.expander("상세 데이터 보기"):
                    st.dataframe(mun_df, use_container_width=True, height=350)

    else:
        st.info("AI 생활지원사 데이터가 없습니다.")


# ============================================================
# 📊 콘텐츠·생활·날씨
# ============================================================
elif page == "🩺 8.건강상담":
    st.markdown('<div class="section-header">🩺 건강상담</div>', unsafe_allow_html=True)

    p_start, p_end = page_week_range_selector("health", weeks)

    health_df = data.get("건강상담", pd.DataFrame())
    if not health_df.empty:
        # 주차 기간 필터링
        for c in health_df.columns:
            if "주차" in str(c):
                health_df = filter_by_week_range(health_df, c, p_start, p_end, weeks)
                break
        # 주차 컬럼 찾기
        week_col = None
        for c in health_df.columns:
            if "주차" in str(c):
                week_col = c
                break

        # 숫자 컬럼 변환
        num_cols = []
        for c in health_df.columns:
            if c not in [week_col, "시작일"] and c is not None:
                health_df[c] = health_df[c].apply(safe_numeric)
                if health_df[c].sum() > 0:
                    num_cols.append(c)

        hc_mun = data.get("건강상담지자체", pd.DataFrame())
        tab1, tab2, tab3 = st.tabs(["이용 추이", "지자체별 서비스 현황", "상세 데이터"])

        with tab1:
            if week_col:
                health_chart = shorten_dates_in_df(health_df, week_col)
            else:
                health_chart = health_df.copy()

            # 이용 건수(막대) + 전체이용비중(꺾은선) 이중축
            if week_col and "실제 이용 건수" in health_chart.columns:
                plot_bar_rate_dual(
                    health_chart, week_col,
                    bar_col="실제 이용 건수",   bar_label="실제 이용 건수", bar_color="#26C6DA",
                    line_col="전체이용비중",    line_label="전체 이용비중", line_color="#FF6F00",
                    title="건강상담 이용 건수 & 전체 이용비중",
                    bar_unit="건", line_unit="%",
                )

            if week_col and "전화버튼클릭건수" in health_chart.columns:
                fig = go.Figure()
                if "메뉴클릭건수" in health_chart.columns:
                    fig.add_trace(go.Scatter(x=health_chart[week_col], y=health_chart["메뉴클릭건수"],
                                             name="메뉴클릭", mode="lines+markers", line=dict(color="#2196F3")))
                fig.add_trace(go.Scatter(x=health_chart[week_col], y=health_chart["전화버튼클릭건수"],
                                         name="전화버튼클릭", mode="lines+markers", line=dict(color="#FF6F00")))
                if "아웃바운드\n성공건수" in health_chart.columns:
                    fig.add_trace(go.Scatter(x=health_chart[week_col], y=health_chart["아웃바운드\n성공건수"],
                                             name="아웃바운드 성공", mode="lines+markers", line=dict(color="#00C853")))
                fig.update_layout(title="건강상담 클릭/통화 추이", height=350,
                                  hovermode="x unified", xaxis=dict(type="category"),
                                  legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5))
                st.plotly_chart(fig, use_container_width=True)

        # ── 탭 2: 지자체별 서비스 유형별 현황 ────────────────────────────
        with tab2:
            # 캐시 데이터가 비어있으면 직접 재시도
            if hc_mun.empty:
                with st.spinner("건강상담 지자체 데이터 로딩 중..."):
                    try:
                        from sheets_data import fetch_sheet, get_health_consult_by_municipality as _get_hc_mun
                        _raw = fetch_sheet("867975933")
                        if not _raw.empty:
                            hc_mun = _get_hc_mun({"건강상담지자체": _raw})
                    except Exception as _e:
                        st.error(f"데이터 로딩 오류: {_e}")
            if hc_mun.empty:
                st.info("건강상담 지자체 서비스 이용 데이터가 아직 없습니다.")
            else:
                SERVICE_COLS_HC = [c for c in ["전문의료진상담", "병원안내", "일반상담", "진료예약"]
                                   if c in hc_mun.columns]
                SERVICE_COLORS_HC = {
                    "전문의료진상담": "#26C6DA",
                    "병원안내":       "#5B73E8",
                    "일반상담":       "#66BB6A",
                    "진료예약":       "#FF6F00",
                }

                # 일별 날짜 → ISO 주차 레이블 추가 (2026-04-13 → "26-16")
                hc_mun = hc_mun.copy()
                hc_mun["주차"] = hc_mun["날짜"].apply(date_to_week_label)

                # ── 탭1과 동일한 X축(주차 범위) 계산 ──
                if p_start and p_end and weeks:
                    _s = weeks.index(p_start) if p_start in weeks else 0
                    _e = weeks.index(p_end)   if p_end   in weeks else len(weeks) - 1
                    week_order = weeks[_s:_e + 1]           # 탭1과 동일한 전체 주차 목록
                    hc_filtered = hc_mun[hc_mun["주차"].isin(set(week_order))].copy()
                else:
                    _tmp = hc_mun.sort_values("날짜")
                    week_order = list(dict.fromkeys(_tmp["주차"].tolist()))
                    hc_filtered = hc_mun.copy()

                if hc_filtered.empty and not hc_mun.empty:
                    hc_filtered = hc_mun.copy()             # fallback: 전체 데이터

                # ① 주차별 서비스 유형 스택 바 (전체 합산 — 일별 데이터를 주 단위로 집계)
                hc_week = hc_filtered.groupby("주차")[SERVICE_COLS_HC].sum().reset_index()
                # 탭1 X축과 동일하게: 데이터 없는 주차도 0으로 채워 표시
                if week_order:
                    _all_w = pd.DataFrame({"주차": week_order})
                    hc_week = _all_w.merge(hc_week, on="주차", how="left").fillna(0)
                    hc_week["주차"] = pd.Categorical(hc_week["주차"], categories=week_order, ordered=True)
                    hc_week = hc_week.sort_values("주차")

                fig_stack = go.Figure()
                for sc in SERVICE_COLS_HC:
                    fig_stack.add_trace(go.Bar(
                        x=hc_week["주차"].astype(str), y=hc_week[sc], name=sc,
                        marker_color=SERVICE_COLORS_HC.get(sc, "#999"),
                        text=hc_week[sc].apply(lambda v: f"{int(v)}" if v > 0 else ""),
                        textposition="inside", textfont=dict(size=11, color="white"),
                        hovertemplate=f"<b>%{{x}}</b><br>{sc}: %{{y:,}}건<extra></extra>",
                    ))
                fig_stack.update_layout(
                    barmode="stack",
                    title="주차별 건강상담 서비스 유형별 이용건수",
                    height=400, hovermode="x unified",
                    xaxis=dict(type="category", tickangle=-45, tickfont=dict(size=11)),
                    yaxis=dict(title="이용건수"),
                    legend=LEGEND_BELOW, margin=dict(t=45, b=90),
                )
                st.plotly_chart(fig_stack, use_container_width=True)

                # ② 실제 데이터가 있는 가장 최근 주차 기준 지자체별 현황
                if not hc_filtered.empty and week_order:
                    _weeks_with_data = [w for w in reversed(week_order)
                                        if w in hc_filtered["주차"].values]
                    latest_week = _weeks_with_data[0] if _weeks_with_data else None
                    if latest_week:
                        latest_hc = hc_filtered[hc_filtered["주차"] == latest_week].groupby("지자체")[SERVICE_COLS_HC].sum().reset_index()
                        latest_hc["합계"] = latest_hc[SERVICE_COLS_HC].sum(axis=1)
                        latest_hc = latest_hc[latest_hc["합계"] > 0].sort_values("합계", ascending=True)
                        if not latest_hc.empty:
                            st.markdown(f"**📌 {latest_week} 기준 지자체별 서비스 이용현황** (이용 있는 지자체만 표시)")
                            fig_mun_hc = go.Figure()
                            for sc in SERVICE_COLS_HC:
                                fig_mun_hc.add_trace(go.Bar(
                                    y=latest_hc["지자체"], x=latest_hc[sc],
                                    name=sc, orientation="h",
                                    marker_color=SERVICE_COLORS_HC.get(sc, "#999"),
                                    text=latest_hc[sc].apply(lambda v: f"{int(v)}" if v > 0 else ""),
                                    textposition="inside", textfont=dict(size=11, color="white"),
                                    hovertemplate=f"<b>%{{y}}</b><br>{sc}: %{{x:,}}건<extra></extra>",
                                ))
                            fig_mun_hc.update_layout(
                                barmode="stack",
                                title=f"지자체별 서비스 유형별 이용건수 ({latest_week})",
                                height=max(300, len(latest_hc) * 38),
                                xaxis=dict(title="이용건수"),
                                yaxis=dict(title=""),
                                legend=LEGEND_BELOW, margin=dict(t=45, b=90),
                            )
                            st.plotly_chart(fig_mun_hc, use_container_width=True)
                        else:
                            st.info(f"📭 {latest_week} 주차에는 이용 데이터가 없습니다.")

                # ③ 지자체별 주차별 라인 차트 (서비스 유형 선택 — 주 단위 집계)
                st.markdown("---")
                sel_svc = st.selectbox("📊 서비스 유형별 지자체 추이",
                                       SERVICE_COLS_HC + ["합계"],
                                       key="hc_svc_select")
                svc_col = sel_svc if sel_svc != "합계" else None
                _svc_agg = sel_svc if sel_svc in SERVICE_COLS_HC else SERVICE_COLS_HC
                hc_line = hc_filtered.groupby(["주차", "지자체"])[SERVICE_COLS_HC].sum().reset_index()
                hc_line["합계"] = hc_line[SERVICE_COLS_HC].sum(axis=1)
                if sel_svc in hc_line.columns:
                    # 선택 서비스에서 값이 하나라도 있는 지자체만 표시 (hover 정리)
                    _nonzero = hc_line.groupby("지자체")[sel_svc].sum()
                    _active_muns = _nonzero[_nonzero > 0].index.tolist()
                    hc_line = hc_line[hc_line["지자체"].isin(_active_muns)]
                    # 주차 정렬 순서 유지 (이미 계산된 week_order 재사용)
                    week_order2 = week_order
                    hc_line["주차"] = pd.Categorical(hc_line["주차"], categories=week_order2, ordered=True)
                    hc_line = hc_line.sort_values(["주차", "지자체"])
                    fig_line = px.line(
                        hc_line, x="주차", y=sel_svc, color="지자체",
                        markers=True,
                        color_discrete_sequence=px.colors.qualitative.Set2,
                        title=f"지자체별 {sel_svc} 주간 추이",
                    )
                    fig_line.update_layout(
                        height=430, hovermode="x unified",
                        xaxis=dict(type="category", tickangle=-45, tickfont=dict(size=11)),
                        yaxis=dict(title="이용건수"),
                        legend=LEGEND_BELOW, margin=dict(t=45, b=100),
                    )
                    fig_line.update_traces(
                        hovertemplate="<b>%{x}</b><br>%{y:,}건<extra>%{fullData.name}</extra>"
                    )
                    st.plotly_chart(fig_line, use_container_width=True)

                # ④ 원본 테이블
                with st.expander("📋 원본 데이터 보기"):
                    st.dataframe(hc_filtered.sort_values(["날짜", "지자체"]),
                                 use_container_width=True, height=350)

        with tab3:
            st.dataframe(health_df, use_container_width=True, height=400)
    else:
        st.info("건강상담 데이터가 없습니다.")


# ============================================================
# 💬 생활상담
# ============================================================
elif page == "💬 9.생활상담":
    st.markdown('<div class="section-header">💬 생활상담</div>', unsafe_allow_html=True)

    p_start, p_end = page_week_range_selector("life", weeks)

    life_df = data.get("생활상담", pd.DataFrame())
    if not life_df.empty:
        for c in life_df.columns:
            if "주차" in str(c):
                life_df = filter_by_week_range(life_df, c, p_start, p_end, weeks)
                break
        week_col = None
        for c in life_df.columns:
            if "주차" in str(c):
                week_col = c
                break

        num_cols = []
        for c in life_df.columns:
            if c not in [week_col, "시작일"] and c is not None:
                life_df[c] = life_df[c].apply(safe_numeric)
                if life_df[c].sum() > 0:
                    num_cols.append(c)

        tab1, tab2 = st.tabs(["이용 추이", "상세 데이터"])

        with tab1:
            if week_col:
                life_chart = shorten_dates_in_df(life_df, week_col)

                # 차트1: 메뉴클릭건수(막대) + 전체이용비중(꺾은선) — 건강상담과 동일 양식
                if "메뉴클릭건수" in life_chart.columns:
                    plot_bar_rate_dual(
                        life_chart, week_col,
                        bar_col="메뉴클릭건수",  bar_label="메뉴클릭건수", bar_color="#8D6E63",
                        line_col="전체이용비중", line_label="전체 이용비중", line_color="#FF6F00",
                        title="생활상담 메뉴클릭건수 & 전체 이용비중",
                        bar_unit="건", line_unit="%",
                    )

                # 차트2: 전화버튼 클릭자수 — 건강상담과 동일 양식
                phone_cols = [c for c in life_chart.columns
                              if "전화" in str(c) and c != "전체이용비중"]
                if phone_cols:
                    fig2 = go.Figure()
                    phone_colors = ["#D32F2F", "#E57373", "#FF8A65"]
                    for i, c in enumerate(phone_cols):
                        fig2.add_trace(go.Scatter(
                            x=life_chart[week_col], y=life_chart[c],
                            name=c, mode="lines+markers",
                            line=dict(color=phone_colors[i % len(phone_colors)])
                        ))
                    fig2.update_layout(
                        title="📞 전화버튼 클릭자수 추이", height=320,
                        hovermode="x unified", xaxis=dict(type="category"),
                        legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5)
                    )
                    st.plotly_chart(fig2, use_container_width=True)

        with tab2:
            st.dataframe(life_df, use_container_width=True, height=400)
    else:
        st.info("생활상담 데이터가 없습니다.")


# ============================================================
# 📝 자동 보고서
# ============================================================
elif page == "📝 자동 보고서":
    st.markdown('<div class="section-header">📝 자동 생성 주간 운영 보고서</div>', unsafe_allow_html=True)

    if selected_week:
        summary = cached_week_summary(sheets, data, selected_week)
        prev_week = get_prev_week(selected_week)
        prev_summary = cached_week_summary(sheets, data, prev_week) if prev_week else {}
        heatmap_df = cached_heatmap(data, selected_week)

        # 4주 트렌드 데이터 수집
        week_idx = weeks.index(selected_week) if selected_week in weeks else -1
        trend_4w = []
        for i in range(4):
            idx = week_idx - i
            if idx >= 0:
                w = weeks[idx]
                s = cached_week_summary(sheets, data, w)
                trend_4w.append({"주차": w, **s})
        trend_4w.reverse()

        # 핵심 지표 추출 (이용자현황 시트 기반 — Summary/회원가입 페이지와 통일)
        reg_report = data.get("registration", pd.DataFrame())
        if not reg_report.empty and "가입완료" in reg_report.columns:
            total_users = int(reg_report["가입완료"].apply(safe_numeric).sum())
        else:
            total_users = summary.get("가입완료합계", 0)
        prev_total = prev_summary.get("가입완료합계", total_users)
        user_delta = total_users - prev_total
        checkin_rate = summary.get("안부체크율", 0)
        prev_checkin = prev_summary.get("안부체크율", 0)
        checkin_delta = checkin_rate - prev_checkin

        # 히트맵 기반 상태 분석
        focus_list = []
        caution_list = []
        excellent_list = []
        if not heatmap_df.empty:
            status_counts = heatmap_df["종합상태"].value_counts()
            focus_df = heatmap_df[heatmap_df["종합상태"] == "집중관리"]
            caution_df = heatmap_df[heatmap_df["종합상태"] == "주의관리"]
            excellent_df = heatmap_df[heatmap_df["종합상태"] == "우수사례"]
            focus_list = focus_df["지자체명"].tolist()
            caution_list = caution_df["지자체명"].tolist()
            excellent_list = excellent_df["지자체명"].tolist()
            total_active = len(heatmap_df)
        else:
            status_counts = pd.Series()
            total_active = 0

        # 4주 트렌드 분석
        trend_text = ""
        if len(trend_4w) >= 4:
            users_4w = [t.get("가입완료합계", 0) for t in trend_4w]
            checkin_4w = [t.get("안부체크율", 0) for t in trend_4w]
            # 가입자 추이
            if all(users_4w[i] <= users_4w[i+1] for i in range(3)):
                trend_text += "- 가입자 수 **4주 연속 증가** 추세로, 서비스 확산이 안정적으로 진행 중입니다.\n"
            elif all(users_4w[i] >= users_4w[i+1] for i in range(3)):
                trend_text += "- 가입자 수 **4주 연속 감소** 추세로, 이탈 원인 분석이 필요합니다.\n"
            else:
                trend_text += f"- 가입자 수 4주간 {users_4w[0]:,.0f}명 → {users_4w[-1]:,.0f}명으로 변동이 있습니다.\n"
            # 안부체크율 추이
            if all(checkin_4w[i] <= checkin_4w[i+1] for i in range(3) if checkin_4w[i] > 0):
                trend_text += "- 안부체크율 **4주 연속 상승**으로, 이용자 참여도가 개선되고 있습니다.\n"
            elif all(checkin_4w[i] >= checkin_4w[i+1] for i in range(3) if checkin_4w[i] > 0):
                trend_text += "- 안부체크율 **4주 연속 하락**으로, 참여 독려 방안 검토가 필요합니다.\n"
            elif checkin_4w[-1] > 0:
                trend_text += f"- 안부체크율 4주간 {checkin_4w[0]:.1f}% → {checkin_4w[-1]:.1f}%로 변동이 있습니다.\n"

        # 보고서 생성
        report = f"""## 와플랫 공공 지표 주간 운영 보고서

### {selected_week}주차 ({summary.get('시작일', '')})

---

### 1. 전체 운영 현황

| 항목 | {selected_week}주차 | {prev_week or '-'}주차 | 전주 대비 |
|------|---------|------|------|
| 전체 가입자 | **{total_users:,.0f}명** | {prev_total:,.0f}명 | {user_delta:+,.0f}명 ({(user_delta/prev_total*100) if prev_total else 0:+.1f}%) |
| 안부체크율 | **{checkin_rate:.1f}%** | {prev_checkin:.1f}% | {checkin_delta:+.1f}%p |
| 운영 지자체 수 | **{total_active}개** | - | - |

**운영 의의:** 전체 가입자 {total_users:,.0f}명 기준, {'가입자가 전주 대비 증가하여 서비스 확산이 순조롭습니다.' if user_delta > 0 else '가입자가 전주 대비 감소하여 이탈 관리에 주의가 필요합니다.' if user_delta < 0 else '가입자 수가 전주와 동일합니다.'}

---

### 2. 4주 트렌드 분석

| 주차 | 가입자 수 | 안부체크율 |
|------|----------|----------|
"""
        for t in trend_4w:
            w = t.get("주차", "")
            u = t.get("가입완료합계", 0)
            c = t.get("안부체크율", 0)
            marker = " ← **이번 주**" if w == selected_week else ""
            report += f"| {w} | {u:,.0f}명 | {c:.1f}% |{marker}\n"

        report += f"""
**트렌드 인사이트:**
{trend_text if trend_text else '- 4주 데이터가 충분하지 않아 트렌드 분석이 제한적입니다.'}

---

### 3. 지자체 운영 상태 분포

| 상태 | 개수 | 비중 |
|------|------|------|
| 🔴 집중관리 | {status_counts.get('집중관리', 0)}개 | {(status_counts.get('집중관리', 0)/total_active*100) if total_active else 0:.0f}% |
| 🟡 주의관리 | {status_counts.get('주의관리', 0)}개 | {(status_counts.get('주의관리', 0)/total_active*100) if total_active else 0:.0f}% |
| ⚪ 정상 | {status_counts.get('정상', 0)}개 | {(status_counts.get('정상', 0)/total_active*100) if total_active else 0:.0f}% |
| 🟢 우수사례 | {status_counts.get('우수사례', 0)}개 | {(status_counts.get('우수사례', 0)/total_active*100) if total_active else 0:.0f}% |

"""
        # 운영 의의
        focus_pct = (status_counts.get('집중관리', 0) / total_active * 100) if total_active else 0
        normal_pct = ((status_counts.get('정상', 0) + status_counts.get('우수사례', 0)) / total_active * 100) if total_active else 0
        if focus_pct > 30:
            report += f"**운영 의의:** 집중관리 지자체가 전체의 {focus_pct:.0f}%로 높은 수준입니다. 지자체별 맞춤 지원 전략이 시급합니다.\n\n"
        elif normal_pct >= 70:
            report += f"**운영 의의:** 정상+우수 지자체가 {normal_pct:.0f}%로 전반적으로 안정적인 운영 상태입니다.\n\n"
        else:
            report += f"**운영 의의:** 집중관리 {focus_pct:.0f}% / 정상+우수 {normal_pct:.0f}%로, 하위 지자체 관리에 집중이 필요합니다.\n\n"

        # 집중관리 지자체 상세
        if focus_list:
            report += "---\n\n### 4. 집중관리 필요 지자체\n\n"
            report += "| 지자체 | 가입율 | 앱삭제율 | 조치 사항 |\n"
            report += "|--------|--------|---------|----------|\n"
            for _, r in focus_df.iterrows():
                reg_r = r.get("가입완료율", 0)
                del_r = r.get("앱삭제율", 0)
                action = ""
                if reg_r < 50:
                    action += "가입 촉진 필요. "
                if del_r >= 15:
                    action += "이탈 원인 파악. "
                if not action:
                    action = "종합 모니터링 강화."
                report += f"| **{r['지자체명']}** | {reg_r:.0f}% | {del_r:.1f}% | {action} |\n"
            report += "\n"

        # 우수사례
        if excellent_list:
            report += "---\n\n### 5. 우수사례 지자체 (벤치마킹 대상)\n\n"
            for _, r in excellent_df.iterrows():
                report += f"- **{r['지자체명']}**: 가입율 {r.get('가입완료율', 0):.0f}%, 앱삭제율 {r.get('앱삭제율', 0):.1f}% — 운영 노하우 공유 권장\n"
            report += "\n"

        # 핵심 액션 아이템
        report += "---\n\n### 6. 금주 핵심 액션 아이템\n\n"
        action_num = 1
        if focus_list:
            for name in focus_list[:5]:
                report += f"{action_num}. **{name}** 담당자 연락 및 현황 파악\n"
                action_num += 1
        if caution_list:
            report += f"{action_num}. 주의관리 지자체({', '.join(caution_list[:3])}) 모니터링 강화\n"
            action_num += 1
        if checkin_delta < -3:
            report += f"{action_num}. 안부체크율 {abs(checkin_delta):.1f}%p 하락 → 참여 독려 캠페인 검토\n"
            action_num += 1
        if excellent_list:
            report += f"{action_num}. 우수사례({', '.join(excellent_list[:3])}) 운영 노하우 정리 및 공유\n"
            action_num += 1
        if action_num == 1:
            report += "- 특별 조치 사항 없음. 정상 운영 유지.\n"

        report += f"""
---

### 7. 종합 운영 소견

{selected_week}주차 와플랫 공공 서비스는 전체 {total_active}개 지자체, {total_users:,.0f}명의 가입자를 대상으로 운영되고 있습니다. """

        if user_delta > 0 and checkin_delta >= 0:
            report += f"가입자 수와 안부체크율이 모두 전주 대비 개선되어 긍정적인 운영 흐름을 보이고 있습니다. "
        elif user_delta > 0 and checkin_delta < 0:
            report += f"가입자는 증가했으나 안부체크율이 {abs(checkin_delta):.1f}%p 하락하여, 신규 가입자의 서비스 활용도를 높이는 방안이 필요합니다. "
        elif user_delta <= 0 and checkin_delta >= 0:
            report += f"안부체크율은 개선되었으나 가입자 감소({user_delta:+,.0f}명)에 대한 이탈 관리가 필요합니다. "
        else:
            report += f"가입자와 안부체크율이 모두 하락하여, 전반적인 서비스 활성화 전략 검토가 필요합니다. "

        if focus_list:
            report += f"특히 집중관리 대상 {len(focus_list)}개 지자체에 대한 즉각적인 대응이 권장됩니다."
        else:
            report += "모든 지자체가 정상 범위 내에서 운영되고 있어 현 운영 체계를 유지하면 됩니다."

        report += f"""

---
*자동 생성: 와플랫 공공 지표 대시보드 | {selected_week}주차*
"""

        st.markdown(report)

        # 다운로드
        st.download_button(
            "📥 보고서 다운로드 (Markdown)",
            data=report.encode("utf-8"),
            file_name=f"와플랫_주간보고_{selected_week}.md",
            mime="text/markdown",
        )
    else:
        st.info("사이드바에서 주차를 선택해주세요.")


# ============================================================
# 📥 데이터 입력
# ============================================================
elif page == "📥 데이터 입력":
    st.markdown('<div class="section-header">📥 데이터 입력 - Postman 데이터 붙여넣기</div>', unsafe_allow_html=True)

    st.markdown("""
    <div class="insight-box">
    <b>사용법:</b> Postman에서 데이터를 복사(Ctrl+C)한 후, 아래 텍스트 영역에 붙여넣기(Ctrl+V)하세요.<br>
    헤더(컬럼명)가 포함된 상태로 붙여넣으면 자동으로 데이터 타입을 감지합니다.
    </div>
    """, unsafe_allow_html=True)

    # Step 1: 데이터 타입 + 날짜 선택
    from datetime import date as date_type, timedelta

    col1, col2, col3, col4 = st.columns([1.2, 0.8, 0.8, 1])
    with col1:
        data_type_input = st.selectbox(
            "데이터 종류",
            ["자동 감지"] + list(DATA_TYPES.keys()),
            help="Postman에서 어떤 항목의 데이터인지 선택하세요."
        )

    with col2:
        input_date_begin = st.date_input(
            "begin 날짜",
            value=date_type.today() - timedelta(days=2),
            help="Postman 쿼리의 begin 날짜",
        )

    with col3:
        input_date_end = st.date_input(
            "end 날짜",
            value=date_type.today(),
            help="Postman 쿼리의 end 날짜",
        )

    with col4:
        if data_type_input != "자동 감지":
            config = DATA_TYPES.get(data_type_input, {})
            st.caption(f"`{config.get('description', '')}`")
        period_str = f"{input_date_begin} ~ {input_date_end}"
        st.caption(f"조회 기간: **{period_str}**")

    # Step 2: 데이터 붙여넣기
    pasted = st.text_area(
        "Postman 데이터 붙여넣기",
        height=250,
        placeholder="Postman에서 복사한 데이터를 여기에 붙여넣으세요...\n(탭으로 구분된 테이블 데이터)",
        key="paste_area",
    )

    if pasted and pasted.strip():
        selected_type = "" if data_type_input == "자동 감지" else data_type_input
        result = process_pasted_data(pasted, selected_type)

        # 날짜 컬럼이 없으면 선택한 날짜(begin~end)를 자동 부여
        if result["success"]:
            for key in ["df_mapped", "df_preview", "df_raw"]:
                df_check = result.get(key, pd.DataFrame())
                if not df_check.empty:
                    if "date" not in df_check.columns:
                        df_check.insert(0, "date", str(input_date_begin))
                        result[key] = df_check
                    else:
                        mask = df_check["date"].isna() | (df_check["date"].astype(str).str.strip() == "")
                        if mask.any():
                            df_check.loc[mask, "date"] = str(input_date_begin)
                            result[key] = df_check
                    # date_end 컬럼 추가
                    if "date_end" not in df_check.columns:
                        df_check["date_end"] = str(input_date_end)
                        result[key] = df_check

            # 날짜 범위 업데이트
            if not result["date_range"]:
                result["date_range"] = f"{input_date_begin} ~ {input_date_end}"

            st.markdown("---")
            cols3 = st.columns(3)
            with cols3[0]:
                detected = result["detected_type"] or "알 수 없음"
                st.markdown(f"""
                <div class="metric-card-green">
                    <h3>데이터 타입</h3>
                    <h1 style="font-size:1rem;">{detected}</h1>
                </div>
                """, unsafe_allow_html=True)
            with cols3[1]:
                st.markdown(f"""
                <div class="metric-card">
                    <h3>데이터 건수</h3>
                    <h1>{result['row_count']}건</h1>
                </div>
                """, unsafe_allow_html=True)
            with cols3[2]:
                st.markdown(f"""
                <div class="metric-card-orange">
                    <h3>날짜 범위</h3>
                    <h1 style="font-size:1rem;">{result['date_range'] or '-'}</h1>
                </div>
                """, unsafe_allow_html=True)

            if result["agencies"]:
                agencies_str = ", ".join(result["agencies"][:10])
                if len(result["agencies"]) > 10:
                    agencies_str += f" 외 {len(result['agencies'])-10}개"
                st.markdown(f"**감지된 지자체:** {agencies_str}")

            st.markdown('<div class="section-header">미리보기</div>', unsafe_allow_html=True)
            preview_df = result["df_preview"]
            if not preview_df.empty:
                st.dataframe(preview_df, use_container_width=True, height=350)

            st.markdown("---")
            col_save, col_cancel = st.columns(2)
            with col_save:
                if st.button("저장하기", type="primary", use_container_width=True):
                    try:
                        dt = result["detected_type"]
                        df_to_save = result["df_mapped"]
                        if dt == "안부확인 (safetyCheck)" and not df_to_save.empty:
                            save_result = save_safety_check(df_to_save)
                            st.success(
                                f"저장 완료! {save_result['inserted']}건 저장"
                                + (f", {save_result['skipped']}건 스킵" if save_result['skipped'] else "")
                            )
                            if save_result["errors"]:
                                for err in save_result["errors"][:5]:
                                    st.warning(err)
                        elif dt and not df_to_save.empty:
                            save_result = save_generic(dt, df_to_save)
                            st.success(f"저장 완료! {save_result['inserted']}건 저장 (범용)")
                        else:
                            st.warning("데이터 타입을 선택해주세요.")
                    except Exception as e:
                        st.error(f"저장 실패: {e}")
            with col_cancel:
                if st.button("취소", use_container_width=True):
                    st.rerun()
        else:
            st.error(result["error"])

    # DB 현황
    st.markdown("---")
    st.markdown('<div class="section-header">저장된 데이터 현황</div>', unsafe_allow_html=True)

    stats = get_data_stats()
    total = sum(stats.values())
    st.markdown(f"**총 {total:,}건** 저장됨")

    stats_cols = st.columns(4)
    table_labels = {
        "raw_safety_check": "안부확인",
        "raw_cardiovascular": "심혈관",
        "raw_dualgo": "맞고",
        "raw_member": "회원",
        "raw_stress_check": "스트레스",
        "raw_ai_care": "AI케어",
        "raw_medicine": "복약",
        "raw_generic": "기타",
    }
    for i, (table, count) in enumerate(stats.items()):
        with stats_cols[i % 4]:
            label = table_labels.get(table, table)
            st.metric(label, f"{count:,}건")

    saved_dates = get_all_dates()
    if saved_dates:
        st.markdown('<div class="section-header">저장된 안부확인 데이터</div>', unsafe_allow_html=True)
        date_filter = st.selectbox("날짜 선택", saved_dates)
        if date_filter:
            saved_df = get_safety_check_data(date_from=date_filter, date_to=date_filter)
            if not saved_df.empty:
                display_cols = ["date", "agency_name", "target_user_count",
                                "complete_user_count", "안부체크율",
                                "uncheck_48hr_user_count", "48시간미확인률"]
                available = [c for c in display_cols if c in saved_df.columns]
                st.dataframe(saved_df[available], use_container_width=True)

    # ── 안부확인 CSV 직접 입력 ──
    st.markdown("---")
    st.markdown('<div class="section-header">📋 안부확인 CSV 데이터 직접 입력</div>', unsafe_allow_html=True)
    st.markdown("""
    <div class="insight-box">
    Postman에서 <b>safetyCheckIndicatorByDateList</b> 데이터를 복사하여 붙여넣으세요.<br>
    헤더(date, agencyName, safetyCheckAlarmSendCount...)가 포함된 TSV/CSV 형식이면 자동 파싱됩니다.<br>
    기존 데이터가 있으면 <b>같은 날짜+지자체는 자동으로 업데이트</b>됩니다.
    </div>
    """, unsafe_allow_html=True)

    csv_pasted = st.text_area(
        "안부확인 CSV 붙여넣기",
        height=200,
        placeholder="Postman에서 safetyCheckIndicatorByDateList 테이블을 복사하여 여기에 붙여넣으세요...",
        key="safety_csv_paste",
    )

    if csv_pasted and csv_pasted.strip():
        import io as _io
        try:
            # TSV 또는 CSV 자동 감지
            if "\t" in csv_pasted:
                csv_df = pd.read_csv(_io.StringIO(csv_pasted), sep="\t")
            else:
                csv_df = pd.read_csv(_io.StringIO(csv_pasted))

            # 컬럼 매핑
            col_remap = {}
            for c in csv_df.columns:
                cl = str(c).replace("\n", "").replace(" ", "").strip().lower()
                if "date" in cl and len(cl) < 10:
                    col_remap[c] = "date"
                elif "agencyname" in cl:
                    col_remap[c] = "agency_name"
                elif "safetycheckalarm" in cl and "send" in cl:
                    col_remap[c] = "alarm_send_count"
                elif "safetycheckconfirm" in cl:
                    col_remap[c] = "confirm_count"
                elif "safetychecktargetuser" in cl:
                    col_remap[c] = "target_user_count"
                elif "safetycheckcompleteuser" in cl:
                    col_remap[c] = "complete_user_count"
                elif "safetycheckimpossibleuser" in cl:
                    col_remap[c] = "impossible_user_count"
                elif "detectmotion" in cl:
                    col_remap[c] = "detect_motion_count"
                elif "aicarealarmtarget" in cl:
                    col_remap[c] = "ai_care_target_count"
                elif "aicarealarmgenerate" in cl:
                    col_remap[c] = "ai_care_generate_count"
                elif "aicarealarmresponse" in cl:
                    col_remap[c] = "ai_care_response_count"
                elif "safetycheckcallgenerate" in cl:
                    col_remap[c] = "call_generate_count"
                elif "safetycheckcallresponse" in cl:
                    col_remap[c] = "call_response_count"
                elif "safetyuncheck48hruser" in cl and "target" not in cl and "alarm" not in cl and "receive" not in cl:
                    col_remap[c] = "uncheck_48hr_user_count"
                elif "safetyuncheck48hrtarget" in cl:
                    col_remap[c] = "uncheck_48hr_target_count"
                elif "safetyuncheck48hralarmgenerate" in cl:
                    col_remap[c] = "uncheck_48hr_alarm_generate_count"
                elif "safetyuncheck48hralarmreceive" in cl:
                    col_remap[c] = "uncheck_48hr_alarm_receive_count"

            csv_df = csv_df.rename(columns=col_remap)

            # 미리보기
            st.markdown(f"**감지된 데이터: {len(csv_df)}건**")
            if "date" in csv_df.columns and "agency_name" in csv_df.columns:
                dates = csv_df["date"].dropna().unique()
                agencies = csv_df["agency_name"].dropna().unique()
                st.caption(f"날짜: {min(dates)} ~ {max(dates)} | 지자체: {len(agencies)}개")

                # 안부체크율 미리 계산
                preview = csv_df[["date", "agency_name"]].copy()
                if "target_user_count" in csv_df.columns and "complete_user_count" in csv_df.columns:
                    t = pd.to_numeric(csv_df["target_user_count"], errors="coerce").fillna(0)
                    c_val = pd.to_numeric(csv_df["complete_user_count"], errors="coerce").fillna(0)
                    preview["안부체크율"] = (c_val / t.replace(0, float("nan")) * 100).round(1).fillna(0)
                    preview["안부체크율"] = preview["안부체크율"].apply(lambda x: f"{x:.1f}%")
                st.dataframe(preview.head(20), use_container_width=True, height=300)

                if st.button("안부확인 데이터 저장", type="primary", use_container_width=True, key="save_csv"):
                    from local_db import get_connection as _get_conn
                    conn = _get_conn()
                    saved = 0
                    for _, row in csv_df.iterrows():
                        d = str(row.get("date", "")).strip()
                        a = str(row.get("agency_name", "")).strip()
                        if not d or d == "nan" or not a or a == "nan":
                            continue
                        def _si(col):
                            try:
                                v = row.get(col, 0)
                                if pd.isna(v) or str(v).strip() == "":
                                    return 0
                                return int(float(str(v).replace(",", "")))
                            except:
                                return 0
                        try:
                            conn.execute("""
                            INSERT INTO raw_safety_check (date, agency_name,
                                alarm_send_count, confirm_count, target_user_count,
                                complete_user_count, impossible_user_count, detect_motion_count,
                                ai_care_target_count, ai_care_generate_count, ai_care_response_count,
                                call_generate_count, call_response_count,
                                uncheck_48hr_user_count, uncheck_48hr_target_count,
                                uncheck_48hr_alarm_generate_count, uncheck_48hr_alarm_receive_count)
                            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                            ON CONFLICT(date, agency_name) DO UPDATE SET
                                alarm_send_count=excluded.alarm_send_count,
                                confirm_count=excluded.confirm_count,
                                target_user_count=excluded.target_user_count,
                                complete_user_count=excluded.complete_user_count,
                                impossible_user_count=excluded.impossible_user_count,
                                detect_motion_count=excluded.detect_motion_count,
                                ai_care_target_count=excluded.ai_care_target_count,
                                ai_care_generate_count=excluded.ai_care_generate_count,
                                ai_care_response_count=excluded.ai_care_response_count,
                                call_generate_count=excluded.call_generate_count,
                                call_response_count=excluded.call_response_count,
                                uncheck_48hr_user_count=excluded.uncheck_48hr_user_count,
                                uncheck_48hr_target_count=excluded.uncheck_48hr_target_count,
                                uncheck_48hr_alarm_generate_count=excluded.uncheck_48hr_alarm_generate_count,
                                uncheck_48hr_alarm_receive_count=excluded.uncheck_48hr_alarm_receive_count
                            """, (d, a,
                                _si("alarm_send_count"), _si("confirm_count"), _si("target_user_count"),
                                _si("complete_user_count"), _si("impossible_user_count"), _si("detect_motion_count"),
                                _si("ai_care_target_count"), _si("ai_care_generate_count"), _si("ai_care_response_count"),
                                _si("call_generate_count"), _si("call_response_count"),
                                _si("uncheck_48hr_user_count"), _si("uncheck_48hr_target_count"),
                                _si("uncheck_48hr_alarm_generate_count"), _si("uncheck_48hr_alarm_receive_count"),
                            ))
                            saved += 1
                        except:
                            pass
                    conn.commit()
                    conn.close()
                    st.success(f"안부확인 데이터 {saved}건 저장 완료! 🖐 안부확인 페이지에서 확인하세요.")
                    st.cache_data.clear()
            else:
                st.warning("date, agencyName 컬럼을 찾을 수 없습니다. Postman에서 safetyCheckIndicatorByDateList를 복사했는지 확인해주세요.")
        except Exception as e:
            st.error(f"파싱 실패: {e}")


# ============================================================
# ⚙ 지자체 설정
# ============================================================
# ============================================================
# 🗄 DB 뷰어
# ============================================================
elif page == "🗄 DB 뷰어":
    st.markdown('<div class="section-header">🗄 저장된 데이터 뷰어</div>', unsafe_allow_html=True)

    from local_db import get_connection as _db_conn, import_sheets_to_db
    import json as _json

    # Google Sheets → DB 동기화 버튼
    sync_col1, sync_col2 = st.columns([3, 1])
    with sync_col1:
        st.markdown("Google Sheets의 모든 데이터를 DB로 가져옵니다 (기존 데이터는 덮어쓰기)")
    with sync_col2:
        if st.button("🔄 Google Sheets → DB 동기화", type="primary"):
            with st.spinner("Google Sheets 데이터 임포트 중..."):
                try:
                    result = import_sheets_to_db(sheets, data)
                    st.success(f"✅ 총 {result['imported']:,}건 임포트 완료!")
                    for s in result.get("sheets", []):
                        st.caption(f"  - {s}")
                    st.cache_data.clear()
                except Exception as e:
                    st.error(f"임포트 실패: {e}")
    st.markdown("---")

    view_tab1, view_tab2 = st.tabs(["📊 통합 뷰 (날짜×지자체)", "🗃 테이블별 뷰"])

    with view_tab1:
        st.markdown("**날짜×지자체별 전체 지표 통합 뷰**")
        conn = _db_conn()

        # 1. 안부확인 데이터 (raw_safety_check) - 원본 + 계산 컬럼
        safety = pd.read_sql_query("""
            SELECT date as 날짜, agency_name as 지자체,
                   alarm_send_count as 안부체크_발송수,
                   confirm_count as 안부체크_응답수,
                   target_user_count as 전체회원,
                   complete_user_count as 안부확인_완료자,
                   impossible_user_count as 안부_미확인자,
                   detect_motion_count as 동작감지_이용자,
                   ai_care_generate_count as AI케어_발송수,
                   ai_care_response_count as AI케어_응답수,
                   call_generate_count as 안부콜_발송수,
                   call_response_count as 안부콜_응답수,
                   uncheck_48hr_user_count as 미확인48h_대상자,
                   uncheck_48hr_target_count as 미확인48h_알림대상,
                   ROUND(confirm_count * 100.0 / NULLIF(alarm_send_count, 0), 1) as 안부체크율,
                   ROUND(impossible_user_count * 100.0 / NULLIF(target_user_count, 0), 1) as 안부미확인률,
                   ROUND(uncheck_48hr_user_count * 100.0 / NULLIF(target_user_count, 0), 1) as 미확인48h률,
                   ROUND(ai_care_response_count * 100.0 / NULLIF(ai_care_generate_count, 0), 1) as AI케어응답률,
                   ROUND(call_response_count * 100.0 / NULLIF(call_generate_count, 0), 1) as 안부콜응답률
            FROM raw_safety_check
            ORDER BY date DESC, agency_name
        """, conn)

        # 2. generic 데이터 (심혈관, 스트레스, 복약, 맞고 등)
        generic = pd.read_sql_query("""
            SELECT data_type, date, agency_name, raw_json
            FROM raw_generic
            WHERE agency_name != 'ALL'
            ORDER BY date DESC, agency_name
        """, conn)
        conn.close()

        # generic 데이터를 피벗
        if not generic.empty:
            generic_rows = []
            for _, r in generic.iterrows():
                try:
                    val = _json.loads(r["raw_json"]).get("value", 0)
                except:
                    val = 0
                generic_rows.append({
                    "날짜": r["date"],
                    "지자체": r["agency_name"],
                    "지표": r["data_type"],
                    "값": val,
                })
            generic_df = pd.DataFrame(generic_rows)
            # 피벗: 날짜×지자체 행, 지표가 컬럼
            if not generic_df.empty:
                generic_pivot = generic_df.pivot_table(
                    index=["날짜", "지자체"], columns="지표", values="값", aggfunc="first"
                ).reset_index()
                generic_pivot.columns.name = None
            else:
                generic_pivot = pd.DataFrame()
        else:
            generic_pivot = pd.DataFrame()

        # 안부확인 + generic 통합
        if not safety.empty and not generic_pivot.empty:
            merged = pd.merge(safety, generic_pivot, on=["날짜", "지자체"], how="outer")
        elif not safety.empty:
            merged = safety
        elif not generic_pivot.empty:
            merged = generic_pivot
        else:
            merged = pd.DataFrame()

        if not merged.empty:
            merged = merged.sort_values(["날짜", "지자체"], ascending=[False, True])

            # 필터
            fcol1, fcol2, fcol3 = st.columns(3)
            with fcol1:
                all_dates = sorted(merged["날짜"].dropna().unique(), reverse=True)
                if all_dates:
                    date_sel = st.select_slider("날짜 범위", options=sorted(all_dates),
                                                value=(all_dates[-1], all_dates[0]), key="unified_dates")
                    merged = merged[(merged["날짜"] >= date_sel[0]) & (merged["날짜"] <= date_sel[1])]
            with fcol2:
                all_agencies = sorted(merged["지자체"].dropna().unique())
                sel_agencies = st.multiselect("지자체 필터", all_agencies, key="unified_agencies")
                if sel_agencies:
                    merged = merged[merged["지자체"].isin(sel_agencies)]
            with fcol3:
                st.metric("조회 건수", f"{len(merged):,}건")

            st.dataframe(merged, use_container_width=True, height=600)

            # CSV 다운로드
            csv_data = merged.to_csv(index=False).encode("utf-8-sig")
            st.download_button("📥 통합 데이터 CSV 다운로드", data=csv_data,
                               file_name=f"waflat_unified_{pd.Timestamp.now().strftime('%Y%m%d')}.csv",
                               mime="text/csv")
        else:
            st.info("저장된 데이터가 없습니다.")

    with view_tab2:
        conn = _db_conn()

        # 테이블 목록 + 건수
        tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name != 'sqlite_sequence'").fetchall()
        table_info = {}
        for t in tables:
            name = t[0]
            count = conn.execute(f"SELECT COUNT(*) FROM {name}").fetchone()[0]
            table_info[name] = count

        # 요약 카드
        st.markdown(f"**총 {sum(table_info.values()):,}건** 저장됨 ({len(table_info)}개 테이블)")

        tcols = st.columns(min(4, len(table_info)))
        table_labels = {
            "raw_safety_check": "🖐 안부확인",
            "raw_cardiovascular": "❤ 심혈관",
            "raw_dualgo": "🎮 맞고",
            "raw_member": "👥 회원",
            "raw_stress_check": "😰 스트레스",
            "raw_ai_care": "🤖 AI케어",
            "raw_medicine": "💊 복약",
            "raw_generic": "📦 기타",
            "agency_master": "🏛 지자체",
        }
        for i, (table, count) in enumerate(table_info.items()):
            with tcols[i % len(tcols)]:
                label = table_labels.get(table, table)
                st.metric(label, f"{count:,}건")

        st.markdown("---")

        # 테이블 선택
        selected_table = st.selectbox(
            "테이블 선택",
            list(table_info.keys()),
            format_func=lambda x: f"{table_labels.get(x, x)} ({table_info[x]:,}건)"
        )

        if selected_table:
            # 컬럼 정보
            col_info = conn.execute(f"PRAGMA table_info({selected_table})").fetchall()
            col_names = [c[1] for c in col_info]

            # 날짜 필터 (date 컬럼이 있으면)
            has_date = "date" in col_names
            has_agency = "agency_name" in col_names

            filter_cols = st.columns(3)
            where_clauses = []
            params = []

            if has_date:
                with filter_cols[0]:
                    dates = conn.execute(f"SELECT DISTINCT date FROM {selected_table} WHERE date IS NOT NULL ORDER BY date DESC").fetchall()
                    date_list = [d[0] for d in dates]
                    if date_list:
                        date_range = st.select_slider(
                            "날짜 범위",
                            options=date_list,
                            value=(date_list[-1], date_list[0]),
                            key="db_date_range"
                        )
                        where_clauses.append("date >= ? AND date <= ?")
                        params.extend([date_range[0], date_range[1]])

            if has_agency:
                with filter_cols[1]:
                    agencies = conn.execute(f"SELECT DISTINCT agency_name FROM {selected_table} WHERE agency_name IS NOT NULL ORDER BY agency_name").fetchall()
                    agency_list = [a[0] for a in agencies]
                    selected_agencies = st.multiselect("지자체 필터", agency_list, key="db_agency_filter")
                    if selected_agencies:
                        placeholders = ",".join(["?"] * len(selected_agencies))
                        where_clauses.append(f"agency_name IN ({placeholders})")
                        params.extend(selected_agencies)

            with filter_cols[2]:
                limit = st.number_input("표시 건수", min_value=10, max_value=5000, value=100, step=50, key="db_limit")

            # 쿼리 실행
            query = f"SELECT * FROM {selected_table}"
            if where_clauses:
                query += " WHERE " + " AND ".join(where_clauses)
            query += f" ORDER BY {'date DESC, ' if has_date else ''}id DESC LIMIT {limit}"

            result_df = pd.read_sql_query(query, conn, params=params)
            conn.close()

            st.markdown(f"**조회 결과: {len(result_df)}건**")
            st.dataframe(result_df, use_container_width=True, height=500)

            # CSV 다운로드
            csv_data = result_df.to_csv(index=False).encode("utf-8-sig")
            st.download_button(
                "📥 CSV 다운로드",
                data=csv_data,
                    file_name=f"{selected_table}_{pd.Timestamp.now().strftime('%Y%m%d')}.csv",
                    mime="text/csv",
                )


# ============================================================
# ⚙ 지자체 설정
# ============================================================
elif page == "⚙ 지자체 설정":
    st.markdown('<div class="section-header">⚙ 지자체 계약 관리</div>', unsafe_allow_html=True)

    st.markdown("""
    <div class="insight-box">
    지자체별 <b>계약 기간</b>과 <b>서비스 모델</b>(베이직/세이프/세이프플러스)을 관리합니다.<br>
    계약이 종료된 지자체는 비활성화하고, 새 계약은 추가할 수 있습니다.
    </div>
    """, unsafe_allow_html=True)

    # 현황 요약
    agency_sum = get_agency_summary()
    scols = st.columns(5)
    with scols[0]:
        st.metric("전체", f"{agency_sum['total']}개")
    with scols[1]:
        st.metric("활성", f"{agency_sum['active']}개")
    with scols[2]:
        st.metric("세이프 (관제)", f"{agency_sum['safe_count']}개")
    with scols[3]:
        st.metric("베이직", f"{agency_sum['basic_count']}개")
    with scols[4]:
        st.metric("종료/비활성", f"{agency_sum['inactive']}개")

    st.markdown("---")

    # 탭: 등록 / 목록
    tab_add, tab_list = st.tabs(["지자체 등록", "지자체 목록"])

    with tab_add:
        st.markdown("#### 새 지자체 등록")
        with st.form("add_agency_form", clear_on_submit=True):
            fc1, fc2 = st.columns(2)
            with fc1:
                new_name = st.text_input("지자체명 *", placeholder="예: 서초구청")
                new_model = st.selectbox("서비스 모델 *", ["safe", "safe_plus", "basic", "basic_plus"],
                                         format_func=lambda x: {"safe": "세이프", "safe_plus": "세이프 플러스", "basic": "베이직", "basic_plus": "베이직 플러스"}.get(x, x))
                new_target = st.number_input("대상자 수 (협약 인원)", min_value=0, value=0)
            with fc2:
                new_start = st.date_input("계약 시작일 *")
                new_end = st.date_input("계약 종료일 (없으면 비워두세요)",
                                        value=None, min_value=new_start)
                new_seq = st.number_input("agency_seq (Postman 코드)", min_value=0, value=0)

            fc3, fc4 = st.columns(2)
            with fc3:
                new_manager = st.text_input("담당자명", placeholder="")
                new_contact = st.text_input("담당자 연락처", placeholder="")
            with fc4:
                new_memo = st.text_area("메모", height=100, placeholder="재계약, 특이사항 등")

            submitted = st.form_submit_button("등록하기", type="primary", use_container_width=True)
            if submitted:
                if not new_name:
                    st.error("지자체명을 입력해주세요.")
                else:
                    try:
                        save_agency(
                            agency_name=new_name,
                            service_model=new_model,
                            contract_start=str(new_start),
                            contract_end=str(new_end) if new_end else "",
                            target_users=new_target,
                            agency_seq=new_seq,
                            manager_name=new_manager,
                            manager_contact=new_contact,
                            memo=new_memo,
                        )
                        st.success(f"'{new_name}' 등록 완료!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"등록 실패: {e}")

    with tab_list:
        show_inactive = st.checkbox("종료/비활성 지자체도 표시", value=False)
        agencies_df = get_agency_master()
        if not show_inactive:
            agencies_df = agencies_df[agencies_df["is_active"] == 1]

        if not agencies_df.empty:
            # 표시 컬럼 정리
            display = agencies_df[["agency_name", "service_model", "contract_start",
                                    "contract_end", "target_users", "is_active", "memo"]].copy()
            display["service_model"] = display["service_model"].map(
                {"safe": "세이프", "basic": "베이직"}).fillna(display["service_model"])
            display["is_active"] = display["is_active"].map({1: "활성", 0: "종료"})
            display.columns = ["지자체명", "모델", "계약시작", "계약종료", "대상자수", "상태", "메모"]

            st.dataframe(display, use_container_width=True, height=400)

            # 상태 변경
            st.markdown("---")
            st.markdown("#### 지자체 상태 변경")
            col_sel, col_act = st.columns([2, 1])
            with col_sel:
                agency_names = agencies_df["agency_name"].tolist()
                selected_agency = st.selectbox("지자체 선택", agency_names, key="agency_action_sel")

            if selected_agency:
                sel_row = agencies_df[agencies_df["agency_name"] == selected_agency].iloc[0]
                sel_start = sel_row["contract_start"]
                is_active = sel_row["is_active"] == 1

                with col_act:
                    st.markdown(f"**현재 상태:** {'활성' if is_active else '비활성'} | **모델:** {sel_row['service_model']}")

                bcol1, bcol2 = st.columns(2)
                with bcol1:
                    if is_active:
                        if st.button("비활성화 (계약 종료)", use_container_width=True):
                            deactivate_agency(selected_agency, sel_start)
                            st.success(f"'{selected_agency}' 비활성화 완료")
                            st.rerun()
                    else:
                        if st.button("다시 활성화", use_container_width=True):
                            activate_agency(selected_agency, sel_start)
                            st.success(f"'{selected_agency}' 활성화 완료")
                            st.rerun()
                with bcol2:
                    if st.button("완전 삭제", type="secondary", use_container_width=True):
                        delete_agency(selected_agency, sel_start)
                        st.warning(f"'{selected_agency}' 삭제 완료")
                        st.rerun()
        else:
            st.info("등록된 지자체가 없습니다. '지자체 등록' 탭에서 추가해주세요.")
