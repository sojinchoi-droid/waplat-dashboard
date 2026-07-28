# -*- coding: utf-8 -*-
"""주간 운영 리포트 스크립트 — 필요할 때 수동 실행.

이번 주 vs 전주를 비교해서 사업구분별로 크게 움직인 지표를 찾아내고,
그 변화에 어느 지자체가 얼마나 기여했는지까지 짚어주는 텍스트 리포트를 만든다.

사용법:
    python weekly_report.py
    python weekly_report.py --threshold 3          # %p 임계값 조정 (기본 5)
    python weekly_report.py --out report.md         # 파일로도 저장
"""
import argparse
import sys
from datetime import datetime

import pandas as pd
import sheets_data as sd

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8")
    except AttributeError:
        pass

safe_numeric = sd.safe_numeric

# ============================================================
# 사업구분 매핑 (app.py와 동일 — 계약 변경 시 양쪽 다 갱신 필요)
# ============================================================
BUSINESS_TYPE_MAP = {
    "진천군청": "통합돌봄", "음성군청": "통합돌봄", "증평군청": "통합돌봄",
    "충북사회서비스원": "통합돌봄", "양양군청": "통합돌봄", "양평군청": "통합돌봄",
    "정선군청": "통합돌봄", "고성군청": "통합돌봄",
    "용인시청통합돌봄": "통합돌봄", "용인시청": "통합돌봄", "광주동구청": "통합돌봄", "연수구청": "통합돌봄",
    "계양구청": "통합돌봄", "영월군청": "통합돌봄",
    "동해시청": "통합돌봄", "세종사회서비스원": "통합돌봄",
    "서초구청": "노인맞춤돌봄", "포천시청": "노인맞춤돌봄",
    "홍천군청": "노인맞춤돌봄", "경기도청": "노인맞춤돌봄",
    "경남사회서비스원": "노인맞춤돌봄",
    "다살림재가노인지원서비스센터": "노인맞춤돌봄",
    "강릉시청": "고독사예방", "금정구청": "고독사예방", "부산금정구청": "고독사예방",
    "삼척시청": "고독사예방", "광명시청": "고독사예방",
    "제주시청": "고독사예방", "서귀포시청": "고독사예방",
    "충남사회서비스원": "취약지지원",
    "희망나래": "장애인지원", "희망나래장애인복지관": "장애인지원",
    "인천사회서비스원": "퇴원환자지원",
    "강원사회서비스원": "기타",
}
BUSINESS_TYPE_ORDER = ["통합돌봄", "노인맞춤돌봄", "고독사예방", "취약지지원", "장애인지원", "퇴원환자지원", "기타"]

TOP_N_CONTRIBUTORS = 3


def biz_of(mun_name: str) -> str:
    n = str(mun_name).strip()
    if n in BUSINESS_TYPE_MAP:
        return BUSINESS_TYPE_MAP[n]
    n_flat = n.replace(" ", "")
    for k, v in BUSINESS_TYPE_MAP.items():
        k_flat = k.replace(" ", "")
        if k_flat == n_flat or k_flat in n_flat or n_flat in k_flat:
            return v
    return None


def build_daymap(weekly_users: pd.DataFrame) -> dict:
    """날짜(YYYY-MM-DD) -> 주차 라벨. 일별 데이터를 주차로 묶을 때 사용."""
    daymap = {}
    if weekly_users.empty or "주차" not in weekly_users.columns or "시작일" not in weekly_users.columns:
        return daymap
    for _, r in weekly_users.iterrows():
        start = pd.to_datetime(str(r["시작일"]), errors="coerce")
        if pd.isna(start):
            continue
        wk = str(r["주차"]).strip()
        for i in range(7):
            daymap[(start + pd.Timedelta(days=i)).strftime("%Y-%m-%d")] = wk
    return daymap


# ============================================================
# 지표별 [주차, 지자체명, ...] long-format 로더
# ============================================================

def load_registration(data: dict) -> pd.DataFrame:
    return data.get("registration", pd.DataFrame())


def load_join_count(data: dict) -> pd.DataFrame:
    """가입률용 분자 — 주차별 지자체별 가입완료 인원."""
    return data.get("weekly_registered_by_mun", pd.DataFrame())


def load_checkin_confirm(data: dict, daymap: dict) -> pd.DataFrame:
    """안부확인율용 — 주차별 지자체별 분자/분모."""
    df = data.get("checkin_mun_weekly", pd.DataFrame())
    if df.empty:
        return df
    df = df.copy()
    df["주차"] = df["시작일"].astype(str).str.strip().map(daymap)
    return df.dropna(subset=["주차"])


def load_checkin_rate(data: dict, daymap: dict) -> pd.DataFrame:
    """안부체크율용 — 주차별 지자체별 발송/응답/off."""
    df = data.get("checkin_municipality_rate", pd.DataFrame())
    if df.empty or "안부체크발송" not in df.columns:
        return pd.DataFrame()
    df = df.copy()
    df["주차"] = df["시작일"].astype(str).str.strip().map(daymap)
    return df.dropna(subset=["주차"])


def load_wide_weekly(sheets: dict, sheet_key: str) -> tuple:
    """심혈관/스트레스이용자처럼 이미 '주차당 1행'인 와이드 시트 -> (df, 주차컬럼명, 지자체->컬럼 매핑)."""
    raw = sheets.get(sheet_key, pd.DataFrame())
    if raw.empty:
        return raw, None, {}
    wc = next((c for c in raw.columns if "주차" in str(c).replace("\n", "").strip()), None)
    if wc is None:
        return raw, None, {}
    skip = {"이용자비중", "합계", "비중", "전체", "wow"}
    mun_cols = {}
    for c in raw.columns:
        cl = str(c).replace("\n", " ").strip()
        if c == wc or any(kw in cl.lower() for kw in skip):
            continue
        mun_name = cl.replace(" 이용자수", "").strip()
        if biz_of(mun_name):
            mun_cols[mun_name] = c
    return raw, wc, mun_cols


def load_daily_avg(df: pd.DataFrame, date_col: str, mun_col: str, val_col: str, daymap: dict) -> pd.DataFrame:
    """건강상담/걸음수처럼 일별로 쌓인 데이터 -> [주차, 지자체명, 값(그날값)]."""
    if df.empty:
        return df
    out = df[[date_col, mun_col, val_col]].copy()
    out.columns = ["_date", "지자체명", "값"]
    out["_date"] = out["_date"].astype(str).str.strip()
    out["주차"] = out["_date"].map(daymap)
    return out.dropna(subset=["주차"])


# ============================================================
# 주차 비교 로직
# ============================================================

def rate_by_mun_week(df: pd.DataFrame, num_col: str, den_col: str, week: str) -> dict:
    """주차 필터 후 지자체별 (분자합, 분모합) dict."""
    if df.empty:
        return {}
    wk = df[df["주차"] == week]
    if wk.empty:
        return {}
    out = {}
    for mun, g in wk.groupby("지자체명"):
        num = g[num_col].apply(safe_numeric).sum()
        den = g[den_col].apply(safe_numeric).sum()
        out[str(mun).strip()] = (num, den)
    return out


def off_by_mun_week(df: pd.DataFrame, week: str) -> dict:
    """off대상자는 지자체당 고정값(날짜마다 반복 저장) — 지자체당 1회만 집계."""
    if df.empty or "off대상자" not in df.columns:
        return {}
    wk = df[df["주차"] == week]
    if wk.empty:
        return {}
    dedup = wk.drop_duplicates(subset=["지자체명"])
    return dict(zip(dedup["지자체명"].astype(str).str.strip(), dedup["off대상자"].apply(safe_numeric)))


def count_by_mun_week(df: pd.DataFrame, mun_col: str, week_col: str, val_col: str, week: str) -> dict:
    if df.empty:
        return {}
    wk = df[df[week_col] == week]
    if wk.empty:
        return {}
    out = {}
    for mun, g in wk.groupby(mun_col):
        out[str(mun).strip()] = g[val_col].apply(safe_numeric).sum()
    return out


def avg_by_mun_week(df: pd.DataFrame, week: str) -> dict:
    if df.empty:
        return {}
    wk = df[df["주차"] == week]
    if wk.empty:
        return {}
    out = {}
    for mun, g in wk.groupby("지자체명"):
        out[str(mun).strip()] = g["값"].apply(safe_numeric).mean()
    return out


def wide_value_by_mun(raw: pd.DataFrame, wc: str, mun_cols: dict, week: str) -> dict:
    row = raw[raw[wc].astype(str).str.strip() == week]
    if row.empty:
        return {}
    row = row.iloc[0]
    return {mun: safe_numeric(row[col]) for mun, col in mun_cols.items()}


def biz_group(values_by_mun: dict) -> dict:
    """{지자체명: 값} -> {사업구분: 값합}. 값이 (num,den) 튜플이면 각각 합산."""
    grouped = {}
    for mun, val in values_by_mun.items():
        b = biz_of(mun)
        if not b:
            continue
        if isinstance(val, tuple):
            n0, d0 = grouped.get(b, (0.0, 0.0))
            grouped[b] = (n0 + val[0], d0 + val[1])
        else:
            grouped[b] = grouped.get(b, 0.0) + val
    return grouped


def rate_dict(num_den: dict) -> dict:
    return {b: round(n / d * 100, 1) if d > 0 else None for b, (n, d) in num_den.items()}


def top_contributors(this_by_mun: dict, prev_by_mun: dict, biz: str, n=TOP_N_CONTRIBUTORS, as_rate=False):
    """해당 사업구분 소속 지자체들의 전주 대비 변화를 큰 순서로 n개 반환.
    as_rate=True면 (num,den) 튜플에서 % 변화를 계산, 아니면 raw 값 변화."""
    rows = []
    munis = set(this_by_mun.keys()) | set(prev_by_mun.keys())
    for mun in munis:
        if biz_of(mun) != biz:
            continue
        cur = this_by_mun.get(mun)
        prev = prev_by_mun.get(mun)
        if as_rate:
            cur_r = round(cur[0] / cur[1] * 100, 1) if cur and cur[1] > 0 else None
            prev_r = round(prev[0] / prev[1] * 100, 1) if prev and prev[1] > 0 else None
            if cur_r is None or prev_r is None:
                continue
            diff = round(cur_r - prev_r, 1)
            rows.append((mun, prev_r, cur_r, diff))
        else:
            cur_v = cur if cur is not None else 0
            prev_v = prev if prev is not None else 0
            diff = round(cur_v - prev_v, 1)
            if diff == 0:
                continue
            rows.append((mun, prev_v, cur_v, diff))
    rows.sort(key=lambda r: abs(r[3]), reverse=True)
    return rows[:n]


# ============================================================
# 리포트 조립
# ============================================================

def fmt_diff(diff, suffix=""):
    sign = "+" if diff > 0 else ""
    return f"{sign}{diff:.1f}{suffix}"


def _direction(diff):
    return ("올랐습니다", "상승") if diff > 0 else ("떨어졌습니다", "하락")


def _contrib_phrase_rate(contributors):
    parts = [f"{mun}({pv}%→{cv}%)" for mun, pv, cv, _ in contributors]
    return ", ".join(parts)


def _contrib_phrase_count(contributors, unit="명"):
    parts = [f"{mun}({pv:.0f}{unit}→{cv:.0f}{unit})" for mun, pv, cv, _ in contributors]
    return ", ".join(parts)


def build_section(title, this_rate, prev_rate, this_by_mun, prev_by_mun, threshold, unit="%p", as_rate=True, biz_list=None):
    paras = []
    biz_list = biz_list or BUSINESS_TYPE_ORDER
    for biz in biz_list:
        cur = this_rate.get(biz)
        prev = prev_rate.get(biz)
        if cur is None or prev is None:
            continue
        diff = round(cur - prev, 1)
        if abs(diff) < threshold:
            continue
        verb, direction = _direction(diff)
        contributors = top_contributors(this_by_mun, prev_by_mun, biz, as_rate=as_rate)
        if as_rate:
            contrib_txt = _contrib_phrase_rate(contributors)
        else:
            contrib_txt = _contrib_phrase_count(contributors)
        if len(contributors) == 1:
            contrib_sentence = f"{contrib_txt}의 변화가 그대로 반영된 결과입니다." if contrib_txt else ""
        elif contrib_txt:
            contrib_sentence = f"{contrib_txt} 순으로 크게 기여했습니다."
        else:
            contrib_sentence = ""
        para = (f"■ {title} — {biz}: 전주 {prev}%에서 이번 주 {cur}%로 "
                f"{abs(diff)}%p {verb} ({direction}). {contrib_sentence}").strip()
        paras.append(para)
    if not paras:
        return None
    return f"[{title}]\n" + "\n".join(paras)


def main():
    parser = argparse.ArgumentParser(description="주간 운영 변동 리포트")
    parser.add_argument("--threshold", type=float, default=5.0, help="%%p 변화 임계값 (기본 5)")
    parser.add_argument("--count-threshold", type=float, default=0.0,
                         help="이용자수/참여자수 등 절대건수 지표의 변화 임계값 (기본: 자동, 이용자수 합의 10%%)")
    parser.add_argument("--out", type=str, default=None, help="결과를 저장할 마크다운 파일 경로")
    args = parser.parse_args()

    print("[weekly_report] 데이터 로딩 중...", file=sys.stderr)
    sheets = sd.fetch_all_sheets()
    data = sd.build_dashboard_data(sheets)

    reg = load_registration(data)
    active_biz = set()
    for _, row in reg.iterrows():
        if safe_numeric(row.get("협약인원", 0)) > 0:
            b = biz_of(row.get("지자체명", ""))
            if b:
                active_biz.add(b)
    biz_list = [b for b in BUSINESS_TYPE_ORDER if b in active_biz]

    weeks = sorted(data.get("주차목록", []))
    if len(weeks) < 2:
        print("주차 데이터가 부족해 비교할 수 없습니다.")
        return
    this_week, prev_week = weeks[-1], weeks[-2]

    weekly_users = data.get("weekly_users", pd.DataFrame())
    daymap = build_daymap(weekly_users)

    reg_contract_by_biz = {}
    reg_completed_by_biz = {}
    for _, row in reg.iterrows():
        b = biz_of(row.get("지자체명", ""))
        if not b:
            continue
        reg_contract_by_biz[b] = reg_contract_by_biz.get(b, 0.0) + safe_numeric(row.get("협약인원", 0))
        reg_completed_by_biz[b] = reg_completed_by_biz.get(b, 0.0) + safe_numeric(row.get("가입완료", 0))
    reg_completed_by_mun = {str(r["지자체명"]).strip(): safe_numeric(r.get("가입완료", 0)) for _, r in reg.iterrows()}

    sections = []

    # ── 1) 가입률 ──────────────────────────────────────────
    jn = load_join_count(data)
    if not jn.empty:
        this_j = count_by_mun_week(jn, "지자체명", "주차", "가입완료", this_week)
        prev_j = count_by_mun_week(jn, "지자체명", "주차", "가입완료", prev_week)
        this_biz_j = biz_group(this_j)
        prev_biz_j = biz_group(prev_j)
        this_rate = {b: round(v / reg_contract_by_biz.get(b, 0) * 100, 1)
                     for b, v in this_biz_j.items() if reg_contract_by_biz.get(b, 0) > 0}
        prev_rate = {b: round(v / reg_contract_by_biz.get(b, 0) * 100, 1)
                     for b, v in prev_biz_j.items() if reg_contract_by_biz.get(b, 0) > 0}
        sec = build_section("가입률", this_rate, prev_rate, this_j, prev_j, args.threshold, as_rate=False, biz_list=biz_list)
        if sec:
            sections.append(sec)

    # ── 2) 안부확인율 ──────────────────────────────────────
    cc = load_checkin_confirm(data, daymap)
    if not cc.empty:
        this_num_den = rate_by_mun_week(cc, "분자", "분모", this_week)
        prev_num_den = rate_by_mun_week(cc, "분자", "분모", prev_week)
        this_rate = rate_dict(biz_group(this_num_den))
        prev_rate = rate_dict(biz_group(prev_num_den))
        sec = build_section("안부확인율", this_rate, prev_rate, this_num_den, prev_num_den, args.threshold, biz_list=biz_list)
        if sec:
            sections.append(sec)

    # ── 3) 안부체크율 ──────────────────────────────────────
    cr = load_checkin_rate(data, daymap)
    if not cr.empty:
        this_send = rate_by_mun_week(cr, "안부체크발송", "안부체크응답", this_week)  # (발송,응답) 임시 보관
        prev_send = rate_by_mun_week(cr, "안부체크발송", "안부체크응답", prev_week)
        this_off = off_by_mun_week(cr, this_week)
        prev_off = off_by_mun_week(cr, prev_week)

        def _to_num_den(send_resp: dict, off: dict) -> dict:
            out = {}
            for mun, (send, resp) in send_resp.items():
                denom = send - off.get(mun, 0)
                out[mun] = (resp, denom)
            return out

        this_num_den = _to_num_den(this_send, this_off)
        prev_num_den = _to_num_den(prev_send, prev_off)
        this_rate = rate_dict(biz_group(this_num_den))
        prev_rate = rate_dict(biz_group(prev_num_den))
        sec = build_section("안부체크율", this_rate, prev_rate, this_num_den, prev_num_den, args.threshold, biz_list=biz_list)
        if sec:
            sections.append(sec)

    # ── 4) 심혈관 / 5) 스트레스 이용비중 ─────────────────────
    for label, sheet_key in [("심혈관", "심혈관이용자"), ("스트레스", "스트레스이용자")]:
        raw, wc, mun_cols = load_wide_weekly(sheets, sheet_key)
        if wc is None:
            continue
        this_cnt = wide_value_by_mun(raw, wc, mun_cols, this_week)
        prev_cnt = wide_value_by_mun(raw, wc, mun_cols, prev_week)
        this_biz_cnt = biz_group(this_cnt)
        prev_biz_cnt = biz_group(prev_cnt)
        this_rate = {b: round(v / reg_completed_by_biz.get(b, 0) * 100, 1)
                     for b, v in this_biz_cnt.items() if reg_completed_by_biz.get(b, 0) > 0}
        prev_rate = {b: round(v / reg_completed_by_biz.get(b, 0) * 100, 1)
                     for b, v in prev_biz_cnt.items() if reg_completed_by_biz.get(b, 0) > 0}
        sec = build_section(f"{label} 이용비중", this_rate, prev_rate, this_cnt, prev_cnt, args.threshold, as_rate=False, biz_list=biz_list)
        if sec:
            sections.append(sec)

    def build_count_section(title, this_by_mun, prev_by_mun, unit, min_diff):
        this_biz = {b: round(v, 1) for b, v in biz_group(this_by_mun).items()}
        prev_biz = {b: round(v, 1) for b, v in biz_group(prev_by_mun).items()}
        paras = []
        for b in biz_list:
            cur, prev = this_biz.get(b), prev_biz.get(b)
            if cur is None or prev is None or prev == 0:
                continue
            diff = round(cur - prev, 1)
            if abs(diff) < min_diff:
                continue
            verb, direction = _direction(diff)
            contributors = top_contributors(this_by_mun, prev_by_mun, b, as_rate=False)
            contrib_txt = _contrib_phrase_count(contributors, unit)
            if len(contributors) == 1:
                contrib_sentence = f"{contrib_txt}의 변화가 그대로 반영된 결과입니다." if contrib_txt else ""
            elif contrib_txt:
                contrib_sentence = f"{contrib_txt} 순으로 크게 기여했습니다."
            else:
                contrib_sentence = ""
            para = (f"■ {title} — {b}: 일평균 전주 {prev}{unit}에서 이번 주 {cur}{unit}으로 "
                    f"{abs(diff)}{unit} {verb} ({direction}). {contrib_sentence}").strip()
            paras.append(para)
        return f"[{title}]\n" + "\n".join(paras) if paras else None

    # ── 6) 건강상담 (일평균) ──────────────────────
    hc = data.get("건강상담지자체", pd.DataFrame())
    if not hc.empty and "합계" in hc.columns:
        hc_long = load_daily_avg(hc, "날짜", "지자체", "합계", daymap)
        this_hc = avg_by_mun_week(hc_long, this_week)
        prev_hc = avg_by_mun_week(hc_long, prev_week)
        sec = build_count_section("건강상담", this_hc, prev_hc, "건", max(args.count_threshold, 0.3))
        if sec:
            sections.append(sec)

    # ── 7) 걸음수 (일평균) ──────────────────────
    steps_raw = sheets.get("걸음수현황", pd.DataFrame())
    STEPS_EXCLUDE = {"WAPLAT", "ai생활지원사테스트", "한전MCS"}
    if not steps_raw.empty and "agencyName" in steps_raw.columns:
        steps = steps_raw[~steps_raw["agencyName"].isin(STEPS_EXCLUDE)].copy()
        steps["_date"] = pd.to_datetime(steps["date"], errors="coerce").dt.strftime("%Y-%m-%d")
        steps_long = load_daily_avg(steps, "_date", "agencyName", "memberCnt", daymap)
        this_st = avg_by_mun_week(steps_long, this_week)
        prev_st = avg_by_mun_week(steps_long, prev_week)
        sec = build_count_section("걸음수", this_st, prev_st, "명", max(args.count_threshold, 1.0))
        if sec:
            sections.append(sec)

    # ── 8) KT 관제율 (세이프 전용, 전사 기준) ─────────────────
    safe_raw = sheets.get("안부체크횟수", pd.DataFrame())
    if not safe_raw.empty:
        date_col = next((c for c in safe_raw.columns if "시작일" in str(c).replace("\n", "").strip()), safe_raw.columns[0])
        mgmt_col = next((c for c in safe_raw.columns if "kt관제율" in str(c).replace("\n", "").replace(" ", "").lower()), None)
        disp_col = next((c for c in safe_raw.columns if "kt출동율" in str(c).replace("\n", "").replace(" ", "").lower()
                          or "kt출동률" in str(c).replace("\n", "").replace(" ", "").lower()), None)
        kt_paras = []
        for label, col in [("KT 관제율", mgmt_col), ("KT 출동율", disp_col)]:
            if col is None:
                continue
            s = safe_raw[[date_col, col]].copy()
            s[col] = s[col].apply(safe_numeric)
            s = s[s[col] > 0].sort_values(date_col)
            if len(s) < 2:
                continue
            cur, prev = s[col].iloc[-1], s[col].iloc[-2]
            diff = round(cur - prev, 1)
            if abs(diff) >= args.threshold:
                verb, direction = _direction(diff)
                kt_paras.append(f"■ {label}: 전주 {prev}%에서 이번 주 {cur}%로 {abs(diff)}%p {verb} ({direction}). "
                                 f"세이프 전체 기준 수치라 지자체별 기여도는 따로 안 나옵니다.")
        if kt_paras:
            sections.append("[KT 관제 현황 (세이프 전용)]\n" + "\n".join(kt_paras))

    # ── 리포트 출력 ────────────────────────────────────────
    now = datetime.now().strftime("%Y-%m-%d %H:%M")
    header = f"주간 운영 변동 리포트 ({prev_week}주차 → {this_week}주차, 생성 {now})\n임계값: ±{args.threshold}%p 이상 변동만 표시\n"
    if sections:
        body = "\n\n".join(sections)
    else:
        body = f"임계값(±{args.threshold}%p) 이상으로 움직인 지표가 없습니다. 이번 주는 특별히 짚을 변화가 없었습니다."
    report = header + "\n" + body + "\n"

    print(report)
    if args.out:
        with open(args.out, "w", encoding="utf-8") as f:
            f.write(report)
        print(f"\n[저장됨] {args.out}", file=sys.stderr)


if __name__ == "__main__":
    main()
