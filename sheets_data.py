# -*- coding: utf-8 -*-
"""Google Sheets 실데이터 fetcher - 와플랫 공공 지표 대시보드"""
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import requests

SPREADSHEET_ID = "15UZ9dZjYdD24PdWoSvrFWpQCM-T0vhc_yy9wrMunSNc"
BASE_URL = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/gviz/tq?tqx=out:csv&gid="

# AI 생활지원사 월간구분 외부 시트 (지자체별 X축 레이블)
# 구조: A=주차, B=날짜범위, C=월간구분("3월 2주" 등), D=지자체명/통합
_EXT_GUBN_URL = "https://docs.google.com/spreadsheets/d/119srCb-aMqslErS7seMjQ1DPQEQPZ7bsY_Q5R-fwgSE/gviz/tq?tqx=out:csv&gid=1010714986"

# 20개 시트 GID 매핑
SHEET_GIDS = {
    "이용자현황":       "33599894",
    "이용자주간":       "0",
    "심혈관현황":       "981210016",
    "안부확인전체":     "261480368",
    "안부확인지자체":   "1922136052",
    "안부체크횟수":     "851523453",
    "심혈관이용자":     "1006361638",
    "심혈관검사횟수":   "1088577927",
    "복약등록회원":     "930297071",
    "복약등록건수":     "2055445729",
    "맞고와플게스트":   "453575597",
    "맞고이용자":       "267414689",
    "맞고플레이판수":   "1231212781",
    "맞고플레이시간":   "272203614",
    "맞고게스트":       "1392482602",
    "건강상담":         "1441355851",
    "생활상담":         "1691440005",
    "스트레스이용자":   "1286220157",
    "스트레스수행횟수": "2115071221",
    "AI생활지원사":     "1751859498",
    "AI생활지원사월별": "552264832",
    "AI생활지원사신규": "887906400",
    "AI삼척월별":       "1891951831",
    "AI양양월별":       "1179230271",
    "AI정선월별":       "926765476",
    "안부확인raw":      "1323180805",
    "안부체크off":      "1043653372",
    "건강상담지자체":   "867975933",
    "걸음수현황":       "1968687679",
    "걸음수이용":       "680392566",
}

# 지자체 키워드 (컬럼명에서 지자체 자동 탐지용)
# NOTE: 더 긴/구체적 키워드를 앞에 배치해야 부분 매칭 오류 방지
# (예: 용인시청통합돌봄 → 용인시청보다 먼저 체크)
MUNICIPALITY_KEYWORDS = [
    "경기도청", "용인시청통합돌봄", "용인시청", "서초구청", "청주시청", "진천군청", "음성군청",
    "강북구청", "금정구청", "괴산군청", "증평군청", "포천시청", "마포구청",
    "광진구청", "경남사회서비스원", "강릉시청", "강원사회서비스원",
    "충북사회서비스원", "독거노인", "희망나래", "홍천군청",
    "충남사회서비스원", "삼척시청",
    "광명시청", "제주시청", "서귀포시청", "양양군청",
    "양평군청", "정선군청",
    "고성군청", "광주동구청",
    "계양구청", "연수구청", "영월군청", "다살림재가노인지원서비스센터",
    "세종사회서비스원", "동해시청", "인천사회서비스원",
]

# 전체 계약 지자체 목록 (31개) — 시트에 없는 경우 0으로 패딩
ALL_KNOWN_AGENCIES = [
    # 8월 기준 라이브 31개 (독거노인종합지원센터·강북구청 계약 종료로 제외)
    "강릉시청", "강원사회서비스원", "경기도청", "경남사회서비스원", "고성군청",
    "광명시청", "광주동구청", "금정구청", "삼척시청", "서귀포시청",
    "서초구청", "양양군청", "양평군청", "영월군청",
    "용인시청통합돌봄", "음성군청", "정선군청", "제주시청", "증평군청",
    "진천군청", "충남사회서비스원", "충북사회서비스원", "포천시청", "홍천군청",
    "희망나래", "계양구청", "연수구청", "다살림재가노인지원서비스센터",
    "세종사회서비스원", "동해시청", "인천사회서비스원",
]


def sync_known_agencies_from_registration(sheets: dict) -> int:
    """이용자현황 시트의 협약인원>0 지자체를 ALL_KNOWN_AGENCIES·MUNICIPALITY_KEYWORDS에
    자동으로 추가한다 (제자리에서 리스트를 수정 — import한 다른 모듈에서도 바로 보임).

    지자체가 계속 새로 생기는데 두 목록을 하드코딩해두면 매번 수동으로 고쳐야 해서,
    fetch_all_sheets() 안에서 매번 호출해 자동으로 최신 상태를 유지한다.
    이미 있는 이름은 건너뜀. Returns: 새로 추가된 지자체 수.
    """
    df = sheets.get("이용자현황", pd.DataFrame())
    if df.empty or len(df.columns) < 2:
        return 0
    name_col = df.columns[0]
    amount_col = next((c for c in df.columns if "협약" in str(c)), None)

    def _already_known(name: str) -> bool:
        """공백 차이나 별칭(예: 희망나래 vs 희망나래장애인복지관)까지 감안한 부분일치 확인."""
        name_n = name.replace(" ", "")
        if name in NAME_ALIASES or name_n in {k.replace(" ", "") for k in NAME_ALIASES}:
            return True
        for known in ALL_KNOWN_AGENCIES:
            known_n = known.replace(" ", "")
            if known_n == name_n or known_n in name_n or name_n in known_n:
                return True
        return False

    added = 0
    for _, row in df.iterrows():
        name = str(row.get(name_col, "")).strip()
        if not name or name == "nan":
            continue
        if amount_col is not None:
            try:
                amt = float(str(row[amount_col]).replace(",", "").strip())
            except (ValueError, TypeError):
                amt = 0
            if amt <= 0:
                continue
        if _already_known(name):
            continue
        ALL_KNOWN_AGENCIES.append(name)
        MUNICIPALITY_KEYWORDS.append(name)
        added += 1
    return added


# 이름 별칭 (같은 지자체의 다른 표기)
NAME_ALIASES = {
    "희망나래장애인복지관": "희망나래",
    "희망나래복지원": "희망나래",
    "충남사회서비스언": "충남사회서비스원",
    "용인시청 통합돌봄": "용인시청통합돌봄",
    "용인시청통합돌봄": "용인시청통합돌봄",
    "독거노인종합지원센터": "독거노인종합지원센터",  # 계약 종료 — 별칭 유지(매칭용)
    "독거노인지원종합센터": "독거노인종합지원센터",
    "강북구청": "강북구청",                          # 계약 종료
}

def normalize_agency_name(name: str) -> str:
    """지자체명을 표준 이름으로 변환"""
    name = str(name).strip()
    if name in NAME_ALIASES:
        return NAME_ALIASES[name]
    # 별칭에 부분 매칭
    for alias, standard in NAME_ALIASES.items():
        if alias in name or name in alias:
            return standard
    return name

def match_municipality_keyword(col_text: str):
    """컬럼명 텍스트가 MUNICIPALITY_KEYWORDS 중 어느 지자체에 해당하는지 찾는다.

    정확히 일치하는 키워드를 최우선으로 채택하고, 없을 때만 부분일치로 폴백하되
    폴백 시엔 가장 긴(구체적인) 키워드를 고른다. 부분일치만 쓰면 짧은 이름이 긴
    이름의 부분문자열인 경우(예: "용인시청"이 "용인시청통합돌봄"의 부분문자열)
    리스트 순서에 따라 엉뚱한 지자체로 잘못 귀속되는 버그가 실제로 있었음 —
    새 지자체가 계속 추가되는 구조라 이 매칭 방식을 쓰는 모든 곳에서 재발 가능.

    Returns: 매칭된 키워드 원문(MUNICIPALITY_KEYWORDS의 항목 그대로) or None
    """
    flat = str(col_text).replace("\n", "").replace(" ", "").strip()
    for kw in MUNICIPALITY_KEYWORDS:
        if kw.replace(" ", "") == flat:
            return kw
    candidates = [kw for kw in MUNICIPALITY_KEYWORDS if kw.replace(" ", "") in flat]
    if candidates:
        return max(candidates, key=lambda k: len(k.replace(" ", "")))
    return None

# 수도권 / 비수도권 분류
REGION_MAP = {
    "경기도청": "수도권", "용인시청": "수도권", "용인시청통합돌봄": "수도권",
    "서초구청": "수도권", "강북구청": "수도권", "포천시청": "수도권",
    "마포구청": "수도권", "광진구청": "수도권", "광명시청": "수도권",
    "양평군청": "수도권", "계양구청": "수도권", "연수구청": "수도권",
    "청주시청": "비수도권", "진천군청": "비수도권", "음성군청": "비수도권",
    "금정구청": "비수도권", "괴산군청": "비수도권", "증평군청": "비수도권",
    "강릉시청": "비수도권", "홍천군청": "비수도권", "삼척시청": "비수도권",
    "경남사회서비스원": "비수도권", "강원사회서비스원": "비수도권",
    "충북사회서비스원": "비수도권", "충남사회서비스원": "비수도권",
    "제주시청": "비수도권", "서귀포시청": "비수도권",
    "양양군청": "비수도권", "정선군청": "비수도권",
    "고성군청": "비수도권", "광주동구청": "비수도권",
    "영월군청": "비수도권",
    "독거노인지원종합센터": "기관", "독거노인종합지원센터": "기관",
    "독거노인": "기관", "희망나래": "기관", "희망나래장애인복지관": "기관",
    "다살림재가노인지원서비스센터": "기관",
}


def fetch_sheet(gid: str) -> pd.DataFrame:
    """Google Sheets에서 CSV 데이터를 가져와 DataFrame으로 반환"""
    url = BASE_URL + gid
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        # BOM 제거 + UTF-8 파싱
        content = resp.content.decode("utf-8-sig")
        df = pd.read_csv(io.StringIO(content))
        # 빈 행/열 제거
        df = df.dropna(how="all").dropna(axis=1, how="all")
        return df
    except Exception as e:
        print(f"[sheets_data] Error fetching gid={gid}: {e}")
        return pd.DataFrame()


def _build_ai_gubn_mapping() -> dict:
    """AI 생활지원사 외부 시트(gid=1010714986)에서 (주차번호, 지자체명) → 월간구분 매핑 구축.

    시트 구조: A=주차, B=날짜범위, C=월간구분(3월 2주 등), D=지자체명/통합
    - 주차 첫 행(통합): A에 "11주차", C에 해당 주 기본 월간구분
    - 지자체별 행: D에 지자체명, C에 지자체별 월간구분(없으면 통합 값 사용)
    """
    import re as _re
    mapping = {}
    try:
        resp = requests.get(_EXT_GUBN_URL, timeout=20, allow_redirects=True)
        resp.raise_for_status()
        content = resp.content.decode("utf-8-sig")
        df = pd.read_csv(io.StringIO(content))
        df = df.dropna(how="all").dropna(axis=1, how="all")
    except Exception as e:
        print(f"[sheets_data] ai_gubn fetch 실패: {e}")
        return {}

    if df.empty or len(df.columns) < 4:
        return {}

    current_wk = 0
    default_gubn = ""

    for _, row in df.iterrows():
        a = str(row.iloc[0]).strip() if len(row) > 0 else ""
        c = str(row.iloc[2]).strip() if len(row) > 2 else ""
        d = str(row.iloc[3]).strip() if len(row) > 3 else ""

        if a not in ("nan", "NaN", ""):
            m = _re.search(r'(\d+)주차', a)
            if m:
                current_wk = int(m.group(1))

        if current_wk == 0:
            continue

        gubn_val = "" if c in ("nan", "NaN") else c
        name_val = "" if d in ("nan", "NaN") else d

        if name_val == "통합":
            pass  # 통합 집계 행은 건너뜀
        elif name_val:
            if gubn_val:
                # C열 명시값만 사용 — 통합 상속 없음
                mapping[(current_wk, name_val)] = gubn_val

    print(f"[sheets_data] ai_gubn_mapping: {len(mapping)}개 매핑 로드")
    return mapping


def get_ai_municipality_ext(sheets: dict) -> pd.DataFrame:
    """메인 시트(gid=887906400) B열('주차별 현황') 기준 지자체별 주차 데이터.

    B열이 있는 행은 B열 값을 X축 라벨로 사용.
    B열이 비어있으면 A열(주차번호) + 외부 시트 매핑(_gubn_map)으로 보완.
    """
    import re as _re_ext
    df = sheets.get("AI생활지원사신규", pd.DataFrame())
    if df.empty or len(df.columns) < 4:
        return pd.DataFrame()
    df = df.copy()

    # A열 forward-fill: 통합 행에만 주차번호가 있으므로 아래 행에 채움
    df.iloc[:, 0] = df.iloc[:, 0].ffill()

    _gubn_map = sheets.get("ai_gubn_mapping", {})

    num_cols = [
        "계약인원", "가입인원", "receiveAlarmCount", "receiveAlarmUserCount",
        "intro", "intro(%)", "service proposal", "service proposal(%)",
        "program complete", "program(%)",
    ]

    result_rows = []
    for _, row in df.iterrows():
        gubn = str(row.iloc[1]).strip() if len(row) > 1 else ""   # B열
        name = str(row.iloc[3]).strip() if len(row) > 3 else ""   # D열 = 지자체명

        # 통합 집계 행 제외
        if name in ("nan", "", "NaN", "통합"):
            continue

        # B열이 비어있으면 외부 시트 매핑으로 보완
        if not _re_ext.search(r'\d+월', gubn):
            wk_str = str(row.iloc[0]).strip()
            wk_m = _re_ext.search(r'(\d+)', wk_str)
            if wk_m:
                wk_num = int(wk_m.group(1))
                gubn = _gubn_map.get((wk_num, name), "")
            if not _re_ext.search(r'\d+월', gubn):
                continue  # 라벨 없으면 제외

        alarm_day = str(row.get("알람요일", "")).strip()
        alarm_day = "" if alarm_day in ("nan", "NaN") else alarm_day

        r = {"기간": gubn, "지자체": name, "알람요일": alarm_day, "월간구분": gubn}
        for nc in num_cols:
            if nc in df.columns:
                r[nc] = safe_numeric(row.get(nc, 0))
        # 월의 1째 주는 서비스 제안율 항상 0 (집계 특성상)
        if _re_ext.search(r'\d+월 1주', gubn):
            r["service proposal"] = 0.0
            r["service proposal(%)"] = 0.0
        result_rows.append(r)

    if not result_rows:
        return pd.DataFrame()
    print(f"[sheets_data] ai_municipality_ext(B열+매핑 기준): {len(result_rows)}행 로드")
    return pd.DataFrame(result_rows)


def fetch_all_sheets() -> dict:
    """모든 시트 데이터를 병렬로 한 번에 가져오기 (ThreadPoolExecutor)"""
    data = {}

    def _fetch_one(name_gid):
        name, gid = name_gid
        return name, fetch_sheet(gid)

    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = {executor.submit(_fetch_one, item): item[0]
                   for item in SHEET_GIDS.items()}
        for future in as_completed(futures):
            name, df = future.result()
            data[name] = df

    # AI 생활지원사 월간구분 매핑 (외부 스프레드시트)
    data["ai_gubn_mapping"] = _build_ai_gubn_mapping()

    # 신규 지자체 자동 반영 (ALL_KNOWN_AGENCIES / MUNICIPALITY_KEYWORDS 갱신)
    try:
        sync_known_agencies_from_registration(data)
    except Exception as e:
        print(f"[sheets_data] sync_known_agencies_from_registration error: {e}")

    return data


def get_check_off_users(sheets: dict = None) -> dict:
    """안부체크 발송 off 대상자 수를 지자체별로 반환

    Returns: {지자체명: off대상자수} dict
    """
    if sheets and "안부체크off" in sheets:
        df = sheets["안부체크off"]
    else:
        df = fetch_sheet(SHEET_GIDS["안부체크off"])

    if df.empty:
        return {}

    result = {}
    # 첫 번째 컬럼은 "구분", 나머지가 지자체명
    for col in df.columns:
        col_clean = str(col).replace("\n", "").strip()
        kw = match_municipality_keyword(col_clean)
        if kw is not None:
            # 마지막 행(최신 데이터) 사용
            val = df[col].dropna()
            if not val.empty:
                result[kw] = safe_numeric(val.iloc[-1])

    return result


def find_municipality_columns(df: pd.DataFrame) -> list:
    """DataFrame 컬럼에서 지자체 관련 컬럼만 추출"""
    mun_cols = []
    for col in df.columns:
        col_clean = str(col).replace("\n", "").replace(" ", "").strip()
        for kw in MUNICIPALITY_KEYWORDS:
            if kw in col_clean:
                mun_cols.append(col)
                break
    return mun_cols


def extract_municipality_name(col_name: str) -> str:
    """컬럼명에서 순수 지자체명만 추출 + 이름 정규화"""
    col_clean = str(col_name).replace("\n", "").replace(" ", "").strip()
    kw = match_municipality_keyword(col_clean)
    if kw is not None:
        # normalize_agency_name()의 부분일치 폴백을 쓰면 "용인시청"(그 자체로 유효한
        # 키워드)이 "용인시청 통합돌봄" 별칭의 부분문자열이라는 이유로 다시 그쪽으로
        # 뒤섞이는 문제가 있었음(실제로 심혈관/스트레스 이용자 시트에서 재현됨) —
        # 정확히 일치하는 별칭 키가 있을 때만 정규화하고, 아니면 매칭된 키워드를 그대로 사용
        return NAME_ALIASES.get(kw, kw)
    # 매칭되는 키워드가 없을 때만 전체 문자열 기준 별칭 정규화 폴백
    return normalize_agency_name(col_clean)


def safe_numeric(val):
    """문자열을 숫자로 변환 (%, 콤마, 빈값 처리)"""
    # Series인 경우 첫 번째 값 사용
    if isinstance(val, pd.Series):
        if val.empty:
            return 0.0
        val = val.iloc[0]
    try:
        if pd.isna(val):
            return 0.0
    except (ValueError, TypeError):
        pass
    if val == "" or val == "-":
        return 0.0
    s = str(val).replace(",", "").replace("%", "").strip()
    try:
        return float(s)
    except (ValueError, TypeError):
        # 시:분:초 형식 처리 (예: "4084:37:25" → 4084.62)
        if ":" in s:
            try:
                parts = s.split(":")
                hours = float(parts[0])
                mins = float(parts[1]) if len(parts) > 1 else 0
                secs = float(parts[2]) if len(parts) > 2 else 0
                return round(hours + mins / 60 + secs / 3600, 1)
            except:
                pass
        return 0.0


# ============================================================
# 데이터 가공 함수들
# ============================================================

def get_registration_status(sheets: dict) -> pd.DataFrame:
    """시트1: 이용자 현황(전체지자체) - 지자체별 회원가입 완료율"""
    df = sheets.get("이용자현황", pd.DataFrame())
    if df.empty:
        return df
    # 컬럼 정리 - 첫번째 컬럼이 지자체명
    df = df.copy()
    cols = df.columns.tolist()
    # 컬럼명 표준화
    rename = {}
    for i, c in enumerate(cols):
        cl = str(c).replace("\n", "").strip()
        if "협약" in cl:
            rename[c] = "협약인원"
        elif "완료" in cl and "미" not in cl and "율" not in cl:
            rename[c] = "가입완료"
        elif "미완" in cl or "미완료" in cl:
            rename[c] = "가입미완료"
        elif "완료율" in cl or "율" in cl:
            rename[c] = "완료율"
    if rename:
        df = df.rename(columns=rename)
    # 첫 번째 텍스트 컬럼을 지자체명으로
    first_col = cols[0]
    df = df.rename(columns={first_col: "지자체명"})
    df["지자체명"] = df["지자체명"].astype(str).str.strip().apply(normalize_agency_name)
    # 숫자 변환
    for col in ["협약인원", "가입완료", "가입미완료"]:
        if col in df.columns:
            df[col] = df[col].apply(safe_numeric)
    if "완료율" in df.columns:
        df["완료율"] = df["완료율"].apply(safe_numeric)
    # 시트에 없는 신규 지자체 0으로 패딩 (28개 전체 표시)
    existing = set(df["지자체명"].tolist())
    num_cols = [c for c in ["협약인원", "가입완료", "가입미완료", "완료율"] if c in df.columns]
    pad_rows = []
    for agency in ALL_KNOWN_AGENCIES:
        if agency not in existing:
            row = {"지자체명": agency}
            for c in num_cols:
                row[c] = 0
            pad_rows.append(row)
    if pad_rows:
        df = pd.concat([df, pd.DataFrame(pad_rows)], ignore_index=True)
    return df


def get_weekly_users(sheets: dict) -> pd.DataFrame:
    """시트2: 이용자수(회원가입수) - 주간 가입 추이"""
    df = sheets.get("이용자주간", pd.DataFrame())
    if df.empty:
        return df
    df = df.copy()
    # 주차, 시작일 컬럼 식별 (중복 방지: 먼저 매칭된 컬럼만)
    cols = df.columns.tolist()
    rename = {}
    used_names = set()
    for c in cols:
        cl = str(c).replace("\n", "").strip()
        if "주차" in cl and "주차" not in used_names:
            rename[c] = "주차"
            used_names.add("주차")
        elif "시작일" in cl and "시작일" not in used_names:
            rename[c] = "시작일"
            used_names.add("시작일")
        elif "회원가입" in cl and "완료" in cl and "주간" not in cl and "율" not in cl and "가입완료합계" not in used_names:
            rename[c] = "가입완료합계"
            used_names.add("가입완료합계")
        elif "주간" in cl and "회원" in cl and "완료" in cl and "주간가입완료" not in used_names:
            rename[c] = "주간가입완료"
            used_names.add("주간가입완료")
        elif "주간" in cl and "활성" in cl and "주간활성사용자" not in used_names:
            rename[c] = "주간활성사용자"
            used_names.add("주간활성사용자")
        elif "주간" in cl and "이탈" in cl and "주간이탈자" not in used_names:
            rename[c] = "주간이탈자"
            used_names.add("주간이탈자")
        elif ("전체가입률" in cl or ("회원가입" in cl and "비중" in cl) or ("완료" in cl and "비중" in cl)) and "전체가입률" not in used_names:
            rename[c] = "전체가입률"
            used_names.add("전체가입률")
        elif "대상자" in cl and "수" in cl and "대상자수" not in used_names:
            rename[c] = "대상자수"
            used_names.add("대상자수")
    if rename:
        df = df.rename(columns=rename)
    # 중복 컬럼 제거 (첫 번째만 유지)
    df = df.loc[:, ~df.columns.duplicated()]
    return df


def get_weekly_registered_by_municipality(sheets: dict) -> pd.DataFrame:
    """이용자주간(gid=0) 시트에서 주차별 지자체별 가입완료 인원 추출

    시트 구조: 주차 | 시작일 | 대상자 수 | 회원가입 완료 | 경기도청 | 경기도청가입률 | 용인시청 | ...
    지자체명 컬럼(숫자값)만 추출 → long-format 반환

    Returns: DataFrame [주차, 지자체명, 가입완료]
    """
    df = sheets.get("이용자주간", pd.DataFrame())
    if df.empty:
        return pd.DataFrame()
    df = df.copy()

    # 주차 컬럼 찾기
    week_col = None
    for c in df.columns:
        cl = str(c).replace("\n", "").strip()
        if "주차" in cl:
            week_col = c
            break
    if week_col is None:
        return pd.DataFrame()

    # 지자체 컬럼 탐지: MUNICIPALITY_KEYWORDS와 일치하고 "가입률" 등 비율 컬럼 제외
    mun_cols = []
    for c in df.columns:
        cl = str(c).replace("\n", "").replace(" ", "").strip()
        if any(kw in cl for kw in MUNICIPALITY_KEYWORDS):
            # 가입률/비중/삭제율 등 파생 컬럼 제외 → 순수 인원수 컬럼만
            if not any(x in cl for x in ["가입률", "비중", "삭제율", "이용률", "체크율", "응답률"]):
                mun_cols.append(c)

    if not mun_cols:
        return pd.DataFrame()

    rows = []
    for _, row in df.iterrows():
        week = str(row.get(week_col, "")).strip()
        if not week or week == "nan":
            continue
        for mc in mun_cols:
            val = safe_numeric(row.get(mc, 0))
            if val > 0:
                mun_name = str(mc).replace("\n", "").strip()
                rows.append({"주차": week, "지자체명": mun_name, "가입완료": val})

    return pd.DataFrame(rows)


def get_weekly_municipality_data(sheets: dict, sheet_key: str) -> pd.DataFrame:
    """주차×지자체 형태의 시트를 long-format으로 변환

    Returns: DataFrame with columns [주차, 시작일, 지자체명, 값]
    """
    df = sheets.get(sheet_key, pd.DataFrame())
    if df.empty:
        return df
    df = df.copy()

    # 주차 컬럼 찾기
    week_col = None
    date_col = None
    for c in df.columns:
        cl = str(c).replace("\n", "").strip()
        if "주차" in cl:
            week_col = c
        elif "시작일" in cl:
            date_col = c

    if week_col is None:
        return pd.DataFrame()

    # 지자체 컬럼 탐지 (이용자수 컬럼만, 비중/삭제율 등 제외)
    mun_cols = []
    for c in df.columns:
        cl = str(c).replace("\n", "").replace(" ", "").strip()
        # 비중, 삭제율, 등록건수 등은 제외 (순수 이용자수/수행횟수만)
        if any(kw in cl for kw in MUNICIPALITY_KEYWORDS):
            # 비중, 삭제율 컬럼 제외
            if "비중" not in cl and "삭제율" not in cl and "대비" not in cl:
                mun_cols.append(c)

    if not mun_cols:
        return pd.DataFrame()

    # Long format 변환 - 빈칸은 제외 (계약 종료 지자체)
    rows = []
    for _, row in df.iterrows():
        week = str(row.get(week_col, "")).strip()
        date = str(row.get(date_col, "")).strip() if date_col else ""
        if not week or week == "nan":
            continue
        for mc in mun_cols:
            mun_name = extract_municipality_name(mc)
            raw_val = row.get(mc, None)
            # 빈칸/NaN → 계약 종료 지자체, 건너뜀
            if pd.isna(raw_val) or str(raw_val).strip() == "":
                continue
            val = safe_numeric(raw_val)
            rows.append({
                "주차": week,
                "시작일": date,
                "지자체명": mun_name,
                "값": val,
            })
    return pd.DataFrame(rows)


def get_checkin_daily(sheets: dict) -> pd.DataFrame:
    """시트4: 복약확인알림(전체) - 일별 안부확인 전체 데이터
    Google Sheets의 이미 계산된 비율 값(R~AA열)을 그대로 사용."""
    df = sheets.get("안부확인전체", pd.DataFrame())
    if df.empty:
        return df
    df = df.copy()

    # 컬럼 매핑 (Google Sheets A~AA열 전체)
    rename = {}
    used = set()
    for c in df.columns:
        cl = str(c).replace("\n", "").strip()
        # A열: 날짜
        if (cl == "date" or "날짜" in cl) and "날짜" not in used:
            rename[c] = "날짜"; used.add("날짜")
        # B열: 전체 회원
        elif "전체" in cl and "회원" in cl and "전체회원" not in used:
            rename[c] = "전체회원"; used.add("전체회원")
        # C열: 안부확인 완료자
        elif "안부확인" in cl and "완료" in cl and "안부확인완료자" not in used:
            rename[c] = "안부확인완료자"; used.add("안부확인완료자")
        # D열: 안부미확인자
        elif "안부미확인자" in cl and "안부미확인자" not in used:
            rename[c] = "안부미확인자"; used.add("안부미확인자")
        # E열: 48시간 안부미확인 대상자
        elif "48시간" in cl and "안부미확인" in cl and "대상" in cl and "48h대상자" not in used:
            rename[c] = "48h대상자"; used.add("48h대상자")
        # F열: ①안부체크 응답자
        elif "안부체크" in cl and "응답자" in cl and "①" in cl and "안부체크응답자" not in used:
            rename[c] = "안부체크응답자"; used.add("안부체크응답자")
        # G열: ②동작감지/서비스 이용자
        elif "동작감지" in cl and "동작감지이용자" not in used:
            rename[c] = "동작감지이용자"; used.add("동작감지이용자")
        # H열: ③AI케어알람 응답자
        elif "AI케어" in cl and "응답자" in cl and "AI케어응답자" not in used:
            rename[c] = "AI케어응답자"; used.add("AI케어응답자")
        # I열: ④안부확인콜 응답자
        elif "안부확인콜" in cl and "응답자" in cl and "안부확인콜응답자" not in used:
            rename[c] = "안부확인콜응답자"; used.add("안부확인콜응답자")
        # J열: 안부체크 발송 수
        elif "안부체크" in cl and "발송" in cl and "수" in cl and "안부체크발송수" not in used:
            rename[c] = "안부체크발송수"; used.add("안부체크발송수")
        # K열: 안부체크 응답자 수
        elif "안부체크" in cl and "응답자" in cl and "수" in cl and "①" not in cl and "안부체크응답자수" not in used:
            rename[c] = "안부체크응답자수"; used.add("안부체크응답자수")
        # L열: AI케어알람 발송 수
        elif "AI케어" in cl and "발송" in cl and "AI케어발송수" not in used:
            rename[c] = "AI케어발송수"; used.add("AI케어발송수")
        # M열: AI케어알람 응답자 수
        elif "AI케어" in cl and "응답자" in cl and "수" in cl and "AI케어응답자수" not in used:
            rename[c] = "AI케어응답자수"; used.add("AI케어응답자수")
        # N열: 안부확인 콜 발송 수
        elif "안부확인" in cl and "콜" in cl and "발송" in cl and "콜발송수" not in used:
            rename[c] = "콜발송수"; used.add("콜발송수")
        # O열: 안부확인 콜 응답자 수
        elif "안부확인" in cl and "콜" in cl and "응답자" in cl and "수" in cl and "콜응답자수" not in used:
            rename[c] = "콜응답자수"; used.add("콜응답자수")
        # R열: 안부미확인률 (Google Sheets 수식 결과)
        elif cl == "안부미확인률" and "안부미확인률" not in used:
            rename[c] = "안부미확인률"; used.add("안부미확인률")
        # S열: 48시간미확인률
        elif cl == "48시간미확인률" and "48시간미확인률" not in used:
            rename[c] = "48시간미확인률"; used.add("48시간미확인률")
        # T열: 안부체크응답률
        elif cl == "안부체크응답률" and "안부체크응답률" not in used:
            rename[c] = "안부체크응답률"; used.add("안부체크응답률")
        # U열: 안부확인 콜 응답률
        elif "안부확인" in cl and "콜" in cl and "응답률" in cl and "콜응답률" not in used:
            rename[c] = "콜응답률"; used.add("콜응답률")
        # V열: AI케어알 응답율
        elif "AI케어" in cl and "응답" in cl and "율" in cl and "AI케어응답률" not in used:
            rename[c] = "AI케어응답률"; used.add("AI케어응답률")
        # W열: ①안부체크 비중
        elif "안부체크" in cl and "비중" in cl and "안부체크비중" not in used:
            rename[c] = "안부체크비중"; used.add("안부체크비중")
        # X열: ②동작감지 비중
        elif "동작감지" in cl and "비중" in cl and "동작감지비중" not in used:
            rename[c] = "동작감지비중"; used.add("동작감지비중")
        # Y열: ③AI케어알람 비중
        elif "AI케어" in cl and "비중" in cl and "AI케어비중" not in used:
            rename[c] = "AI케어비중"; used.add("AI케어비중")
        # Z열: ④안부확인콜 비중
        elif "안부확인콜" in cl and "비중" in cl and "안부확인콜비중" not in used:
            rename[c] = "안부확인콜비중"; used.add("안부확인콜비중")
        # AA열: 안부체크율 (원본)
        elif cl == "안부체크율" and "안부체크율_원본" not in used:
            rename[c] = "안부체크율_원본"; used.add("안부체크율_원본")
        # AB열 근처: 전체 off 제외 대상자
        elif "off" in cl.lower() and "제외" in cl and "대상" in cl and "off제외대상자" not in used:
            rename[c] = "off제외대상자"; used.add("off제외대상자")
        # AC열 근처: 안부체크율(OFF 제외) — 이 값을 안부체크율로 사용
        elif "안부체크율" in cl and ("OFF" in cl or "off" in cl or "제외" in cl) and "안부체크율" not in used:
            rename[c] = "안부체크율"; used.add("안부체크율")

    if rename:
        df = df.rename(columns=rename)
    # 숫자 변환 (날짜 제외)
    for col in df.columns:
        if col not in ["날짜", "date"]:
            df[col] = df[col].apply(safe_numeric)
    # 안부확인율 = 100 - 안부미확인률 (R열 기반 파생 컬럼)
    if "안부미확인률" in df.columns:
        df["안부확인율"] = (100 - df["안부미확인률"]).round(1)
    return df


def get_c_col_total(df: pd.DataFrame) -> pd.DataFrame:
    """시트의 C열(합계) 추출 — 주차별 합계값 반환

    Google Sheets에서 A열=주차, B열=시작일, C열=합계 구조를 가정.
    컬럼명에 '합계'가 있으면 그 컬럼 사용, 없으면 3번째 컬럼 사용.

    Returns: DataFrame with [주차, 시작일, 값]
    """
    if df.empty:
        return pd.DataFrame()

    week_col, date_col, total_col = None, None, None
    for i, c in enumerate(df.columns):
        cl = str(c).replace("\n", "").strip()
        if "주차" in cl and week_col is None:
            week_col = c
        elif "시작일" in cl and date_col is None:
            date_col = c
        elif "합계" in cl and total_col is None:
            total_col = c

    if week_col is None:
        return pd.DataFrame()

    # 합계 컬럼이 이름으로 안 찾아지면 3번째 컬럼(C열) 사용
    if total_col is None and len(df.columns) >= 3:
        total_col = df.columns[2]

    if total_col is None:
        return pd.DataFrame()

    rows = []
    for _, row in df.iterrows():
        week = str(row.get(week_col, "")).strip()
        if not week or week == "nan":
            continue
        date = str(row.get(date_col, "")).strip() if date_col else ""
        val = safe_numeric(row.get(total_col, 0))
        rows.append({"주차": week, "시작일": date, "값": val})

    return pd.DataFrame(rows)


def get_ai_monthly(sheets: dict) -> pd.DataFrame:
    """AI생활지원사 월별 추이 데이터 (gid=552264832)

    컬럼 구조: 월별, 회원수, ..., 인트로, ..., 서비스, ..., 프로그램 완료
    '인트로'/'서비스'/'프로그램 완료' 컬럼이 각각 참여율(%) 값
    """
    df = sheets.get("AI생활지원사월별", pd.DataFrame())
    if df.empty:
        return df
    df = df.copy()
    rename = {}
    used = set()
    for c in df.columns:
        cl = str(c).replace("\n", "").replace(" ", "").strip()
        # 월별 컬럼
        if ("월별" in cl or cl == "월") and "월" not in used:
            rename[c] = "월"; used.add("월")
        # 회원수
        elif cl == "회원수" and "회원수" not in used:
            rename[c] = "회원수"; used.add("회원수")
        # 인트로 참여율 — 컬럼명이 딱 "인트로"
        elif cl == "인트로" and "인트로참여율" not in used:
            rename[c] = "인트로참여율"; used.add("인트로참여율")
        # 서비스 제안율 — 컬럼명이 딱 "서비스"
        elif cl == "서비스" and "서비스제안율" not in used:
            rename[c] = "서비스제안율"; used.add("서비스제안율")
        # 프로그램 완료율 — 컬럼명이 "프로그램완료" 또는 "프로그램 완료"
        elif cl == "프로그램완료" and "프로그램완료율" not in used:
            rename[c] = "프로그램완료율"; used.add("프로그램완료율")
        # AI 알림도달률
        elif "도달률" in cl and "AI알림도달률" not in used:
            rename[c] = "AI알림도달률"; used.add("AI알림도달률")
    if rename:
        df = df.rename(columns=rename)
    for col in ["인트로참여율", "서비스제안율", "프로그램완료율", "AI알림도달률", "회원수"]:
        if col in df.columns:
            df[col] = df[col].apply(safe_numeric)
    # 월 컬럼이 없으면 빈 반환
    if "월" not in df.columns:
        return pd.DataFrame()
    return df


def get_ai_mun_monthly(sheets: dict) -> dict:
    """삼척/양양/정선 지자체별 월별 데이터
    Returns dict: {"삼척시청": df, "양양군청": df, "정선군청": df}
    컬럼: 월, 회원수, 인트로참여율, 서비스제안율, 프로그램완료율
    """
    MUN_SHEET_MAP = {
        "삼척시청": "AI삼척월별",
        "양양군청": "AI양양월별",
        "정선군청": "AI정선월별",
    }
    result = {}
    for mun, sheet_key in MUN_SHEET_MAP.items():
        df = sheets.get(sheet_key, pd.DataFrame())
        if df.empty:
            continue
        df = df.copy()
        rename = {}
        used = set()
        for c in df.columns:
            cl = str(c).replace("\n", "").replace(" ", "").strip()
            if ("월별" in cl or cl == "월") and "월" not in used:
                rename[c] = "월"; used.add("월")
            elif cl == "회원수" and "회원수" not in used:
                rename[c] = "회원수"; used.add("회원수")
            elif cl == "인트로" and "인트로참여율" not in used:
                rename[c] = "인트로참여율"; used.add("인트로참여율")
            elif cl == "서비스" and "서비스제안율" not in used:
                rename[c] = "서비스제안율"; used.add("서비스제안율")
            elif cl == "프로그램완료" and "프로그램완료율" not in used:
                rename[c] = "프로그램완료율"; used.add("프로그램완료율")
        if rename:
            df = df.rename(columns=rename)
        for col in ["인트로참여율", "서비스제안율", "프로그램완료율", "회원수"]:
            if col in df.columns:
                df[col] = df[col].apply(safe_numeric)
        if "월" not in df.columns:
            continue
        df = df[df["월"].notna() & (df["월"].astype(str).str.strip() != "")]
        if not df.empty:
            result[mun] = df.reset_index(drop=True)
    return result


def get_ai_funnel(sheets: dict) -> pd.DataFrame:
    """시트20: AI생활지원사 알림 - funnel 데이터"""
    df = sheets.get("AI생활지원사", pd.DataFrame())
    if df.empty:
        return df
    df = df.copy()
    # 주차 컬럼 찾기
    rename = {}
    for c in df.columns:
        cl = str(c).replace("\n", "").strip()
        if "주차" in cl:
            rename[c] = "주차"
        elif "시작일" in cl:
            rename[c] = "시작일"
    if rename:
        df = df.rename(columns=rename)
    return df


def get_ai_municipality_data(sheets: dict) -> pd.DataFrame:
    """AI생활지원사 신규 시트(gid=887906400): 지자체별 주차 데이터

    시트 구조 (실제):
      - col[0] '구분': 주차 번호 (e.g. '11주차') — 각 주 첫 행에만 있고 나머지 NaN
      - col[1]: 날짜 범위 (e.g. '3월 8일~14일') — 각 주 첫 행에만 있고 나머지 NaN
      - 지자체명 컬럼: '통합' / '삼척시청' / '양양군청' / '정선군청' — 자동 탐지
      - 계약인원, 가입인원, 알람요일, receiveAlarmCount, receiveAlarmUserCount
      - intro, intro(%): 인트로 수/율
      - service proposal(%): 서비스 제안율
      - program(%): 프로그램 완료율
    """
    df = sheets.get("AI생활지원사신규", pd.DataFrame())
    if df.empty:
        return pd.DataFrame()
    df = df.copy()

    period_col    = df.columns[0]   # '구분' — 주차 번호 (e.g. '11주차')
    date_range_col = df.columns[1]  # 날짜범위 (e.g. '3월 8일~14일')

    # 두 컬럼 forward-fill (각 주 첫 행에만 값이 있음)
    df[period_col]    = df[period_col].ffill()
    df[date_range_col] = df[date_range_col].ffill()

    # 외부 시트 월간구분 매핑: (주차번호, 지자체명) → "3월 2주" 등
    import re as _re2
    _gubn_map = sheets.get("ai_gubn_mapping", {})

    # ── 지자체명 컬럼 자동 탐지 ─────────────────────────────────────────
    # '통합' 또는 시청/군청/서비스원 등 지자체 키워드를 포함하는 컬럼 탐색
    _MUN_KEYWORDS = ["통합", "시청", "군청", "구청", "서비스원", "복지관", "센터", "삼척", "양양", "정선"]
    name_col = None
    for c in df.columns[2:]:   # col[0]=주차, col[1]=날짜범위 이후부터 탐색
        vals = df[c].dropna().astype(str).str.strip()
        if any(any(kw in v for kw in _MUN_KEYWORDS) for v in vals):
            name_col = c
            break

    if name_col is None:
        # fallback: 두 번째 컬럼부터 순서대로 시도
        for c in df.columns[1:]:
            vals = df[c].dropna().astype(str).str.strip()
            if any(any(kw in v for kw in _MUN_KEYWORDS) for v in vals):
                name_col = c
                break

    if name_col is None:
        return pd.DataFrame()

    # ── 수치 컬럼 정리 ───────────────────────────────────────────────────
    num_cols = ["계약인원", "가입인원", "receiveAlarmCount", "receiveAlarmUserCount",
                "intro", "intro(%)", "service proposal", "service proposal(%)",
                "program complete", "program(%)"]

    result_rows = []
    for _, row in df.iterrows():
        name = str(row.get(name_col, "")).strip()
        if name in ("nan", "", "NaN"):
            continue
        period    = str(row.get(period_col,    "")).strip()   # '11주차'
        date_range = str(row.get(date_range_col, "")).strip() # '3월 8일~14일'
        alarm_day = str(row.get("알람요일", "")).strip()
        alarm_day = "" if alarm_day in ("nan", "NaN") else alarm_day

        # 외부 시트 C열: (주차번호, 지자체명) → "3월 2주" 매핑
        _wk_m = _re2.search(r'(\d+)주차', period)
        _wk_num = int(_wk_m.group(1)) if _wk_m else 0
        gubn = _gubn_map.get((_wk_num, name), "")

        # 기간: '11주차 (3월 8일~14일)' 형태로 조합 → 월 정보 포함
        if period and date_range and date_range not in ("nan", "NaN"):
            full_period = f"{period} ({date_range})"
        else:
            full_period = period or date_range

        r = {"기간": full_period, "지자체": name, "알람요일": alarm_day, "월간구분": gubn}
        for nc in num_cols:
            if nc in df.columns:
                r[nc] = safe_numeric(row.get(nc, 0))
        result_rows.append(r)

    if not result_rows:
        return pd.DataFrame()
    return pd.DataFrame(result_rows)


def get_app_deletion_data(sheets: dict) -> pd.DataFrame:
    """시트3: 심혈관현황 시트에서 앱삭제율 데이터 추출"""
    df = sheets.get("심혈관현황", pd.DataFrame())
    if df.empty:
        return df
    df = df.copy()
    # 주차 컬럼 찾기
    week_col = None
    date_col = None
    for c in df.columns:
        cl = str(c).replace("\n", "").strip()
        if "주차" in cl:
            week_col = c
        elif "시작일" in cl:
            date_col = c

    if week_col is None:
        return pd.DataFrame()

    # 앱삭제율 컬럼 추출
    deletion_cols = []
    user_cols = []
    for c in df.columns:
        cl = str(c).replace("\n", "").replace(" ", "").strip()
        if "삭제율" in cl and any(kw in cl for kw in MUNICIPALITY_KEYWORDS):
            deletion_cols.append(c)
        elif "이용자수" in cl and any(kw in cl for kw in MUNICIPALITY_KEYWORDS):
            user_cols.append(c)

    # 총 앱삭제의심자, WoW 컬럼
    total_delete_col = None
    wow_col = None
    for c in df.columns:
        cl = str(c).replace("\n", "").strip()
        if "앱" in cl and "삭제" in cl and "의심" in cl:
            total_delete_col = c
        elif cl == "WoW" or cl == "wow":
            wow_col = c

    # Long format 변환 (삭제율) - 빈칸은 제외 (계약 종료 지자체)
    rows = []
    for _, row in df.iterrows():
        week = str(row.get(week_col, "")).strip()
        date = str(row.get(date_col, "")).strip() if date_col else ""
        if not week or week == "nan":
            continue
        for dc in deletion_cols:
            mun_name = extract_municipality_name(dc)
            raw_val = row.get(dc, None)
            # 빈칸/NaN 체크 → 계약 종료 지자체는 건너뜀
            if pd.isna(raw_val) or str(raw_val).strip() == "":
                continue
            val = safe_numeric(raw_val)
            rows.append({
                "주차": week,
                "시작일": date,
                "지자체명": mun_name,
                "앱삭제율": val,
            })

    return pd.DataFrame(rows)


def get_mun_check_rate_direct() -> pd.DataFrame:
    """안부확인지자체 시트 QF~(지자체별 "안부체크율1" 컬럼 전체)에서 안부체크율 직접 읽기

    (예전엔 PH:QJ를 썼는데, 지자체가 늘면서 안부체크율1 컬럼 그룹이 오른쪽으로
    밀려나 일부만 걸리고 나머지는 다른 컬럼(안부콜응답률)이 섞여 들어가거나
    아예 빠져서 0으로 잘못 표시되는 버그가 있었음 — 예: 포천시청가 QF:RJ 안에
    있는데 PH:QJ 범위엔 안 걸려서 0%로 잘못 나왔음)

    QF:RJ로 고쳤을 때도 RJ가 당시 시트의 마지막 컬럼과 정확히 일치해서 여유가
    0이었음 — 지자체가 하나만 더 늘어도 이 블록이 RJ를 넘어가면서 같은 버그가
    바로 재발하는 구조였음. 끝 경계를 ZZ까지 넉넉히 잡아 앞으로 지자체가 계속
    늘어도 안 깨지게 함 (이름 매칭으로 걸러내므로 범위를 넓게 잡아도 안전함).

    최신 행(마지막 주차)의 값을 반환. 값 형식 "X%"는 safe_numeric으로 처리.
    Returns: DataFrame [지자체명, 안부체크율]
    """
    GID = SHEET_GIDS["안부확인지자체"]
    url = BASE_URL + GID + "&range=QF:ZZ"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        content = resp.content.decode("utf-8-sig")
        df = pd.read_csv(io.StringIO(content))
        df = df.dropna(how="all")
    except Exception as e:
        print(f"[sheets_data] get_mun_check_rate_direct error: {e}")
        return pd.DataFrame()

    if df.empty:
        return pd.DataFrame()

    header = list(df.columns)
    latest_row = df.iloc[-1].values

    rows = []
    for i, col_name in enumerate(header):
        name_raw = str(col_name).replace("\n", "").replace(" ", "").strip()
        if "체크율" not in name_raw:
            continue  # 범위가 밀려서 다른 지표(안부콜응답률 등)가 섞여 들어오는 걸 방지
        # normalize_agency_name()은 자체 부분일치 폴백이 있어 "용인시청"(구)이
        # "용인시청 통합돌봄"의 부분문자열이라는 이유로 다시 뒤섞일 수 있어 쓰지 않음 —
        # match_municipality_keyword가 고른 키워드를 그대로 지자체명으로 사용
        mun_name = match_municipality_keyword(name_raw)
        if mun_name is None:
            continue
        if mun_name not in ALL_KNOWN_AGENCIES:
            continue
        rate = safe_numeric(latest_row[i])
        rows.append({"지자체명": mun_name, "안부체크율": rate})

    # 누락 기관 0으로 패딩
    found = {r["지자체명"] for r in rows}
    for agency in ALL_KNOWN_AGENCIES:
        if agency not in found:
            rows.append({"지자체명": agency, "안부체크율": 0.0})

    return pd.DataFrame(rows)


def _resolve_checkin_mun_denom_numer(header: list) -> tuple:
    """안부확인지자체 시트에서 지자체별 (분모 컬럼 인덱스, 분자 컬럼 인덱스)를 헤더 텍스트로 찾는다.

    분모 컬럼은 지자체명만(접미사 없음), 분자 컬럼은 "지자체명 안부확인자"처럼 접미사가 붙어있음.
    예전 코드는 분모→분자 오프셋을 고정값(35)으로 가정했는데, 지자체가 늘어(동해시청·
    인천사회서비스원 추가) 분모 블록이 37칸으로 커지면서 오프셋이 어긋나 엉뚱한 지자체
    값이 섞여 들어가는 버그가 있었음. 위치 가정 없이 헤더 이름만으로 매칭해서 앞으로
    지자체가 더 늘어도 안 깨지게 함.

    Returns: (denom_idx: {지자체명: 컬럼인덱스}, numer_idx: {지자체명: 컬럼인덱스})
    """
    known_flat = {a.replace(" ", ""): a for a in ALL_KNOWN_AGENCIES}
    alias_flat = {k.replace(" ", ""): v for k, v in NAME_ALIASES.items()}

    denom_idx, numer_idx = {}, {}
    for i, col in enumerate(header):
        if i < 2:  # 시작일, Total 컬럼 스킵
            continue
        flat = str(col).replace("\n", "").replace(" ", "").strip()
        is_numer = "안부확인자" in flat
        name_part = flat.replace("안부확인자", "")
        # 정확히 일치할 때만 채택 — 부분일치를 쓰면 "용인시청"(폐지된 옛 컬럼)이
        # "용인시청통합돌봄"(현재 활성 컬럼)의 부분 문자열이라 서로 뒤섞이는 문제가 있었음
        mun_name = None
        if name_part in alias_flat and alias_flat[name_part] in ALL_KNOWN_AGENCIES:
            mun_name = alias_flat[name_part]
        elif name_part in known_flat:
            mun_name = known_flat[name_part]
        if mun_name is None:
            continue
        target = numer_idx if is_numer else denom_idx
        target.setdefault(mun_name, i)  # 같은 이름의 뒤쪽 블록(안부미확인 등)과 안 겹치게 첫 매칭만 사용
    return denom_idx, numer_idx


def get_checkin_mun_rate_direct() -> pd.DataFrame:
    """안부확인지자체 시트에서 지자체별 최신 안부확인율을 헤더 이름 매칭으로 계산

    gviz range=A:CZ로 넉넉히 fetch 후 _resolve_checkin_mun_denom_numer()로 이름 매칭
    Returns: DataFrame [지자체명, 분모, 분자, 안부확인율, 시작일]
    """
    GID = SHEET_GIDS["안부확인지자체"]
    url = BASE_URL + GID + "&range=A:CZ"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        content = resp.content.decode("utf-8-sig")
        df = pd.read_csv(io.StringIO(content))
        df = df.dropna(how="all")
    except Exception as e:
        print(f"[sheets_data] get_checkin_mun_rate_direct error: {e}")
        return pd.DataFrame()

    if df.empty or len(df.columns) < 40:
        return pd.DataFrame()

    header = list(df.columns)
    denom_idx, numer_idx = _resolve_checkin_mun_denom_numer(header)

    # 최신 유효 날짜 행 (YYYY-MM-DD 형식)
    date_series = df.iloc[:, 0].astype(str).str.strip()
    valid_mask = date_series.str.match(r"\d{4}-\d{2}-\d{2}")
    valid_df = df[valid_mask]
    if valid_df.empty:
        return pd.DataFrame()

    latest_date = valid_df.iloc[-1, 0]
    latest_vals = valid_df.iloc[-1].values

    rows = []
    for mun_name, d_i in denom_idx.items():
        n_i = numer_idx.get(mun_name)
        if n_i is None:
            continue
        denom = safe_numeric(latest_vals[d_i])
        numer = safe_numeric(latest_vals[n_i])
        rate = round(numer / denom * 100, 1) if denom > 0 else 0.0
        rows.append({"지자체명": mun_name, "분모": int(denom), "분자": int(numer),
                     "안부확인율": rate, "시작일": str(latest_date)})

    # 누락 기관 → 0으로 패딩
    found = {r["지자체명"] for r in rows}
    for agency in ALL_KNOWN_AGENCIES:
        if agency not in found:
            rows.append({"지자체명": agency, "분모": 0, "분자": 0,
                         "안부확인율": 0.0, "시작일": str(latest_date)})

    return pd.DataFrame(rows)


def get_checkin_mun_weekly() -> pd.DataFrame:
    """안부확인지자체 시트에서 지자체별 안부확인율 시계열을 헤더 이름 매칭으로 계산
    (long-format, 모든 주차)

    gviz range=A:CZ로 넉넉히 fetch 후 _resolve_checkin_mun_denom_numer()로 이름 매칭
    Returns: DataFrame [시작일, 지자체명, 분모, 분자, 안부확인율]
    """
    GID = SHEET_GIDS["안부확인지자체"]
    url = BASE_URL + GID + "&range=A:CZ"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        content = resp.content.decode("utf-8-sig")
        df = pd.read_csv(io.StringIO(content))
        df = df.dropna(how="all")
    except Exception as e:
        print(f"[sheets_data] get_checkin_mun_weekly error: {e}")
        return pd.DataFrame()

    if df.empty or len(df.columns) < 40:
        return pd.DataFrame()

    header = list(df.columns)
    denom_idx, numer_idx = _resolve_checkin_mun_denom_numer(header)

    date_series = df.iloc[:, 0].astype(str).str.strip()
    valid_mask = date_series.str.match(r"\d{4}-\d{2}-\d{2}")
    valid_df = df[valid_mask].reset_index(drop=True)
    if valid_df.empty:
        return pd.DataFrame()

    rows = []
    for row_idx in range(len(valid_df)):
        date_val = str(valid_df.iloc[row_idx, 0])
        vals = valid_df.iloc[row_idx].values

        for mun_name, d_i in denom_idx.items():
            n_i = numer_idx.get(mun_name)
            if n_i is None:
                continue
            denom = safe_numeric(vals[d_i])
            numer = safe_numeric(vals[n_i])
            if denom > 0:
                rows.append({
                    "시작일": date_val, "지자체명": mun_name,
                    "분모": int(denom), "분자": int(numer),
                    "안부확인율": round(numer / denom * 100, 1),
                })

    return pd.DataFrame(rows)


def get_checkin_municipality_rate(sheets: dict) -> pd.DataFrame:
    """안부확인지자체 시트에서 지자체별 안부체크율 추출 (off 대상자 반영)

    수정된 계산: 안부체크율 = 안부체크응답 / (안부체크발송 - off대상자) × 100

    Returns: DataFrame with [시작일, 지자체명, 안부체크율, 안부체크율_원본, 안부체크발송, 안부체크응답, off대상자, ...]
    """
    df = sheets.get("안부확인지자체", pd.DataFrame())
    if df.empty:
        return df
    df = df.copy()

    # off 대상자 가져오기
    off_users = get_check_off_users(sheets)

    # 시작일 컬럼 찾기
    date_col = None
    for c in df.columns:
        cl = str(c).replace("\n", "").strip()
        if "시작일" in cl:
            date_col = c
            break
    if date_col is None:
        date_col = df.columns[0]

    # 지자체별 컬럼 찾기 (안부체크율 + 발송 + 응답 + 기타 비율)
    rate_columns = {}  # {지자체명: {지표: 컬럼명}}
    for c in df.columns:
        cl = str(c).replace("\n", "").replace(" ", "").strip()
        kw = match_municipality_keyword(cl)
        if kw is not None:
            # normalize_agency_name()은 자체 부분일치 폴백이 있어 "용인시청"(구)이
            # "용인시청 통합돌봄"의 부분문자열이라는 이유로 다시 뒤섞일 수 있음 —
            # match_municipality_keyword가 이미 올바른 키워드를 골라준 뒤이므로
            # 여기선 그 키워드를 그대로 키로 쓴다(기존 동작과 동일).
            mun_key = kw
            if "안부체크율1" in cl:
                # LP~MJ열: 직접 할당(override) — IT~JR보다 우선
                rate_columns.setdefault(mun_key, {})
                rate_columns[mun_key]["안부체크율_원본"] = c
            elif "안부체크율" in cl:
                # IT~JR열: LP~MJ가 없을 때만 fallback
                rate_columns.setdefault(mun_key, {}).setdefault("안부체크율_원본", c)
            elif "안부체크발송" in cl:
                rate_columns.setdefault(mun_key, {})["안부체크발송"] = c
            elif "안부체크응답" in cl:
                rate_columns.setdefault(mun_key, {})["안부체크응답"] = c
            elif "안부확인율" in cl:
                rate_columns.setdefault(mun_key, {})["안부확인율"] = c
            elif "48미확인율" in cl or "48시간미확인율" in cl:
                rate_columns.setdefault(mun_key, {})["48미확인율"] = c
            elif "안부콜응답률" in cl:
                rate_columns.setdefault(mun_key, {})["안부콜응답률"] = c

    if not rate_columns:
        return pd.DataFrame()

    # Long format 변환 + off 대상자 반영
    rows = []
    for _, row in df.iterrows():
        date_val = str(row.get(date_col, "")).strip()
        if not date_val or date_val == "nan":
            continue
        for mun_name, metrics in rate_columns.items():
            entry = {"시작일": date_val, "지자체명": mun_name}
            has_data = False

            # 기존 비율 지표
            for metric_name, col_name in metrics.items():
                val = row.get(col_name, None)
                if pd.isna(val) or str(val).strip() == "":
                    entry[metric_name] = None
                else:
                    entry[metric_name] = safe_numeric(val)
                    has_data = True

            # off 대상자 반영하여 안부체크율 재계산
            send = entry.get("안부체크발송", None)
            resp = entry.get("안부체크응답", None)
            off = off_users.get(mun_name, 0)
            entry["off대상자"] = off

            # IT~JR 사전 계산값 우선 사용 (Google Sheets 수식 결과가 더 정확)
            # 단, 안부체크 발송이 0인 경우 시트 수식 오류값 무시 (회원 0명 기관 등)
            orig = entry.get("안부체크율_원본")
            if orig is not None and orig > 0 and send is not None and send > 0:
                entry["안부체크율"] = orig
            elif send is not None and resp is not None and send > 0:
                if off >= send:
                    # off대상자 명단이 정리 안 돼서 발송인원보다 많아지는 경우(예: 희망나래
                    # — off 11명 > 발송 8명). 이땐 off 보정 자체가 의미가 없으므로
                    # (분모를 억지로 맞추면 0%로 지워지거나 200%처럼 더 엉뚱한 값이 나옴)
                    # off 보정을 포기하고 원래 발송 기준으로 계산
                    entry["안부체크율"] = round(resp / send * 100, 1)
                else:
                    actual_target = send - off
                    entry["안부체크율"] = round(resp / actual_target * 100, 1)
            else:
                entry["안부체크율"] = None

            if has_data:
                rows.append(entry)

    return pd.DataFrame(rows)


def get_health_consult_by_municipality(sheets: dict) -> pd.DataFrame:
    """건강상담 지자체별 서비스유형별 이용현황 (gid=867975933)

    컬럼 구조 (위치 기반):
      A(0): 날짜(주차),  B(1): 지자체,
      C(2): 전문의료진상담, D(3): 병원안내, E(4): 일반상담, F(5): 진료예약

    Returns: DataFrame [날짜, 지자체, 전문의료진상담, 병원안내, 일반상담, 진료예약, 합계]
    """
    df = sheets.get("건강상담지자체", pd.DataFrame())
    if df.empty or len(df.columns) < 3:
        return pd.DataFrame()

    df = df.copy()
    cols = list(df.columns)

    # 위치 기반 컬럼 이름 매핑
    POSITION_MAP = {0: "날짜", 1: "지자체", 2: "전문의료진상담",
                    3: "병원안내", 4: "일반상담", 5: "진료예약"}
    rename = {cols[i]: name for i, name in POSITION_MAP.items() if i < len(cols)}
    df = df.rename(columns=rename)

    SERVICE_COLS = [c for c in ["전문의료진상담", "병원안내", "일반상담", "진료예약"]
                    if c in df.columns]

    # 날짜/지자체 비어있는 행 제거 + 날짜 포맷 정규화 (20260413 → 2026-04-13)
    def _normalize_date(v):
        s = str(v).strip().split(".")[0]  # float "20260413.0" → "20260413"
        if len(s) == 8 and s.isdigit():
            return f"{s[:4]}-{s[4:6]}-{s[6:]}"  # YYYYMMDD → YYYY-MM-DD
        return s

    df["날짜"]   = df["날짜"].apply(_normalize_date)
    df["지자체"] = df["지자체"].astype(str).str.strip()
    # 합계/소계/합산 행 제거 (시트 내 집계 행)
    _exclude = {"합계", "소계", "합산", "total", "sum"}
    df = df[(df["날짜"] != "") & (df["날짜"] != "nan") &
            (df["지자체"] != "") & (df["지자체"] != "nan") &
            (~df["지자체"].str.lower().isin(_exclude))].copy()

    # 서비스 컬럼 숫자 변환
    for c in SERVICE_COLS:
        df[c] = df[c].apply(safe_numeric)

    df["합계"] = df[SERVICE_COLS].sum(axis=1)
    # 비영 필터 제거 — 0인 행도 포함하여 주차별 집계 시 모든 일별 데이터 반영

    return df[["날짜", "지자체"] + SERVICE_COLS + ["합계"]].reset_index(drop=True)


# ============================================================
# 통합 대시보드 데이터 생성
# ============================================================

def build_dashboard_data(sheets: dict) -> dict:
    """모든 시트 데이터를 대시보드용으로 가공하여 반환"""
    result = {}

    # 1. 회원가입 현황 (지자체별 스냅샷)
    result["registration"] = get_registration_status(sheets)

    # 2. 주간 이용자 추이
    result["weekly_users"] = get_weekly_users(sheets)

    # 3. 안부확인 일별 전체
    result["checkin_daily"] = get_checkin_daily(sheets)

    # 3-1. 지자체별 안부체크율 (안부확인지자체 시트의 수식 결과)
    result["checkin_municipality_rate"] = get_checkin_municipality_rate(sheets)

    # 3-2. 지자체별 안부확인율 (C:AK 분모 / AL:BT 분자 직접 계산)
    result["checkin_mun_rate_direct"] = get_checkin_mun_rate_direct()

    # 3-2b. 주차별 지자체별 안부확인율 시계열 (사업구분별 필터 용)
    result["checkin_mun_weekly"] = get_checkin_mun_weekly()

    # 3-3. 지자체별 안부체크율 (PH:QJ 직접 계산)
    result["checkin_mun_check_direct"] = get_mun_check_rate_direct()

    # 4. 지자체별 주간 데이터 (여러 시트)
    for key, label in [
        ("안부체크횟수", "안부체크"),
        ("심혈관이용자", "심혈관이용자"),
        ("심혈관검사횟수", "심혈관검사"),
        ("복약등록회원", "복약등록회원"),
        ("복약등록건수", "복약등록건수"),
        ("맞고와플게스트", "맞고와플게스트"),
        ("맞고이용자", "맞고이용자"),
        ("맞고플레이판수", "맞고플레이판수"),
        ("맞고플레이시간", "맞고플레이시간"),
        ("맞고게스트", "맞고게스트"),
        ("스트레스이용자", "스트레스이용자"),
        ("스트레스수행횟수", "스트레스수행횟수"),
    ]:
        result[f"weekly_{label}"] = get_weekly_municipality_data(sheets, key)

    # 5. 앱삭제율
    result["app_deletion"] = get_app_deletion_data(sheets)

    # 6. AI 생활지원사 funnel + 월별 추이 + 지자체별
    result["ai_funnel"] = get_ai_funnel(sheets)
    result["ai_monthly"] = get_ai_monthly(sheets)
    result["ai_municipality"] = get_ai_municipality_data(sheets)
    result["ai_municipality_ext"] = get_ai_municipality_ext(sheets)
    result["ai_mun_monthly"] = get_ai_mun_monthly(sheets)
    result["weekly_registered_by_mun"] = get_weekly_registered_by_municipality(sheets)

    # 6-1. 심혈관/스트레스 C열(합계) 직접 추출
    for _sheet_key, _result_key in [
        ("심혈관이용자",   "total_심혈관이용자"),
        ("심혈관검사횟수", "total_심혈관검사"),
        ("스트레스이용자",   "total_스트레스이용자"),
        ("스트레스수행횟수", "total_스트레스수행횟수"),
        ("복약등록건수",   "total_복약등록건수"),
    ]:
        _raw = sheets.get(_sheet_key, pd.DataFrame())
        result[_result_key] = get_c_col_total(_raw)

    # 7. 집계형 시트 (전체 추이용)
    for key in ["건강상담", "생활상담"]:
        result[key] = sheets.get(key, pd.DataFrame())

    # 8. 건강상담 지자체별 서비스 유형별 이용현황
    result["건강상담지자체"] = get_health_consult_by_municipality(sheets)

    # 8. DB fallback — Google Sheets 데이터가 비어있으면 DB에서 가져오기
    try:
        import sqlite3
        _db_path = __import__('os').path.join(__import__('os').path.dirname(__file__), 'waplat.db')
        if __import__('os').path.exists(_db_path):
            _conn = sqlite3.connect(_db_path)

            # raw_generic에서 주차별 지자체 데이터 복원
            _type_map = {
                "심혈관이용자": "weekly_심혈관이용자",
                "심혈관검사횟수": "weekly_심혈관검사",
                "스트레스이용자": "weekly_스트레스이용자",
                "스트레스수행횟수": "weekly_스트레스수행횟수",
                "맞고이용자": "weekly_맞고이용자",
                "맞고플레이판수": "weekly_맞고플레이판수",
                "맞고플레이시간": "weekly_맞고플레이시간",
                "복약등록회원": "weekly_복약등록회원",
                "복약등록건수": "weekly_복약등록건수",
            }

            for db_type, result_key in _type_map.items():
                if result_key not in result or result[result_key].empty:
                    import json as _json
                    _rows = _conn.execute(
                        "SELECT date, agency_name, raw_json FROM raw_generic WHERE data_type = ? ORDER BY date, agency_name",
                        (db_type,)
                    ).fetchall()
                    if _rows:
                        _data_rows = []
                        for _r in _rows:
                            try:
                                _d = _json.loads(_r[2])
                                _val = 0
                                for _k, _v in _d.items():
                                    if _k not in ("date", "date_end", "agency_name", "agency_seq", "org_name"):
                                        try:
                                            _val = float(_v) if _v else 0
                                            break
                                        except:
                                            pass
                                _data_rows.append({"주차": _r[0], "지자체명": _r[1], "값": _val})
                            except:
                                pass
                        if _data_rows:
                            result[result_key] = pd.DataFrame(_data_rows)

            # 집계형 시트도 DB에서 복원
            for _agg_type in ["맞고와플게스트", "맞고게스트", "건강상담", "생활상담", "AI생활지원사"]:
                _rkey = f"weekly_{_agg_type}" if _agg_type not in result else _agg_type
                if _rkey not in result or (isinstance(result.get(_rkey), pd.DataFrame) and result[_rkey].empty):
                    _rows = _conn.execute(
                        "SELECT date, raw_json FROM raw_generic WHERE data_type = ? AND agency_name = 'ALL' ORDER BY date",
                        (_agg_type,)
                    ).fetchall()
                    if _rows:
                        import json as _json
                        _data_rows = []
                        for _r in _rows:
                            try:
                                _d = _json.loads(_r[2])
                                _d["주차"] = _r[0]
                                _data_rows.append(_d)
                            except:
                                pass
                        if _data_rows:
                            result[_rkey] = pd.DataFrame(_data_rows)

            _conn.close()
    except Exception as _e:
        pass  # DB 실패 시 무시

    # 9. 주차 목록 추출 (이용자주간 시트 기준)
    if "weekly_users" in result and not result["weekly_users"].empty:
        wu = result["weekly_users"]
        if "주차" in wu.columns:
            weeks = wu["주차"].dropna().unique().tolist()
            weeks = [str(w).strip() for w in weeks if str(w).strip() and str(w).strip() != "nan"]
            result["주차목록"] = sorted(weeks)
        else:
            result["주차목록"] = []
    else:
        result["주차목록"] = []

    # 10. 주차별 안부확인율 사전 계산 — C열(안부확인완료자)/B열(전체회원)*100 직접 계산
    _cd = result.get("checkin_daily", pd.DataFrame())
    _wu = result.get("weekly_users", pd.DataFrame())
    _weekly_cr = {}
    if not _cd.empty and not _wu.empty:
        _date_col = next((c for c in _cd.columns
                          if str(c).replace("\n","").strip().lower() in ("날짜","date","일자","일")
                          or "날짜" in str(c) or "date" in str(c).lower()), None)
        _comp_col = next((c for c in _cd.columns if "완료자" in str(c) and "안부확인" in str(c)), None)
        _total_col = next((c for c in _cd.columns if str(c).replace("\n","").strip() in ("전체회원", "전체 회원")), None)
        if _date_col and _comp_col and _total_col and "주차" in _wu.columns and "시작일" in _wu.columns:
            try:
                _wmap = {}
                for _, _r in _wu.iterrows():
                    _rs = pd.to_datetime(str(_r["시작일"]), errors="coerce")
                    if pd.isna(_rs):
                        continue
                    _wk = str(_r["주차"]).strip()
                    for _i in range(7):
                        _wmap[(_rs + pd.Timedelta(days=_i)).strftime("%Y-%m-%d")] = _wk
                _cd2 = _cd.copy()
                _cd2["_comp"] = _cd2[_comp_col].apply(safe_numeric)
                _cd2["_total"] = _cd2[_total_col].apply(safe_numeric)
                _cd2["_cr"] = (_cd2["_comp"] / _cd2["_total"].replace(0, float("nan")) * 100).round(1)
                _dt = pd.to_datetime(_cd2[_date_col].astype(str), errors="coerce")
                _cd2["_wk"] = _dt.dt.strftime("%Y-%m-%d").map(_wmap)
                _cd2 = _cd2[_cd2["_wk"].notna() & (_cd2["_cr"] > 0) & (_cd2["_cr"] < 100.0)]
                _weekly_cr = _cd2.groupby("_wk")["_cr"].mean().round(1).to_dict()
            except Exception:
                pass
    result["weekly_안부확인율"] = _weekly_cr

    return result


def get_week_summary(sheets: dict, data: dict, week: str) -> dict:
    """특정 주차의 요약 데이터 반환"""
    summary = {"주차": week}

    # 이용자 주간 데이터에서 해당 주차 추출
    wu = data.get("weekly_users", pd.DataFrame())
    if not wu.empty and "주차" in wu.columns:
        week_row = wu[wu["주차"].astype(str).str.strip() == week]
        if not week_row.empty:
            row = week_row.iloc[0]
            summary["시작일"] = str(row.get("시작일", ""))
            summary["가입완료합계"] = safe_numeric(row.get("가입완료합계", 0))
            summary["주간가입완료"] = safe_numeric(row.get("주간가입완료", 0))
            summary["주간활성사용자"] = safe_numeric(row.get("주간활성사용자", 0))
            summary["주간이탈자"] = safe_numeric(row.get("주간이탈자", 0))

    # 안부체크 일별 데이터에서 최근 7일 평균 안부체크율
    cd = data.get("checkin_daily", pd.DataFrame())
    if not cd.empty and "안부체크율" in cd.columns:
        cd_valid = cd[cd["안부체크율"].apply(safe_numeric) > 0].copy()
        if not cd_valid.empty:
            recent7 = cd_valid.tail(7)
            avg_rate = round(recent7["안부체크율"].apply(safe_numeric).mean(), 1)
            summary["안부체크율"] = avg_rate
        latest = cd.iloc[-1] if len(cd) > 0 else None
        if latest is not None:
            summary["전체회원"] = safe_numeric(latest.get("전체회원", 0))
            summary["안부확인완료자"] = safe_numeric(latest.get("안부확인완료자", 0))

    # 안부확인율: build_dashboard_data에서 사전 계산된 주차별 평균 조회
    _weekly_cr = data.get("weekly_안부확인율", {})
    _wk_key = str(week).strip()
    if _wk_key in _weekly_cr:
        summary["안부확인율"] = round(float(_weekly_cr[_wk_key]), 1)

    return summary


# ============================================================
# 주차별 지자체 비교 데이터 (히트맵용)
# ============================================================

def build_municipality_heatmap_data(data: dict, week: str) -> pd.DataFrame:
    """특정 주차의 지자체별 지표 히트맵 데이터 생성"""
    rows = []

    # 회원가입 완료율
    reg = data.get("registration", pd.DataFrame())
    reg_dict = {}
    if not reg.empty and "지자체명" in reg.columns:
        for _, r in reg.iterrows():
            name = str(r["지자체명"]).strip()
            reg_dict[name] = safe_numeric(r.get("완료율", 0))

    # 앱삭제율
    del_df = data.get("app_deletion", pd.DataFrame())
    del_dict = {}
    if not del_df.empty:
        week_del = del_df[del_df["주차"].astype(str).str.strip() == week]
        del_dict = dict(zip(week_del["지자체명"], week_del["앱삭제율"]))

    # 심혈관 이용자 수 (참고 지표)
    cardio_dict = {}
    cardio_df = data.get("weekly_심혈관이용자", pd.DataFrame())
    if not cardio_df.empty:
        week_cardio = cardio_df[cardio_df["주차"].astype(str).str.strip() == week]
        cardio_dict = dict(zip(week_cardio["지자체명"], week_cardio["값"]))

    # 맞고 이용자 수 (참고 지표)
    matgo_dict = {}
    matgo_df = data.get("weekly_맞고이용자", pd.DataFrame())
    if not matgo_df.empty:
        week_matgo = matgo_df[matgo_df["주차"].astype(str).str.strip() == week]
        matgo_dict = dict(zip(week_matgo["지자체명"], week_matgo["값"]))

    # 스트레스 이용자 수
    stress_dict = {}
    stress_df = data.get("weekly_스트레스이용자", pd.DataFrame())
    if not stress_df.empty:
        week_stress = stress_df[stress_df["주차"].astype(str).str.strip() == week]
        stress_dict = dict(zip(week_stress["지자체명"], week_stress["값"]))

    # 협약인원 (비율 계산용)
    contract_dict = {}
    if not reg.empty and "지자체명" in reg.columns and "협약인원" in reg.columns:
        for _, r in reg.iterrows():
            name = str(r["지자체명"]).strip()
            contract_dict[name] = safe_numeric(r.get("협약인원", 0))

    # 지자체명 부분 매칭 헬퍼 (extract_municipality_name이 이름을 줄이는 경우 대비)
    def fuzzy_get(d, key, default=0):
        """정확한 키가 없으면 부분 매칭으로 시도"""
        if key in d:
            return d[key]
        # key가 다른 키에 포함되어 있거나, 다른 키가 key에 포함
        for k, v in d.items():
            if key in k or k in key:
                return v
        return default

    # 해당 주차에 데이터가 있는 지자체만 (빈칸 = 계약 종료 = 제외)
    active_muns = set(del_dict.keys())
    if active_muns:
        all_muns = active_muns
    else:
        all_muns = set(reg_dict.keys())

    for mun in sorted(all_muns):
        if not mun or mun == "nan":
            continue
        contract = fuzzy_get(contract_dict, mun, 0)
        cardio_count = fuzzy_get(cardio_dict, mun, 0)
        matgo_count = fuzzy_get(matgo_dict, mun, 0)
        stress_count = fuzzy_get(stress_dict, mun, 0)

        # 이용률 계산 (협약인원 대비 %)
        cardio_rate = round(cardio_count / contract * 100, 1) if contract > 0 else 0
        matgo_rate = round(matgo_count / contract * 100, 1) if contract > 0 else 0
        stress_rate = round(stress_count / contract * 100, 1) if contract > 0 else 0

        row = {
            "지자체명": mun,
            "권역": REGION_MAP.get(mun, fuzzy_get(REGION_MAP, mun, "기타")),
            "가입완료율": fuzzy_get(reg_dict, mun, 0),
            "앱삭제율": del_dict.get(mun, 0),
            "심혈관이용률": cardio_rate,
            "스트레스이용률": stress_rate,
            "맞고이용률": matgo_rate,
            "협약인원": contract,
        }

        # 상태 분류 - 가입완료율
        reg_rate = row["가입완료율"]
        if reg_rate >= 90:
            row["가입상태"] = "우수"
        elif reg_rate >= 70:
            row["가입상태"] = "보통"
        elif reg_rate >= 50:
            row["가입상태"] = "주의"
        else:
            row["가입상태"] = "위험"

        # 상태 분류 - 앱삭제율
        del_rate = row["앱삭제율"]
        if del_rate < 3:
            row["삭제상태"] = "우수"
        elif del_rate < 8:
            row["삭제상태"] = "보통"
        elif del_rate < 15:
            row["삭제상태"] = "주의"
        else:
            row["삭제상태"] = "위험"

        # 종합 상태
        statuses = [row["가입상태"], row["삭제상태"]]
        danger = statuses.count("위험")
        caution = statuses.count("주의")
        excellent = statuses.count("우수")
        if danger >= 1:
            row["종합상태"] = "집중관리"
        elif caution >= 2:
            row["종합상태"] = "주의관리"
        elif excellent >= 1:
            row["종합상태"] = "우수사례"
        else:
            row["종합상태"] = "정상"

        rows.append(row)

    return pd.DataFrame(rows)


# ============================================================
# 테스트
# ============================================================
if __name__ == "__main__":
    print("Google Sheets 데이터 가져오기 시작...")
    sheets = fetch_all_sheets()
    print(f"\n총 {len(sheets)}개 시트 로드 완료:")
    for name, df in sheets.items():
        print(f"  {name}: {df.shape[0]}행 × {df.shape[1]}열")

    # 대시보드 데이터 빌드
    data = build_dashboard_data(sheets)
    print(f"\n주차 목록: {data.get('주차목록', [])[:5]}...{data.get('주차목록', [])[-3:]}")

    # 최신 주차 히트맵 테스트
    weeks = data.get("주차목록", [])
    if weeks:
        latest = weeks[-1]
        heatmap = build_municipality_heatmap_data(data, latest)
        print(f"\n{latest}주차 히트맵 데이터:")
        print(heatmap[["지자체명", "가입완료율", "앱삭제율", "종합상태"]].to_string())

    print("\n[OK] 테스트 완료!")


# ============================================================
# Google Sheets 기반 메모 저장 (Streamlit Cloud 영구 저장용)
# ============================================================

def _get_gspread_client():
    """서비스 계정으로 gspread 클라이언트 반환. st.secrets에 gcp_service_account 없으면 None."""
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        import streamlit as st

        if "gcp_service_account" not in st.secrets:
            return None

        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
        ]
        creds = Credentials.from_service_account_info(
            dict(st.secrets["gcp_service_account"]),
            scopes=scopes,
        )
        return gspread.authorize(creds)
    except Exception:
        return None


def get_sheet_note(key: str, default: str = "") -> str:
    """Google Sheet '메모' 탭에서 key에 해당하는 값을 읽어 반환. 실패 시 default 반환."""
    gc = _get_gspread_client()
    if gc is None:
        return default
    try:
        import gspread
        sh = gc.open_by_key(SPREADSHEET_ID)
        try:
            ws = sh.worksheet("메모")
        except gspread.WorksheetNotFound:
            return default
        records = ws.get_all_records()
        for row in records:
            if str(row.get("key", "")) == key:
                return str(row.get("value", default))
        return default
    except Exception:
        return default


def save_sheet_note(key: str, value: str) -> bool:
    """Google Sheet '메모' 탭에 key-value 저장. 탭이 없으면 자동 생성. 성공 시 True 반환."""
    gc = _get_gspread_client()
    if gc is None:
        return False
    try:
        import gspread
        from datetime import datetime
        sh = gc.open_by_key(SPREADSHEET_ID)
        try:
            ws = sh.worksheet("메모")
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title="메모", rows=100, cols=3)
            ws.append_row(["key", "value", "updated_at"])

        records = ws.get_all_records()
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for i, row in enumerate(records, start=2):  # header row=1, data starts=2
            if str(row.get("key", "")) == key:
                ws.update(f"B{i}:C{i}", [[value, now_str]])
                return True
        ws.append_row([key, value, now_str])
        return True
    except Exception:
        return False


# ============================================================
# Google Sheets 기반 세이프 현황 저장 (Streamlit Cloud 영구 저장용)
# ============================================================

_SAFE_STATUS_COLS = [
    "monitoring_start_date", "memo", "agency_name",
    "contract_users", "registered_users", "joined_users",
    "registered_rate", "joined_rate",
]


def get_safe_status_from_sheet() -> "pd.DataFrame":
    """Google Sheet '세이프현황' 탭에서 세이프 대상 지자체 현황을 읽어 DataFrame으로 반환."""
    gc = _get_gspread_client()
    if gc is None:
        return pd.DataFrame()
    try:
        import gspread
        sh = gc.open_by_key(SPREADSHEET_ID)
        try:
            ws = sh.worksheet("세이프현황")
        except gspread.WorksheetNotFound:
            return pd.DataFrame()
        records = ws.get_all_records()
        if not records:
            return pd.DataFrame()
        df = pd.DataFrame(records)
        for col in ["contract_users", "registered_users", "joined_users"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
        for col in ["registered_rate", "joined_rate"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)
        return df
    except Exception:
        return pd.DataFrame()


def save_safe_status_to_sheet(rows: list) -> bool:
    """세이프 현황 행 목록을 Google Sheet '세이프현황' 탭에 저장(전체 덮어쓰기). 성공 시 True."""
    gc = _get_gspread_client()
    if gc is None:
        return False
    try:
        import gspread
        sh = gc.open_by_key(SPREADSHEET_ID)
        try:
            ws = sh.worksheet("세이프현황")
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title="세이프현황", rows=200, cols=len(_SAFE_STATUS_COLS))

        # 헤더 + 데이터 일괄 업데이트
        write_rows = [_SAFE_STATUS_COLS]
        for r in rows:
            write_rows.append([
                r.get("monitoring_start_date", ""),
                r.get("memo", ""),
                r.get("agency_name", ""),
                r.get("contract_users", 0),
                r.get("registered_users", 0),
                r.get("joined_users", 0),
                r.get("registered_rate", 0.0),
                r.get("joined_rate", 0.0),
            ])
        ws.clear()
        ws.update("A1", write_rows)
        return True
    except Exception:
        return False
