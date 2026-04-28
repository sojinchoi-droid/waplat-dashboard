# -*- coding: utf-8 -*-
"""Google Sheets 실데이터 fetcher - 와플랫 공공 지표 대시보드"""
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
import pandas as pd
import requests

SPREADSHEET_ID = "15UZ9dZjYdD24PdWoSvrFWpQCM-T0vhc_yy9wrMunSNc"
BASE_URL = f"https://docs.google.com/spreadsheets/d/{SPREADSHEET_ID}/gviz/tq?tqx=out:csv&gid="

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
    "안부확인raw":      "1323180805",
    "안부체크off":      "1043653372",
    "건강상담지자체":   "867975933",
}

# 지자체 키워드 (컬럼명에서 지자체 자동 탐지용)
MUNICIPALITY_KEYWORDS = [
    "경기도청", "용인시청", "서초구청", "청주시청", "진천군청", "음성군청",
    "강북구청", "금정구청", "괴산군청", "증평군청", "포천시청", "마포구청",
    "광진구청", "경남사회서비스원", "강릉시청", "강원사회서비스원",
    "충북사회서비스원", "독거노인", "희망나래", "홍천군청",
    "충남사회서비스원", "삼척시청",
    "광명시청", "제주시청", "서귀포시청", "양양군청",
    "양평군청", "정선군청",
]

# 이름 별칭 (같은 지자체의 다른 표기)
NAME_ALIASES = {
    "독거노인종합지원센터": "독거노인지원종합센터",
    "독거노인지원종합센터": "독거노인지원종합센터",
    "희망나래장애인복지관": "희망나래",
    "희망나래복지원": "희망나래",
    "충남사회서비스언": "충남사회서비스원",
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

# 수도권 / 비수도권 분류
REGION_MAP = {
    "경기도청": "수도권", "용인시청": "수도권", "서초구청": "수도권",
    "강북구청": "수도권", "포천시청": "수도권", "마포구청": "수도권",
    "광진구청": "수도권",
    "청주시청": "비수도권", "진천군청": "비수도권", "음성군청": "비수도권",
    "금정구청": "비수도권", "괴산군청": "비수도권", "증평군청": "비수도권",
    "강릉시청": "비수도권", "홍천군청": "비수도권", "삼척시청": "비수도권",
    "경남사회서비스원": "비수도권", "강원사회서비스원": "비수도권",
    "충북사회서비스원": "비수도권", "충남사회서비스원": "비수도권",
    "독거노인지원종합센터": "기관", "독거노인종합지원센터": "기관",
    "독거노인": "기관", "희망나래": "기관", "희망나래장애인복지관": "기관",
    "광명시청": "수도권", "제주시청": "비수도권", "서귀포시청": "비수도권",
    "양양군청": "비수도권", "양평군청": "수도권", "정선군청": "비수도권",
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
        # 지자체 키워드 매칭
        for kw in MUNICIPALITY_KEYWORDS:
            if kw in col_clean:
                # 마지막 행(최신 데이터) 사용
                val = df[col].dropna()
                if not val.empty:
                    result[kw] = safe_numeric(val.iloc[-1])
                break

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
    for kw in MUNICIPALITY_KEYWORDS:
        if kw in col_clean:
            return normalize_agency_name(kw)
    # 별칭 직접 체크
    result = normalize_agency_name(col_clean)
    return result


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
        elif cl == "전체가입률" and "전체가입률" not in used_names:
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
        # 서비스 이용률 — 컬럼명이 딱 "서비스"
        elif cl == "서비스" and "서비스이용률" not in used:
            rename[c] = "서비스이용률"; used.add("서비스이용률")
        # 프로그램 완료율 — 컬럼명이 "프로그램완료" 또는 "프로그램 완료"
        elif cl == "프로그램완료" and "프로그램완료율" not in used:
            rename[c] = "프로그램완료율"; used.add("프로그램완료율")
        # AI 알림도달률
        elif "도달률" in cl and "AI알림도달률" not in used:
            rename[c] = "AI알림도달률"; used.add("AI알림도달률")
    if rename:
        df = df.rename(columns=rename)
    for col in ["인트로참여율", "서비스이용률", "프로그램완료율", "AI알림도달률", "회원수"]:
        if col in df.columns:
            df[col] = df[col].apply(safe_numeric)
    # 월 컬럼이 없으면 빈 반환
    if "월" not in df.columns:
        return pd.DataFrame()
    return df


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

    시트 구조:
      - 구분: 주차 기간(예: '4월 5일~11일') — 통합 행에만 값, 이하 NaN
      - Unnamed: 1: 지자체명('통합' 또는 '삼척시청'/'양양군청'/'정선군청')
      - 계약인원, 가입인원, 알람요일
      - receiveAlarmCount, receiveAlarmUserCount
      - intro, intro(%): 인트로 수/율
      - service proposal(%): 서비스 제안율
      - program(%): 프로그램 완료율

    반환: 지자체별 행만 포함 (통합 제외), 기간 컬럼 forward-fill
    """
    df = sheets.get("AI생활지원사신규", pd.DataFrame())
    if df.empty:
        return pd.DataFrame()
    df = df.copy()

    # 구분 컬럼 forward-fill (기간명이 통합 행에만 있음)
    period_col = df.columns[0]   # '구분'
    name_col   = df.columns[1]   # 'Unnamed: 1' → 지자체명

    df[period_col] = df[period_col].ffill()

    # 수치 컬럼 정리
    num_cols = ["계약인원", "가입인원", "receiveAlarmCount", "receiveAlarmUserCount",
                "intro", "intro(%)", "service proposal(%)", "program(%)"]

    result_rows = []
    for _, row in df.iterrows():
        name = str(row.get(name_col, "")).strip()
        if name in ("nan", "", "NaN"):
            continue
        period = str(row.get(period_col, "")).strip()
        alarm_day = str(row.get("알람요일", "")).strip()
        alarm_day = "" if alarm_day in ("nan", "NaN") else alarm_day

        r = {"기간": period, "지자체": name, "알람요일": alarm_day}
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
        for kw in MUNICIPALITY_KEYWORDS:
            if kw in cl:
                if "안부체크율1" in cl:
                    # LP~MJ열: 직접 할당(override) — IT~JR보다 우선
                    rate_columns.setdefault(kw, {})
                    rate_columns[kw]["안부체크율_원본"] = c
                elif "안부체크율" in cl:
                    # IT~JR열: LP~MJ가 없을 때만 fallback
                    rate_columns.setdefault(kw, {}).setdefault("안부체크율_원본", c)
                elif "안부체크발송" in cl:
                    rate_columns.setdefault(kw, {})["안부체크발송"] = c
                elif "안부체크응답" in cl:
                    rate_columns.setdefault(kw, {})["안부체크응답"] = c
                elif "안부확인율" in cl:
                    rate_columns.setdefault(kw, {})["안부확인율"] = c
                elif "48미확인율" in cl or "48시간미확인율" in cl:
                    rate_columns.setdefault(kw, {})["48미확인율"] = c
                elif "안부콜응답률" in cl:
                    rate_columns.setdefault(kw, {})["안부콜응답률"] = c
                break

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
            orig = entry.get("안부체크율_원본")
            if orig is not None and orig > 0:
                entry["안부체크율"] = orig
            elif send is not None and resp is not None and send > 0:
                actual_target = send - off
                if actual_target > 0:
                    entry["안부체크율"] = round(resp / actual_target * 100, 1)
                else:
                    entry["안부체크율"] = 0.0
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
        _db_path = __import__('os').path.join(__import__('os').path.dirname(__file__), 'waflat.db')
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

    # 안부확인율: checkin_daily R열 안부미확인률 → 100 - 주차평균 (추이 차트와 완전 동일)
    # ① 일별로 round(100 - 미확인률, 1) → ② ISO 주차별 groupby mean → ③ 시작일 기준 주차 lookup
    cd_raw = data.get("checkin_daily", pd.DataFrame())
    if not cd_raw.empty and "안부미확인률" in cd_raw.columns and "날짜" in cd_raw.columns:
        try:
            from datetime import datetime as _dt
            def _to_wlabel(d):
                s = str(d).strip()
                try:
                    if len(s) >= 10 and s[4:5] == "-":
                        dt = _dt.strptime(s[:10], "%Y-%m-%d")
                        yr, wk, _ = dt.isocalendar()
                        return f"{str(yr)[2:]}-{wk:02d}"
                except Exception:
                    pass
                return s
            cd_tmp = cd_raw[cd_raw["안부미확인률"].apply(safe_numeric) > 0].copy()
            # 추이 차트와 동일: 일별 먼저 round 후 평균
            cd_tmp["_cr"] = (100 - cd_tmp["안부미확인률"].apply(safe_numeric)).round(1)
            cd_tmp["_wk"] = cd_tmp["날짜"].apply(_to_wlabel)
            weekly_avg = cd_tmp.groupby("_wk")["_cr"].mean().round(1)
            # 시작일의 ISO 주차로 lookup (추이 차트 groupby 기준과 동일)
            start_date = summary.get("시작일", "")
            if start_date:
                wlabel = _to_wlabel(str(start_date))
                if wlabel in weekly_avg.index:
                    summary["안부확인율"] = float(weekly_avg[wlabel])
        except Exception:
            pass

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
