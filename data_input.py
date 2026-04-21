# -*- coding: utf-8 -*-
"""데이터 붙여넣기 파서 - Postman TSV 데이터를 자동 파싱"""
import io
import pandas as pd

# ============================================================
# 데이터 타입별 컬럼 매핑 설정
# ============================================================

DATA_TYPES = {
    "안부확인 (safetyCheck)": {
        "table": "raw_safety_check",
        "description": "safetyCheckExtractIndicator > safetyCheckIndicatorByDateList",
        "column_map": {
            "date": "date",
            "agencySeq": "agency_seq",
            "agencyName": "agency_name",
            "safetyCheckAlarmSendCount": "alarm_send_count",
            "safetyCheckConfirmCount": "confirm_count",
            "safetyCheckTargetUserCount": "target_user_count",
            "safetyCheckCompleteUserCount": "complete_user_count",
            "safetyCheckImpossibleUserCount": "impossible_user_count",
            "detectMotionOrUseServiceUserCount": "detect_motion_count",
            "aiCareAlarmTargetUserCount": "ai_care_target_count",
            "aiCareAlarmGenerateUserCount": "ai_care_generate_count",
            "aiCareAlarmResponseUserCount": "ai_care_response_count",
            "safetyCheckCallGenerateUserCount": "call_generate_count",
            "safetyCheckCallResponseUserCount": "call_response_count",
            "safetyUncheck48hrUserCount": "uncheck_48hr_user_count",
            "safetyUncheck48hrTargetUserCount": "uncheck_48hr_target_count",
            "safetyUncheck48hrAlarmGenerateCount": "uncheck_48hr_alarm_generate_count",
            "safetyUncheck48hrAlarmReceiveUserCount": "uncheck_48hr_alarm_receive_count",
        },
        "computed_columns": {
            "안부체크율(%)": lambda df: (df["complete_user_count"] / df["target_user_count"].replace(0, float("nan")) * 100).round(1).fillna(0),
            "48h미확인률(%)": lambda df: (df["uncheck_48hr_user_count"] / df["target_user_count"].replace(0, float("nan")) * 100).round(1).fillna(0),
            "AI케어응답률(%)": lambda df: (df["ai_care_response_count"] / df["ai_care_generate_count"].replace(0, float("nan")) * 100).round(1).fillna(0),
        },
        "preview_columns": ["date", "agency_name", "target_user_count", "complete_user_count", "안부체크율(%)", "uncheck_48hr_user_count", "48h미확인률(%)"],
    },
    "심혈관체크 (cardiovascularCheck)": {
        "table": "raw_cardiovascular",
        "description": "cardiovascularCheckExtractIndicatorList",
        "column_map": {
            "date": "date",
            "agencySeq": "agency_seq",
            "agencyName": "agency_name",
        },
        "auto_detect": True,  # 나머지 컬럼은 자동 매핑
        "preview_columns": None,
    },
    "맞고 (dualgo)": {
        "table": "raw_dualgo",
        "description": "dualgoExtractIndicatorList",
        "column_map": {
            "date": "date",
            "agencySeq": "agency_seq",
            "agencyName": "agency_name",
        },
        "auto_detect": True,
        "preview_columns": None,
    },
    "회원현황 (member)": {
        "table": "raw_member",
        "description": "memberExtractIndicatorList",
        "column_map": {
            "date": "date",
            "agencySeq": "agency_seq",
            "agencyName": "agency_name",
        },
        "auto_detect": True,
        "preview_columns": None,
    },
    "스트레스체크 (stressCheck)": {
        "table": "raw_stress_check",
        "description": "stressCheckExtractIndicatorList",
        "column_map": {
            "date": "date",
            "agencySeq": "agency_seq",
            "agencyName": "agency_name",
        },
        "auto_detect": True,
        "preview_columns": None,
    },
    "AI케어 (aiCareAlarm)": {
        "table": "raw_ai_care",
        "description": "aiCareAlarmExtractIndicatorList",
        "column_map": {
            "date": "date",
            "agencySeq": "agency_seq",
            "agencyName": "agency_name",
        },
        "auto_detect": True,
        "preview_columns": None,
    },
    "복약관리 (medicine)": {
        "table": "raw_medicine",
        "description": "medicineScheduleExtractIndicator",
        "column_map": {
            "date": "date",
            "agencySeq": "agency_seq",
            "agencyName": "agency_name",
        },
        "auto_detect": True,
        "preview_columns": None,
    },
    "건강상담": {
        "table": "raw_generic",
        "description": "건강상담 데이터",
        "column_map": {
            "date": "date",
            "agencySeq": "agency_seq",
            "agencyName": "agency_name",
        },
        "auto_detect": True,
        "preview_columns": None,
    },
    "생활상담": {
        "table": "raw_generic",
        "description": "생활상담 데이터",
        "column_map": {
            "date": "date",
            "agencySeq": "agency_seq",
            "agencyName": "agency_name",
        },
        "auto_detect": True,
        "preview_columns": None,
    },
}


def parse_pasted_data(text: str) -> pd.DataFrame:
    """붙여넣기된 TSV 텍스트를 DataFrame으로 파싱"""
    if not text or not text.strip():
        return pd.DataFrame()

    text = text.strip()

    # 탭 구분 시도
    try:
        df = pd.read_csv(io.StringIO(text), sep="\t")
        if len(df.columns) > 2:
            return _clean_df(df)
    except Exception:
        pass

    # 쉼표 구분 시도
    try:
        df = pd.read_csv(io.StringIO(text), sep=",")
        if len(df.columns) > 2:
            return _clean_df(df)
    except Exception:
        pass

    # 공백 구분 시도
    try:
        df = pd.read_csv(io.StringIO(text), sep=r"\s+", engine="python")
        if len(df.columns) > 2:
            return _clean_df(df)
    except Exception:
        pass

    return pd.DataFrame()


def _clean_df(df: pd.DataFrame) -> pd.DataFrame:
    """DataFrame 정리"""
    # 완전히 빈 행/열 제거
    df = df.dropna(how="all").dropna(axis=1, how="all")
    # 인덱스 컬럼 제거 (Unnamed 컬럼)
    unnamed_cols = [c for c in df.columns if "Unnamed" in str(c)]
    if unnamed_cols:
        df = df.drop(columns=unnamed_cols)
    # 첫 번째 컬럼이 숫자 인덱스이면 제거
    first_col = df.columns[0]
    if str(first_col).isdigit() or first_col == "":
        try:
            df[first_col].astype(int)
            if df[first_col].is_monotonic_increasing:
                df = df.drop(columns=[first_col])
        except (ValueError, TypeError):
            pass
    return df.reset_index(drop=True)


def map_columns(df: pd.DataFrame, data_type: str) -> pd.DataFrame:
    """데이터 타입에 맞게 컬럼명을 매핑"""
    config = DATA_TYPES.get(data_type, {})
    column_map = config.get("column_map", {})

    if not column_map:
        return df

    # 컬럼명 매핑
    rename = {}
    for orig_col in df.columns:
        col_clean = str(orig_col).strip()
        if col_clean in column_map:
            rename[orig_col] = column_map[col_clean]

    if rename:
        df = df.rename(columns=rename)

    # 숫자 컬럼 변환
    for col in df.columns:
        if col not in ["date", "agency_name", "agency_seq"]:
            try:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)
            except (ValueError, TypeError):
                pass

    # agency_seq 정수화
    if "agency_seq" in df.columns:
        df["agency_seq"] = pd.to_numeric(df["agency_seq"], errors="coerce").fillna(0).astype(int)

    return df


def add_computed_columns(df: pd.DataFrame, data_type: str) -> pd.DataFrame:
    """자동 계산 컬럼 추가"""
    config = DATA_TYPES.get(data_type, {})
    computed = config.get("computed_columns", {})

    for col_name, formula in computed.items():
        try:
            df[col_name] = formula(df)
        except Exception:
            df[col_name] = 0

    return df


def get_preview_columns(data_type: str) -> list:
    """미리보기에 표시할 컬럼 목록"""
    config = DATA_TYPES.get(data_type, {})
    return config.get("preview_columns", None)


def detect_data_type(df: pd.DataFrame) -> str:
    """DataFrame 컬럼명으로 데이터 타입 자동 감지"""
    cols = set(str(c).strip() for c in df.columns)

    if "safetyCheckAlarmSendCount" in cols or "safetyCheckTargetUserCount" in cols:
        return "안부확인 (safetyCheck)"
    elif any("cardiovascular" in c.lower() for c in cols):
        return "심혈관체크 (cardiovascularCheck)"
    elif any("dualgo" in c.lower() for c in cols):
        return "맞고 (dualgo)"
    elif any("stress" in c.lower() for c in cols):
        return "스트레스체크 (stressCheck)"
    elif any("aicare" in c.lower() or "aiCare" in c for c in cols):
        return "AI케어 (aiCareAlarm)"
    elif any("medicine" in c.lower() or "schedule" in c.lower() for c in cols):
        return "복약관리 (medicine)"
    elif any("member" in c.lower() for c in cols):
        return "회원현황 (member)"

    return ""


def process_pasted_data(text: str, data_type: str = "") -> dict:
    """붙여넣기 데이터를 전체 처리 파이프라인으로 실행

    Returns: {
        "success": bool,
        "df_raw": DataFrame (원본 파싱 결과),
        "df_mapped": DataFrame (컬럼 매핑 후),
        "df_preview": DataFrame (미리보기용),
        "detected_type": str,
        "row_count": int,
        "date_range": str,
        "agencies": list,
        "error": str,
    }
    """
    result = {
        "success": False, "df_raw": pd.DataFrame(),
        "df_mapped": pd.DataFrame(), "df_preview": pd.DataFrame(),
        "detected_type": data_type, "row_count": 0,
        "date_range": "", "agencies": [], "error": "",
    }

    # Step 1: 파싱
    df = parse_pasted_data(text)
    if df.empty:
        result["error"] = "데이터를 파싱할 수 없습니다. 탭으로 구분된 데이터인지 확인해주세요."
        return result

    result["df_raw"] = df
    result["row_count"] = len(df)

    # Step 2: 데이터 타입 감지
    if not data_type:
        data_type = detect_data_type(df)
        result["detected_type"] = data_type

    # Step 2.5: 공통 컬럼 이름 정규화 (모든 데이터 타입 공통)
    common_rename = {
        "agencyName": "agency_name",
        "agencySeq": "agency_seq",
        "orgName": "org_name",
    }
    for orig, new in common_rename.items():
        if orig in df.columns and new not in df.columns:
            df = df.rename(columns={orig: new})

    # 인덱스 컬럼(첫 열이 0,1,2... 숫자) 제거
    first_col = df.columns[0]
    if str(first_col).strip() == "" or str(first_col).strip().isdigit():
        try:
            vals = pd.to_numeric(df[first_col], errors="coerce")
            if vals.notna().all() and vals.is_monotonic_increasing:
                df = df.drop(columns=[first_col])
        except:
            pass

    result["df_raw"] = df

    # Step 3: 컬럼 매핑
    if data_type:
        df_mapped = map_columns(df.copy(), data_type)
        df_mapped = add_computed_columns(df_mapped, data_type)
        result["df_mapped"] = df_mapped

        # 날짜 범위
        if "date" in df_mapped.columns:
            dates = df_mapped["date"].dropna().unique()
            if len(dates) > 0:
                result["date_range"] = f"{min(dates)} ~ {max(dates)}"

        # 지자체 목록
        if "agency_name" in df_mapped.columns:
            result["agencies"] = df_mapped["agency_name"].dropna().unique().tolist()

        # 미리보기
        preview_cols = get_preview_columns(data_type)
        if preview_cols:
            available_cols = [c for c in preview_cols if c in df_mapped.columns]
            result["df_preview"] = df_mapped[available_cols] if available_cols else df_mapped
        else:
            result["df_preview"] = df_mapped
    else:
        result["df_mapped"] = df
        result["df_preview"] = df

    result["success"] = True
    return result
