# -*- coding: utf-8 -*-
"""통합 데이터 조회 레이어 - SQLite 우선, Google Sheets 보충"""
import pandas as pd
from datetime import datetime

from local_db import get_connection, init_db
from sheets_data import (
    fetch_all_sheets, build_dashboard_data,
    build_municipality_heatmap_data, get_week_summary,
    safe_numeric, REGION_MAP,
)

# DB 초기화
init_db()


def get_db_data(table: str, date_from: str = None, date_to: str = None) -> pd.DataFrame:
    """SQLite에서 데이터 조회"""
    conn = get_connection()
    query = f"SELECT * FROM {table}"
    conditions = []
    params = []
    if date_from:
        conditions.append("date >= ?")
        params.append(date_from)
    if date_to:
        conditions.append("date <= ?")
        params.append(date_to)
    if conditions:
        query += " WHERE " + " AND ".join(conditions)
    query += " ORDER BY date, agency_name"
    try:
        df = pd.read_sql_query(query, conn, params=params)
    except Exception:
        df = pd.DataFrame()
    conn.close()
    return df


def get_db_date_range(table: str) -> tuple:
    """DB에 저장된 데이터의 날짜 범위 반환"""
    conn = get_connection()
    try:
        row = conn.execute(
            f"SELECT MIN(date), MAX(date), COUNT(*) FROM {table}"
        ).fetchone()
        conn.close()
        return row[0], row[1], row[2]
    except Exception:
        conn.close()
        return None, None, 0


def get_all_db_stats() -> dict:
    """모든 DB 테이블의 현황 반환"""
    tables = [
        "raw_safety_check", "raw_cardiovascular", "raw_dualgo",
        "raw_member", "raw_stress_check", "raw_ai_care",
        "raw_medicine", "raw_generic",
    ]
    stats = {}
    for t in tables:
        min_d, max_d, count = get_db_date_range(t)
        stats[t] = {"min_date": min_d, "max_date": max_d, "count": count}
    return stats


def load_unified_data() -> dict:
    """통합 데이터 로드 - SQLite 우선, Google Sheets 보충

    Returns:
        dict with keys:
        - source: "db", "sheets", "hybrid"
        - sheets_data: Google Sheets 원본 (과거 데이터)
        - db_stats: DB 테이블별 현황
        - dashboard_data: 대시보드용 가공 데이터
    """
    result = {
        "source": "sheets",  # 기본값
        "sheets_data": {},
        "db_stats": {},
        "dashboard_data": {},
    }

    # 1. Google Sheets 데이터 로드 (과거 데이터)
    try:
        sheets = fetch_all_sheets()
        dashboard_data = build_dashboard_data(sheets)
        result["sheets_data"] = sheets
        result["dashboard_data"] = dashboard_data
    except Exception as e:
        print(f"[unified_data] Google Sheets 로드 실패: {e}")
        sheets = {}
        dashboard_data = {}

    # 2. SQLite DB 현황 확인
    db_stats = get_all_db_stats()
    result["db_stats"] = db_stats

    # 3. DB에 데이터가 있으면 하이브리드 모드
    has_db_data = any(s["count"] > 0 for s in db_stats.values())
    if has_db_data:
        result["source"] = "hybrid"

        # DB 데이터로 대시보드 보강
        _enrich_with_db_data(result)

    return result


def _enrich_with_db_data(result: dict):
    """SQLite DB 데이터로 대시보드 데이터 보강"""
    dashboard = result.get("dashboard_data", {})

    # 안부확인 DB 데이터 추가
    safety_df = get_db_data("raw_safety_check")
    if not safety_df.empty:
        # 안부체크율 자동 계산
        safety_df["안부체크율"] = (
            safety_df["complete_user_count"] /
            safety_df["target_user_count"].replace(0, float("nan")) * 100
        ).round(1).fillna(0)

        safety_df["48시간미확인률"] = (
            safety_df["uncheck_48hr_user_count"] /
            safety_df["target_user_count"].replace(0, float("nan")) * 100
        ).round(1).fillna(0)

        dashboard["db_safety_check"] = safety_df

    # 심혈관 DB 데이터
    cardio_df = get_db_data("raw_cardiovascular")
    if not cardio_df.empty:
        dashboard["db_cardiovascular"] = cardio_df

    # 맞고 DB 데이터
    dualgo_df = get_db_data("raw_dualgo")
    if not dualgo_df.empty:
        dashboard["db_dualgo"] = dualgo_df

    # 스트레스체크 DB 데이터
    stress_df = get_db_data("raw_stress_check")
    if not stress_df.empty:
        dashboard["db_stress_check"] = stress_df

    # 회원현황 DB 데이터
    member_df = get_db_data("raw_member")
    if not member_df.empty:
        dashboard["db_member"] = member_df

    # AI케어 DB 데이터
    ai_df = get_db_data("raw_ai_care")
    if not ai_df.empty:
        dashboard["db_ai_care"] = ai_df

    # 복약관리 DB 데이터
    med_df = get_db_data("raw_medicine")
    if not med_df.empty:
        dashboard["db_medicine"] = med_df

    # 건강상담/생활상담 (generic 테이블)
    generic_df = get_db_data("raw_generic")
    if not generic_df.empty:
        for dtype in generic_df["data_type"].unique():
            sub = generic_df[generic_df["data_type"] == dtype].copy()
            dashboard[f"db_generic_{dtype}"] = sub

    result["dashboard_data"] = dashboard


# ============================================================
# 지자체 마스터 관리
# ============================================================

def get_agency_master() -> pd.DataFrame:
    """지자체 마스터 데이터 조회"""
    conn = get_connection()
    df = pd.read_sql_query(
        "SELECT * FROM agency_master ORDER BY is_active DESC, agency_name",
        conn
    )
    conn.close()
    return df


def save_agency(agency_name: str, service_model: str,
                contract_start: str, contract_end: str = None,
                target_users: int = 0, memo: str = "",
                agency_seq: int = 0, manager_name: str = "",
                manager_contact: str = "") -> bool:
    """지자체 추가/수정"""
    conn = get_connection()
    try:
        conn.execute("""
        INSERT INTO agency_master (agency_name, agency_seq, service_model, contract_start,
                                   contract_end, target_users, manager_name, manager_contact, memo)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(agency_name, contract_start) DO UPDATE SET
            agency_seq = excluded.agency_seq,
            service_model = excluded.service_model,
            contract_end = excluded.contract_end,
            target_users = excluded.target_users,
            manager_name = excluded.manager_name,
            manager_contact = excluded.manager_contact,
            memo = excluded.memo,
            updated_at = datetime('now', 'localtime')
        """, (agency_name, agency_seq, service_model, contract_start,
              contract_end, target_users, manager_name, manager_contact, memo))
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        print(f"[unified_data] save_agency error: {e}")
        conn.close()
        return False


def toggle_agency_active(agency_id: int, is_active: bool) -> bool:
    """지자체 활성/비활성 전환"""
    conn = get_connection()
    try:
        conn.execute(
            "UPDATE agency_master SET is_active = ?, updated_at = datetime('now', 'localtime') WHERE id = ?",
            (1 if is_active else 0, agency_id)
        )
        conn.commit()
        conn.close()
        return True
    except Exception:
        conn.close()
        return False


def get_agency_summary() -> dict:
    """지자체 현황 요약 - 현재 날짜 기준 (local_db로 위임)"""
    from local_db import get_agency_summary as _local_summary
    return _local_summary()


def seed_agencies_from_sheets(sheets: dict) -> int:
    """Google Sheets 이용자현황에서 지자체 마스터 초기 데이터 생성
    이미 등록된 지자체는 건너뜀 (덮어쓰지 않음)
    Returns: 새로 등록된 건수
    """
    from sheets_data import get_registration_status, safe_numeric
    reg = get_registration_status(sheets)
    if reg.empty:
        return 0

    # 기존 등록된 지자체 확인
    existing = get_agency_master()
    existing_names = set(existing["agency_name"].tolist()) if not existing.empty else set()

    def _already_exists(name):
        """정확 매칭 또는 부분 매칭으로 이미 등록 여부 확인"""
        if name in existing_names:
            return True
        for en in existing_names:
            if name in en or en in name:
                return True
        return False

    count = 0
    for _, row in reg.iterrows():
        name = str(row.get("지자체명", "")).strip()
        if not name or name == "nan" or _already_exists(name):
            continue

        target = int(safe_numeric(row.get("협약인원", 0)))

        # 기본값: safe 모델, 계약시작 2025-01-01
        save_agency(
            agency_name=name,
            service_model="safe",
            contract_start="2025-01-01",
            contract_end="",
            target_users=target,
            memo="Google Sheets에서 자동 등록",
        )
        count += 1

    return count


def get_active_agencies() -> list:
    """현재 활성(계약 중) 지자체 목록 반환"""
    from datetime import date
    today = date.today().isoformat()
    conn = get_connection()
    try:
        rows = conn.execute("""
        SELECT agency_name FROM agency_master
        WHERE is_active = 1
          AND (contract_end IS NULL OR contract_end = '' OR contract_end >= ?)
        ORDER BY agency_name
        """, (today,)).fetchall()
        conn.close()
        return [r[0] for r in rows]
    except Exception:
        conn.close()
        return []


def import_safety_check_from_sheets(sheets: dict) -> int:
    """Google Sheets의 안부확인 raw CSV 데이터를 DB에 자동 임포트
    gid=1323180805 → raw_safety_check 테이블
    Returns: 임포트된 건수
    """
    df = sheets.get("안부확인raw", pd.DataFrame())
    if df.empty:
        return 0

    conn = get_connection()
    count = 0

    # 컬럼 매핑 (Google Sheets 컬럼명 → DB 컬럼명)
    col_map = {}
    for c in df.columns:
        cl = str(c).replace("\n", "").replace(" ", "").strip().lower()
        if "date" in cl and "date" not in col_map:
            col_map["date"] = c
        elif "agencyname" in cl and "agency_name" not in col_map:
            col_map["agency_name"] = c
        elif "safetycheckalarm" in cl and "send" in cl and "alarm_send" not in col_map:
            col_map["alarm_send"] = c
        elif "safetycheckconfirm" in cl and "confirm" not in col_map:
            col_map["confirm"] = c
        elif "safetychecktargetuser" in cl and "target" not in col_map:
            col_map["target"] = c
        elif "safetycheckcompleteuser" in cl and "complete" not in col_map:
            col_map["complete"] = c
        elif "safetycheckimpossibleuser" in cl and "impossible" not in col_map:
            col_map["impossible"] = c
        elif "detectmotion" in cl and "detect" not in col_map:
            col_map["detect"] = c
        elif "aicarealarmtarget" in cl and "ai_target" not in col_map:
            col_map["ai_target"] = c
        elif "aicarealarmgenerate" in cl and "ai_generate" not in col_map:
            col_map["ai_generate"] = c
        elif "aicarealarmresponse" in cl and "ai_response" not in col_map:
            col_map["ai_response"] = c
        elif "safetycheckcallgenerate" in cl and "call_generate" not in col_map:
            col_map["call_generate"] = c
        elif "safetycheckcallresponse" in cl and "call_response" not in col_map:
            col_map["call_response"] = c
        elif "safetyuncheck48hruser" in cl and "target" not in cl and "alarm" not in cl and "receive" not in cl and "uncheck_48hr" not in col_map:
            col_map["uncheck_48hr"] = c
        elif "safetyuncheck48hrtarget" in cl and "uncheck_48hr_target" not in col_map:
            col_map["uncheck_48hr_target"] = c
        elif "safetyuncheck48hralarmgenerate" in cl and "uncheck_48hr_alarm" not in col_map:
            col_map["uncheck_48hr_alarm"] = c
        elif "safetyuncheck48hralarmreceive" in cl and "uncheck_48hr_receive" not in col_map:
            col_map["uncheck_48hr_receive"] = c

    if "date" not in col_map or "agency_name" not in col_map:
        return 0

    def _safe_int(val):
        try:
            if pd.isna(val) or str(val).strip() == "":
                return 0
            return int(float(str(val).replace(",", "").strip()))
        except:
            return 0

    for _, row in df.iterrows():
        date_val = str(row.get(col_map.get("date", ""), "")).strip()
        agency = str(row.get(col_map.get("agency_name", ""), "")).strip()
        if not date_val or date_val == "nan" or not agency or agency == "nan":
            continue

        try:
            conn.execute("""
            INSERT INTO raw_safety_check (
                date, agency_seq, agency_name,
                alarm_send_count, confirm_count, target_user_count,
                complete_user_count, impossible_user_count, detect_motion_count,
                ai_care_target_count, ai_care_generate_count, ai_care_response_count,
                call_generate_count, call_response_count,
                uncheck_48hr_user_count, uncheck_48hr_target_count,
                uncheck_48hr_alarm_generate_count, uncheck_48hr_alarm_receive_count
            ) VALUES (?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(date, agency_name) DO UPDATE SET
                alarm_send_count = excluded.alarm_send_count,
                confirm_count = excluded.confirm_count,
                target_user_count = excluded.target_user_count,
                complete_user_count = excluded.complete_user_count,
                impossible_user_count = excluded.impossible_user_count,
                detect_motion_count = excluded.detect_motion_count,
                ai_care_target_count = excluded.ai_care_target_count,
                ai_care_generate_count = excluded.ai_care_generate_count,
                ai_care_response_count = excluded.ai_care_response_count,
                call_generate_count = excluded.call_generate_count,
                call_response_count = excluded.call_response_count,
                uncheck_48hr_user_count = excluded.uncheck_48hr_user_count,
                uncheck_48hr_target_count = excluded.uncheck_48hr_target_count,
                uncheck_48hr_alarm_generate_count = excluded.uncheck_48hr_alarm_generate_count,
                uncheck_48hr_alarm_receive_count = excluded.uncheck_48hr_alarm_receive_count,
                imported_at = datetime('now', 'localtime')
            """, (
                date_val, agency,
                _safe_int(row.get(col_map.get("alarm_send", ""), 0)),
                _safe_int(row.get(col_map.get("confirm", ""), 0)),
                _safe_int(row.get(col_map.get("target", ""), 0)),
                _safe_int(row.get(col_map.get("complete", ""), 0)),
                _safe_int(row.get(col_map.get("impossible", ""), 0)),
                _safe_int(row.get(col_map.get("detect", ""), 0)),
                _safe_int(row.get(col_map.get("ai_target", ""), 0)),
                _safe_int(row.get(col_map.get("ai_generate", ""), 0)),
                _safe_int(row.get(col_map.get("ai_response", ""), 0)),
                _safe_int(row.get(col_map.get("call_generate", ""), 0)),
                _safe_int(row.get(col_map.get("call_response", ""), 0)),
                _safe_int(row.get(col_map.get("uncheck_48hr", ""), 0)),
                _safe_int(row.get(col_map.get("uncheck_48hr_target", ""), 0)),
                _safe_int(row.get(col_map.get("uncheck_48hr_alarm", ""), 0)),
                _safe_int(row.get(col_map.get("uncheck_48hr_receive", ""), 0)),
            ))
            count += 1
        except Exception as e:
            pass

    conn.commit()
    conn.close()
    return count


def get_data_source_info() -> str:
    """현재 데이터 소스 상태를 사람이 읽을 수 있는 텍스트로 반환"""
    stats = get_all_db_stats()
    has_db = any(s["count"] > 0 for s in stats.values())

    lines = []
    if has_db:
        lines.append("SQLite DB + Google Sheets (하이브리드)")
        for table, s in stats.items():
            if s["count"] > 0:
                name = table.replace("raw_", "").replace("_", " ").title()
                lines.append(f"  {name}: {s['count']}건 ({s['min_date']} ~ {s['max_date']})")
    else:
        lines.append("Google Sheets (과거 데이터만)")

    return "\n".join(lines)
