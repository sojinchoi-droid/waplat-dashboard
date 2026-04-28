# -*- coding: utf-8 -*-
"""SQLite 로컬 DB 관리 - raw 데이터 축적 및 자동 계산"""
import sqlite3
import os
import pandas as pd
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(__file__), "waflat.db")


def get_connection():
    """SQLite DB 연결 반환"""
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    """DB 테이블 초기화 (없으면 생성)"""
    conn = get_connection()
    c = conn.cursor()

    # 0. 지자체 마스터 (계약 정보 + 서비스 모델)
    c.execute("""
    CREATE TABLE IF NOT EXISTS agency_master (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        agency_name TEXT NOT NULL,
        agency_seq INTEGER,
        service_model TEXT NOT NULL DEFAULT 'safe',
        contract_start TEXT NOT NULL,
        contract_end TEXT,
        target_users INTEGER DEFAULT 0,
        manager_name TEXT DEFAULT '',
        manager_contact TEXT DEFAULT '',
        memo TEXT DEFAULT '',
        is_active INTEGER DEFAULT 1,
        created_at TEXT DEFAULT (datetime('now', 'localtime')),
        updated_at TEXT DEFAULT (datetime('now', 'localtime')),
        UNIQUE(agency_name, contract_start)
    )
    """)

    # 1. 안부확인 (safetyCheck)
    c.execute("""
    CREATE TABLE IF NOT EXISTS raw_safety_check (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        imported_at TEXT DEFAULT (datetime('now', 'localtime')),
        date TEXT NOT NULL,
        agency_seq INTEGER,
        agency_name TEXT NOT NULL,
        alarm_send_count INTEGER DEFAULT 0,
        confirm_count INTEGER DEFAULT 0,
        target_user_count INTEGER DEFAULT 0,
        complete_user_count INTEGER DEFAULT 0,
        impossible_user_count INTEGER DEFAULT 0,
        detect_motion_count INTEGER DEFAULT 0,
        ai_care_target_count INTEGER DEFAULT 0,
        ai_care_generate_count INTEGER DEFAULT 0,
        ai_care_response_count INTEGER DEFAULT 0,
        call_generate_count INTEGER DEFAULT 0,
        call_response_count INTEGER DEFAULT 0,
        uncheck_48hr_user_count INTEGER DEFAULT 0,
        uncheck_48hr_target_count INTEGER DEFAULT 0,
        uncheck_48hr_alarm_generate_count INTEGER DEFAULT 0,
        uncheck_48hr_alarm_receive_count INTEGER DEFAULT 0,
        UNIQUE(date, agency_seq)
    )
    """)

    # 2. 심혈관체크 (cardiovascularCheck)
    c.execute("""
    CREATE TABLE IF NOT EXISTS raw_cardiovascular (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        imported_at TEXT DEFAULT (datetime('now', 'localtime')),
        date TEXT NOT NULL,
        agency_seq INTEGER,
        agency_name TEXT NOT NULL,
        user_count INTEGER DEFAULT 0,
        check_count INTEGER DEFAULT 0,
        avg_per_user REAL DEFAULT 0,
        UNIQUE(date, agency_seq)
    )
    """)

    # 3. 맞고 (dualgo)
    c.execute("""
    CREATE TABLE IF NOT EXISTS raw_dualgo (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        imported_at TEXT DEFAULT (datetime('now', 'localtime')),
        date TEXT NOT NULL,
        agency_seq INTEGER,
        agency_name TEXT NOT NULL,
        user_count INTEGER DEFAULT 0,
        play_count INTEGER DEFAULT 0,
        registration_count INTEGER DEFAULT 0,
        UNIQUE(date, agency_seq)
    )
    """)

    # 4. 회원현황 (member)
    c.execute("""
    CREATE TABLE IF NOT EXISTS raw_member (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        imported_at TEXT DEFAULT (datetime('now', 'localtime')),
        date TEXT NOT NULL,
        agency_seq INTEGER,
        agency_name TEXT NOT NULL,
        total_member INTEGER DEFAULT 0,
        new_member INTEGER DEFAULT 0,
        active_user INTEGER DEFAULT 0,
        churned_user INTEGER DEFAULT 0,
        app_delete_count INTEGER DEFAULT 0,
        UNIQUE(date, agency_seq)
    )
    """)

    # 5. 스트레스체크 (stressCheck)
    c.execute("""
    CREATE TABLE IF NOT EXISTS raw_stress_check (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        imported_at TEXT DEFAULT (datetime('now', 'localtime')),
        date TEXT NOT NULL,
        agency_seq INTEGER,
        agency_name TEXT NOT NULL,
        user_count INTEGER DEFAULT 0,
        check_count INTEGER DEFAULT 0,
        UNIQUE(date, agency_seq)
    )
    """)

    # 6. AI케어 (aiCareAlarm)
    c.execute("""
    CREATE TABLE IF NOT EXISTS raw_ai_care (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        imported_at TEXT DEFAULT (datetime('now', 'localtime')),
        date TEXT NOT NULL,
        agency_seq INTEGER,
        agency_name TEXT NOT NULL,
        alarm_target_count INTEGER DEFAULT 0,
        alarm_generate_count INTEGER DEFAULT 0,
        alarm_response_count INTEGER DEFAULT 0,
        UNIQUE(date, agency_seq)
    )
    """)

    # 7. 복약관리 (medicine)
    c.execute("""
    CREATE TABLE IF NOT EXISTS raw_medicine (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        imported_at TEXT DEFAULT (datetime('now', 'localtime')),
        date TEXT NOT NULL,
        agency_seq INTEGER,
        agency_name TEXT NOT NULL,
        schedule_count INTEGER DEFAULT 0,
        complete_count INTEGER DEFAULT 0,
        UNIQUE(date, agency_seq)
    )
    """)

    # 8. 범용 테이블 (알 수 없는 데이터 타입용)
    c.execute("""
    CREATE TABLE IF NOT EXISTS raw_generic (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        imported_at TEXT DEFAULT (datetime('now', 'localtime')),
        data_type TEXT NOT NULL,
        date TEXT,
        date_end TEXT,
        agency_seq INTEGER,
        agency_name TEXT,
        raw_json TEXT,
        UNIQUE(data_type, date, agency_name)
    )
    """)

    # 9. 세이프 대상 지자체 현황 (주간 업데이트)
    c.execute("""
    CREATE TABLE IF NOT EXISTS safe_agency_status (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        updated_at TEXT DEFAULT (datetime('now', 'localtime')),
        monitoring_start_date TEXT NOT NULL,
        memo TEXT DEFAULT '',
        agency_name TEXT NOT NULL,
        contract_users INTEGER DEFAULT 0,
        registered_users INTEGER DEFAULT 0,
        joined_users INTEGER DEFAULT 0,
        registered_rate REAL DEFAULT 0,
        joined_rate REAL DEFAULT 0,
        UNIQUE(agency_name)
    )
    """)
    # 마이그레이션: 구버전 DB에 target_users로 생성된 경우 contract_users로 rename
    _cols = [r[1] for r in c.execute("PRAGMA table_info(safe_agency_status)").fetchall()]
    if "target_users" in _cols and "contract_users" not in _cols:
        c.execute("ALTER TABLE safe_agency_status RENAME COLUMN target_users TO contract_users")
    elif "contract_users" not in _cols:
        c.execute("ALTER TABLE safe_agency_status ADD COLUMN contract_users INTEGER DEFAULT 0")

    # 10. 주간 지표 통합 테이블 (Google Sheets → DB 임포트용)
    c.execute("""
    CREATE TABLE IF NOT EXISTS dashboard_notes (
        key TEXT PRIMARY KEY,
        value TEXT DEFAULT '',
        updated_at TEXT DEFAULT (datetime('now', 'localtime'))
    )
    """)

    c.execute("""
    CREATE TABLE IF NOT EXISTS raw_weekly_indicator (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        imported_at TEXT DEFAULT (datetime('now', 'localtime')),
        sheet_name TEXT NOT NULL,
        week_code TEXT NOT NULL,
        start_date TEXT,
        agency_name TEXT DEFAULT 'ALL',
        metric_name TEXT NOT NULL,
        value REAL DEFAULT 0,
        UNIQUE(sheet_name, week_code, agency_name, metric_name)
    )
    """)

    conn.commit()
    conn.close()


def import_sheets_to_db(sheets_data: dict, dashboard_data: dict) -> dict:
    """Google Sheets의 모든 주차별 데이터를 DB로 임포트

    Args:
        sheets_data: fetch_all_sheets() 결과
        dashboard_data: build_dashboard_data() 결과

    Returns: {"imported": N, "sheets": [...]}
    """
    conn = get_connection()
    result = {"imported": 0, "sheets": []}

    # 주차 → 시작일 매핑
    wu = dashboard_data.get("weekly_users", pd.DataFrame())
    week_to_date = {}
    if not wu.empty and "주차" in wu.columns and "시작일" in wu.columns:
        for _, row in wu.iterrows():
            w = str(row.get("주차", "")).strip()
            d = str(row.get("시작일", "")).strip()
            if w and w != "nan" and d and d != "nan":
                week_to_date[w] = d

    # weekly_* 데이터 임포트
    weekly_keys = [k for k in dashboard_data.keys() if k.startswith("weekly_")]

    for key in weekly_keys:
        df = dashboard_data.get(key, pd.DataFrame())
        if df.empty or "주차" not in df.columns or "값" not in df.columns:
            continue

        sheet_label = key.replace("weekly_", "")
        count = 0

        for _, row in df.iterrows():
            week = str(row.get("주차", "")).strip()
            if not week or week == "nan":
                continue
            start_date = week_to_date.get(week, "")
            agency = str(row.get("지자체명", "ALL")).strip()
            value = float(row.get("값", 0))

            try:
                conn.execute("""
                INSERT INTO raw_weekly_indicator
                    (sheet_name, week_code, start_date, agency_name, metric_name, value)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(sheet_name, week_code, agency_name, metric_name)
                DO UPDATE SET value=excluded.value, start_date=excluded.start_date,
                              imported_at=datetime('now','localtime')
                """, (sheet_label, week, start_date, agency, sheet_label, value))
                count += 1
            except Exception as e:
                pass

        if count > 0:
            result["sheets"].append(f"{sheet_label}: {count}건")
            result["imported"] += count

    # 안부확인 일별 전체 (checkin_daily)
    cd = dashboard_data.get("checkin_daily", pd.DataFrame())
    if not cd.empty and "날짜" in cd.columns:
        count = 0
        for col in cd.columns:
            if col == "날짜":
                continue
            for _, row in cd.iterrows():
                date_val = str(row.get("날짜", "")).strip()
                if not date_val or date_val == "nan":
                    continue
                val = float(row.get(col, 0)) if not pd.isna(row.get(col)) else 0
                try:
                    conn.execute("""
                    INSERT INTO raw_weekly_indicator
                        (sheet_name, week_code, start_date, agency_name, metric_name, value)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(sheet_name, week_code, agency_name, metric_name)
                    DO UPDATE SET value=excluded.value, imported_at=datetime('now','localtime')
                    """, ("안부확인전체", date_val, date_val, "ALL", col, val))
                    count += 1
                except:
                    pass
        if count > 0:
            result["sheets"].append(f"안부확인전체: {count}건")
            result["imported"] += count

    # 지자체별 안부체크율
    cr = dashboard_data.get("checkin_municipality_rate", pd.DataFrame())
    if not cr.empty:
        count = 0
        for _, row in cr.iterrows():
            date_val = str(row.get("시작일", "")).strip()
            agency = str(row.get("지자체명", "")).strip()
            if not date_val or not agency:
                continue
            for metric in ["안부체크율", "안부확인율", "48미확인율", "안부콜응답률"]:
                val = row.get(metric)
                if val is not None and not pd.isna(val):
                    try:
                        conn.execute("""
                        INSERT INTO raw_weekly_indicator
                            (sheet_name, week_code, start_date, agency_name, metric_name, value)
                        VALUES (?, ?, ?, ?, ?, ?)
                        ON CONFLICT(sheet_name, week_code, agency_name, metric_name)
                        DO UPDATE SET value=excluded.value, imported_at=datetime('now','localtime')
                        """, ("안부체크율", date_val, date_val, agency, metric, float(val)))
                        count += 1
                    except:
                        pass
        if count > 0:
            result["sheets"].append(f"안부체크율: {count}건")
            result["imported"] += count

    conn.commit()
    conn.close()
    return result


def get_weekly_indicator(sheet_name: str = None, metric_name: str = None,
                         agency_name: str = None, date_from: str = None, date_to: str = None) -> pd.DataFrame:
    """raw_weekly_indicator에서 조건에 맞는 데이터 조회"""
    conn = get_connection()
    query = "SELECT * FROM raw_weekly_indicator WHERE 1=1"
    params = []
    if sheet_name:
        query += " AND sheet_name = ?"
        params.append(sheet_name)
    if metric_name:
        query += " AND metric_name = ?"
        params.append(metric_name)
    if agency_name:
        query += " AND agency_name = ?"
        params.append(agency_name)
    if date_from:
        query += " AND start_date >= ?"
        params.append(date_from)
    if date_to:
        query += " AND start_date <= ?"
        params.append(date_to)
    query += " ORDER BY start_date, agency_name"
    df = pd.read_sql_query(query, conn, params=params)
    conn.close()
    return df


def save_safety_check(df: pd.DataFrame) -> dict:
    """안부확인 데이터 저장. 반환: {inserted: N, updated: N, skipped: N}"""
    conn = get_connection()
    result = {"inserted": 0, "updated": 0, "skipped": 0, "errors": []}

    for _, row in df.iterrows():
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
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(date, agency_seq) DO UPDATE SET
                agency_name = excluded.agency_name,
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
                str(row.get("date", "")),
                int(row.get("agency_seq", 0)),
                str(row.get("agency_name", "")),
                int(row.get("alarm_send_count", 0)),
                int(row.get("confirm_count", 0)),
                int(row.get("target_user_count", 0)),
                int(row.get("complete_user_count", 0)),
                int(row.get("impossible_user_count", 0)),
                int(row.get("detect_motion_count", 0)),
                int(row.get("ai_care_target_count", 0)),
                int(row.get("ai_care_generate_count", 0)),
                int(row.get("ai_care_response_count", 0)),
                int(row.get("call_generate_count", 0)),
                int(row.get("call_response_count", 0)),
                int(row.get("uncheck_48hr_user_count", 0)),
                int(row.get("uncheck_48hr_target_count", 0)),
                int(row.get("uncheck_48hr_alarm_generate_count", 0)),
                int(row.get("uncheck_48hr_alarm_receive_count", 0)),
            ))
            result["inserted"] += 1
        except Exception as e:
            result["errors"].append(f"{row.get('agency_name', '?')}: {e}")
            result["skipped"] += 1

    conn.commit()
    conn.close()
    return result


def save_generic(data_type: str, df: pd.DataFrame) -> dict:
    """범용 데이터 저장 (컬럼 매핑 없이 raw 저장)"""
    conn = get_connection()
    result = {"inserted": 0, "skipped": 0, "errors": []}

    for _, row in df.iterrows():
        try:
            # 날짜, 지자체코드, 지자체명 컬럼 탐색
            date = ""
            agency_seq = 0
            agency_name = ""

            if "date" in row.index:
                date = str(row["date"])
            if "agency_seq" in row.index:
                try:
                    agency_seq = int(float(row["agency_seq"]))
                except (ValueError, TypeError):
                    agency_seq = 0
            if "agency_name" in row.index:
                agency_name = str(row["agency_name"])
            elif "agencyName" in row.index:
                agency_name = str(row["agencyName"])

            date_end = ""
            if "date_end" in row.index:
                date_end = str(row["date_end"])

            raw_json = row.to_json(force_ascii=False)

            conn.execute("""
            INSERT INTO raw_generic (data_type, date, date_end, agency_seq, agency_name, raw_json)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(data_type, date, agency_name) DO UPDATE SET
                date_end = excluded.date_end,
                agency_seq = excluded.agency_seq,
                raw_json = excluded.raw_json,
                imported_at = datetime('now', 'localtime')
            """, (data_type, date, date_end, agency_seq, agency_name, raw_json))
            result["inserted"] += 1
        except Exception as e:
            result["errors"].append(str(e))
            result["skipped"] += 1

    conn.commit()
    conn.close()
    return result


# ============================================================
# 데이터 조회 함수
# ============================================================

def get_safety_check_data(date_from=None, date_to=None) -> pd.DataFrame:
    """안부확인 데이터 조회 + 자동 계산 컬럼 포함"""
    conn = get_connection()
    query = "SELECT * FROM raw_safety_check WHERE 1=1"
    params = []
    if date_from:
        query += " AND date >= ?"
        params.append(date_from)
    if date_to:
        query += " AND date <= ?"
        params.append(date_to)
    query += " ORDER BY date DESC, agency_name"

    df = pd.read_sql_query(query, conn, params=params)
    conn.close()

    if not df.empty:
        # 자동 계산
        df["안부체크율"] = (df["complete_user_count"] / df["target_user_count"].replace(0, float("nan")) * 100).round(1).fillna(0)
        df["48시간미확인률"] = (df["uncheck_48hr_user_count"] / df["target_user_count"].replace(0, float("nan")) * 100).round(1).fillna(0)
        df["AI케어응답률"] = (df["ai_care_response_count"] / df["ai_care_generate_count"].replace(0, float("nan")) * 100).round(1).fillna(0)
        df["통화응답률"] = (df["call_response_count"] / df["call_generate_count"].replace(0, float("nan")) * 100).round(1).fillna(0)

    return df


def get_all_dates() -> list:
    """저장된 모든 날짜 목록"""
    conn = get_connection()
    dates = pd.read_sql_query(
        "SELECT DISTINCT date FROM raw_safety_check ORDER BY date DESC", conn
    )
    conn.close()
    return dates["date"].tolist() if not dates.empty else []


def get_data_stats() -> dict:
    """DB 통계 요약"""
    conn = get_connection()
    stats = {}
    for table in ["raw_safety_check", "raw_cardiovascular", "raw_dualgo",
                   "raw_member", "raw_stress_check", "raw_ai_care",
                   "raw_medicine", "raw_generic"]:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            stats[table] = count
        except:
            stats[table] = 0
    conn.close()
    return stats


# ============================================================
# 지자체 마스터 관리
# ============================================================

def save_agency(agency_name: str, service_model: str, contract_start: str,
                contract_end: str = "", target_users: int = 0,
                agency_seq: int = 0, manager_name: str = "",
                manager_contact: str = "", memo: str = "") -> bool:
    """지자체 등록/수정"""
    conn = get_connection()
    try:
        conn.execute("""
        INSERT INTO agency_master (
            agency_name, agency_seq, service_model, contract_start, contract_end,
            target_users, manager_name, manager_contact, memo, is_active
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
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
        conn.close()
        raise e


def deactivate_agency(agency_name: str, contract_start: str) -> bool:
    """지자체 비활성화 (계약 종료)"""
    conn = get_connection()
    conn.execute("""
    UPDATE agency_master SET is_active = 0, updated_at = datetime('now', 'localtime')
    WHERE agency_name = ? AND contract_start = ?
    """, (agency_name, contract_start))
    conn.commit()
    conn.close()
    return True


def activate_agency(agency_name: str, contract_start: str) -> bool:
    """지자체 다시 활성화"""
    conn = get_connection()
    conn.execute("""
    UPDATE agency_master SET is_active = 1, updated_at = datetime('now', 'localtime')
    WHERE agency_name = ? AND contract_start = ?
    """, (agency_name, contract_start))
    conn.commit()
    conn.close()
    return True


def delete_agency(agency_name: str, contract_start: str) -> bool:
    """지자체 완전 삭제"""
    conn = get_connection()
    conn.execute("DELETE FROM agency_master WHERE agency_name = ? AND contract_start = ?",
                 (agency_name, contract_start))
    conn.commit()
    conn.close()
    return True


def get_all_agencies(include_inactive=False) -> pd.DataFrame:
    """전체 지자체 목록 조회"""
    conn = get_connection()
    if include_inactive:
        df = pd.read_sql_query(
            "SELECT * FROM agency_master ORDER BY is_active DESC, agency_name", conn)
    else:
        df = pd.read_sql_query(
            "SELECT * FROM agency_master WHERE is_active = 1 ORDER BY agency_name", conn)
    conn.close()
    return df


def get_agency_summary(sheets=None) -> dict:
    """지자체 현황 요약
    - 활성 지자체 = 이용자현황 시트(gid=33599894)의 지자체 수
    - 세이프 = safe_agency_status 테이블의 지자체 수
    - 베이직 = 활성 - 세이프
    """
    import pandas as pd
    summary = {}

    # 1. 활성 지자체 수: 이용자현황 시트에서 가져오기
    active_count = 0
    total_target = 0
    total_completed = 0
    try:
        if sheets and "이용자현황" in sheets and not sheets["이용자현황"].empty:
            reg = sheets["이용자현황"]
        else:
            from sheets_data import fetch_sheet, SHEET_GIDS, get_registration_status
            reg_sheets = {"이용자현황": fetch_sheet(SHEET_GIDS["이용자현황"])}
            reg = get_registration_status(reg_sheets)

        if not reg.empty:
            from sheets_data import safe_numeric
            first_col = reg.columns[0]
            agencies = [str(v).strip() for v in reg[first_col] if pd.notna(v) and str(v).strip()]
            active_count = len(agencies)

            if "협약인원" in reg.columns:
                total_target = int(reg["협약인원"].apply(safe_numeric).sum())
            if "가입완료" in reg.columns:
                total_completed = int(reg["가입완료"].apply(safe_numeric).sum())
    except:
        pass

    # 2. 세이프: safe_agency_status 테이블
    conn = get_connection()
    c = conn.cursor()

    safe_row = c.execute("SELECT COUNT(*) FROM safe_agency_status").fetchone()
    safe_count = safe_row[0] if safe_row else 0

    conn.close()

    # 3. 베이직 = 활성 - 세이프
    basic_count = max(0, active_count - safe_count)

    summary["total"] = active_count
    summary["active"] = active_count
    summary["inactive"] = 0
    summary["safe"] = safe_count
    summary["safe_count"] = safe_count
    summary["basic"] = basic_count
    summary["basic_count"] = basic_count
    summary["total_target_users"] = total_target
    summary["total_completed"] = total_completed
    summary["registration_rate"] = round(total_completed / total_target * 100, 1) if total_target > 0 else 0

    return summary


def get_note(key: str, default: str = "") -> str:
    conn = get_connection()
    row = conn.execute("SELECT value FROM dashboard_notes WHERE key = ?", (key,)).fetchone()
    conn.close()
    return row[0] if row else default


def save_note(key: str, value: str):
    conn = get_connection()
    conn.execute("""
        INSERT INTO dashboard_notes (key, value, updated_at)
        VALUES (?, ?, datetime('now', 'localtime'))
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
    """, (key, value))
    conn.commit()
    conn.close()


# 앱 시작 시 자동 초기화
init_db()


if __name__ == "__main__":
    init_db()
    print(f"DB path: {DB_PATH}")
    stats = get_data_stats()
    for table, count in stats.items():
        print(f"  {table}: {count} rows")
    summary = get_agency_summary()
    print(f"  agencies: {summary}")
    print("[OK] DB initialized!")
