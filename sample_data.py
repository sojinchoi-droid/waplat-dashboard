# -*- coding: utf-8 -*-
"""샘플 데이터 생성 모듈 - PDF 보고서 구조 반영 (나중에 DB 연결로 교체)"""
import pandas as pd
import numpy as np
import random

random.seed(42)
np.random.seed(42)

# 실제 지자체 목록 (PDF 보고서 기준)
MUNICIPALITIES = [
    ('경기도청', '수도권', '세이프'),
    ('서초구청', '수도권', '세이프'),
    ('강북구청', '수도권', '세이프'),
    ('포천시청', '수도권', '세이프'),
    ('독거노인종합지원센터', '수도권', '세이프'),
    ('희망나래장애인복지관', '수도권', '베이직'),
    ('음성군청', '비수도권', '세이프'),
    ('진천군청', '비수도권', '세이프'),
    ('금정구청', '비수도권', '세이프'),
    ('증평군청', '비수도권', '세이프'),
    ('홍천군청', '비수도권', '세이프'),
    ('강원사회서비스원', '비수도권', '세이프'),
    ('경남사회서비스원', '비수도권', '세이프'),
    ('충북사회서비스원', '비수도권', '세이프'),
    ('강릉시청', '비수도권', '세이프'),
    ('삼척시청', '비수도권', '세이프플러스'),
    ('충남사회서비스원', '비수도권', '베이직&세이프'),
    ('광명시청', '수도권', '세이프'),
]

WEEKS = [f'26-{w:02d}' for w in range(2, 12)]
WEEK_LABELS = [
    '01.04~01.10', '01.11~01.17', '01.18~01.24', '01.25~01.31',
    '02.01~02.07', '02.08~02.14', '02.15~02.21', '02.22~02.28',
    '03.01~03.07', '03.08~03.14'
]


def generate_weekly_data() -> pd.DataFrame:
    """지자체 × 10주 전체 데이터 생성 (PDF 보고서의 모든 지표 포함)"""
    rows = []
    for mun_name, region, service_type in MUNICIPALITIES:
        base_users = random.randint(80, 500)
        base_check = random.uniform(5, 14)
        base_ai = random.uniform(15, 60)
        base_health = random.randint(10, 80)
        base_med = random.randint(5, 60)
        base_talk = random.uniform(2.5, 9)
        base_cardio = random.uniform(5, 40)
        base_stress = random.uniform(10, 45)
        base_consult = random.randint(5, 50)
        base_game_users = random.randint(30, 200)
        base_game_plays = random.randint(500, 8000)
        base_game_time = random.uniform(100, 2000)
        base_delete_rate = random.uniform(1, 15)
        base_ai_intro = random.randint(50, 110)
        base_ai_program = random.randint(20, 80)
        base_ai_service = random.randint(5, 50)
        base_status_change = random.randint(5, 45)
        base_uncheck_48h = random.uniform(5, 15)

        for w_idx, week in enumerate(WEEKS):
            users = max(30, base_users + random.randint(-15, 25) + w_idx * random.randint(-2, 4))
            new_users = max(0, random.randint(-3, 20))
            check_rate = round(max(1.5, min(16, base_check + random.uniform(-2.5, 2.5) + w_idx * random.uniform(-0.3, 0.3))), 2)
            ai_rate = round(max(5, min(75, base_ai + random.uniform(-10, 10) + w_idx * random.uniform(-1.5, 1))), 2)
            health = max(2, base_health + random.randint(-15, 15))
            med = max(1, base_med + random.randint(-10, 10))
            talk = round(max(0.5, base_talk + random.uniform(-1.5, 1.5)), 2)

            # 심혈관체크 이용률
            cardio_rate = round(max(1, min(60, base_cardio + random.uniform(-8, 8) + w_idx * random.uniform(-0.5, 0.5))), 2)
            # 스트레스체크 이용률
            stress_rate = round(max(3, min(55, base_stress + random.uniform(-8, 8) + w_idx * random.uniform(-0.5, 0.5))), 2)
            # 건강상담 이용건수
            consult = max(1, base_consult + random.randint(-10, 10))
            # 맞고 이용자수
            game_users = max(10, base_game_users + random.randint(-20, 20) + w_idx * random.randint(-3, 5))
            # 맞고 플레이판수
            game_plays = max(100, base_game_plays + random.randint(-500, 500) + w_idx * random.randint(-100, 200))
            # 맞고 플레이시간(시간)
            game_time = round(max(50, base_game_time + random.uniform(-200, 200)), 1)
            # 앱 삭제율
            delete_rate = round(max(0.5, min(25, base_delete_rate + random.uniform(-3, 3))), 1)
            # AI 생활지원사 단계별
            ai_intro = max(5, base_ai_intro + random.randint(-10, 10))
            ai_program = max(2, min(ai_intro, base_ai_program + random.randint(-8, 8)))
            ai_service = max(0, min(ai_program, base_ai_service + random.randint(-5, 5)))
            # 안부상태변경 건
            status_change = max(0, base_status_change + random.randint(-8, 8))
            status_change_rate = round(status_change / users * 100, 2) if users > 0 else 0
            # 48시간 미확인률
            uncheck_48h = round(max(2, min(25, base_uncheck_48h + random.uniform(-3, 3))), 2)
            # 안부체크 응답수 (일별 합산 근사)
            check_response = max(5, int(users * check_rate / 100 * 7))

            rows.append({
                '주차': week,
                '기간': WEEK_LABELS[w_idx],
                '지자체명': mun_name,
                '권역': region,
                '서비스유형': service_type,
                '가입자수': users,
                '신규가입': new_users,
                '활성사용자': max(10, int(users * random.uniform(0.5, 0.9))),
                '앱삭제율': delete_rate,
                '안부체크율': check_rate,
                '안부체크응답수': check_response,
                '48시간미확인률': uncheck_48h,
                '안부상태변경건': status_change,
                '안부상태변경률': status_change_rate,
                'AI대화참여율': ai_rate,
                '심혈관체크이용률': cardio_rate,
                '스트레스체크이용률': stress_rate,
                '건강상담이용건수': consult,
                '복약알림이용수': med,
                '맞고이용자수': game_users,
                '맞고플레이판수': game_plays,
                '맞고플레이시간': game_time,
                '1인당대화수': talk,
                'AI생활지원사_인트로': ai_intro,
                'AI생활지원사_프로그램완료': ai_program,
                'AI생활지원사_서비스제안': ai_service,
            })
    return pd.DataFrame(rows)


def get_current_week_data(df: pd.DataFrame) -> pd.DataFrame:
    """최신 주차 데이터 + 자동 계산 결과 반환"""
    current_week = df['주차'].max()
    weeks_sorted = sorted(df['주차'].unique())
    prev_week = weeks_sorted[-2] if len(weeks_sorted) >= 2 else None
    prev2_week = weeks_sorted[-3] if len(weeks_sorted) >= 3 else None
    prev3_week = weeks_sorted[-4] if len(weeks_sorted) >= 4 else None

    cur = df[df['주차'] == current_week].copy()
    prev = df[df['주차'] == prev_week].set_index('지자체명') if prev_week else None
    prev2 = df[df['주차'] == prev2_week].set_index('지자체명') if prev2_week else None
    prev3 = df[df['주차'] == prev3_week].set_index('지자체명') if prev3_week else None

    results = []
    for _, row in cur.iterrows():
        name = row['지자체명']
        r = row.to_dict()

        # 전주 데이터
        if prev is not None and name in prev.index:
            p = prev.loc[name]
            r['전주_안부체크율'] = p['안부체크율']
            r['전주_AI참여율'] = p['AI대화참여율']
            r['전주_복약이용수'] = p['복약알림이용수']
            r['전주_가입자수'] = p['가입자수']
            r['전주_앱삭제율'] = p['앱삭제율']
            r['전주_심혈관체크'] = p['심혈관체크이용률']
            r['전주_스트레스체크'] = p['스트레스체크이용률']
            r['전주_맞고이용자'] = p['맞고이용자수']
        else:
            r['전주_안부체크율'] = r['안부체크율']
            r['전주_AI참여율'] = r['AI대화참여율']
            r['전주_복약이용수'] = r['복약알림이용수']
            r['전주_가입자수'] = r['가입자수']
            r['전주_앱삭제율'] = r['앱삭제율']
            r['전주_심혈관체크'] = r['심혈관체크이용률']
            r['전주_스트레스체크'] = r['스트레스체크이용률']
            r['전주_맞고이용자'] = r['맞고이용자수']

        # 증감 계산
        r['안부체크_증감'] = round(r['안부체크율'] - r['전주_안부체크율'], 2)
        r['AI참여_증감'] = round(r['AI대화참여율'] - r['전주_AI참여율'], 2)
        r['가입자_증감'] = r['가입자수'] - r['전주_가입자수']
        r['앱삭제_증감'] = round(r['앱삭제율'] - r['전주_앱삭제율'], 1)
        r['안부체크_증감률'] = round((r['안부체크율'] - r['전주_안부체크율']) / r['전주_안부체크율'] * 100, 1) if r['전주_안부체크율'] != 0 else 0
        r['AI참여_증감률'] = round((r['AI대화참여율'] - r['전주_AI참여율']) / r['전주_AI참여율'] * 100, 1) if r['전주_AI참여율'] != 0 else 0

        # 3주 연속 하락
        consec = False
        if prev is not None and prev2 is not None and prev3 is not None:
            if name in prev.index and name in prev2.index and name in prev3.index:
                consec = (r['안부체크율'] < prev.loc[name]['안부체크율'] and
                          prev.loc[name]['안부체크율'] < prev2.loc[name]['안부체크율'] and
                          prev2.loc[name]['안부체크율'] < prev3.loc[name]['안부체크율'])
        r['3주연속하락'] = consec

        # 상태 분류
        cr = r['안부체크율']
        cd = r['안부체크_증감']
        cc = r['안부체크_증감률']
        if cr >= 12 and cd >= 0:
            r['안부체크_상태'] = '우수'
        elif cr < 5 or consec:
            r['안부체크_상태'] = '위험'
        elif cr < 8 or cc <= -15:
            r['안부체크_상태'] = '주의'
        else:
            r['안부체크_상태'] = '보통'

        ar = r['AI대화참여율']
        ad = r['AI참여_증감']
        ac = r['AI참여_증감률']
        ai_consec = False
        if prev is not None and prev2 is not None and prev3 is not None:
            if name in prev.index and name in prev2.index and name in prev3.index:
                ai_consec = (ar < prev.loc[name]['AI대화참여율'] and
                             prev.loc[name]['AI대화참여율'] < prev2.loc[name]['AI대화참여율'] and
                             prev2.loc[name]['AI대화참여율'] < prev3.loc[name]['AI대화참여율'])
        if ar >= 50 and ad >= 0:
            r['AI참여_상태'] = '우수'
        elif ar < 15 or ai_consec:
            r['AI참여_상태'] = '위험'
        elif ar < 30 or ac <= -20:
            r['AI참여_상태'] = '주의'
        else:
            r['AI참여_상태'] = '보통'

        med_rate = r['복약알림이용수'] / r['가입자수'] * 100 if r['가입자수'] > 0 else 0
        if med_rate >= 20:
            r['복약_상태'] = '우수'
        elif med_rate < 5:
            r['복약_상태'] = '위험'
        elif med_rate < 10:
            r['복약_상태'] = '주의'
        else:
            r['복약_상태'] = '보통'

        # 종합 상태
        statuses = [r['안부체크_상태'], r['AI참여_상태'], r['복약_상태']]
        danger = statuses.count('위험')
        caution = statuses.count('주의')
        excellent = statuses.count('우수')
        ok_or_better = statuses.count('보통') + excellent
        if danger >= 1:
            r['종합상태'] = '집중관리'
        elif caution >= 2:
            r['종합상태'] = '주의관리'
        elif ok_or_better == 3 and excellent >= 1:
            r['종합상태'] = '우수사례'
        else:
            r['종합상태'] = '정상'

        # 서술형 인사이트
        arrow = '▲' if cd > 0 else '▼' if cd < 0 else '→'
        if r['종합상태'] == '우수사례':
            comment = '양호합니다. 모범 사례로 선정됩니다.'
        elif r['종합상태'] == '정상':
            comment = '정상 범위입니다.'
        elif r['종합상태'] == '주의관리':
            comment = '주의가 필요합니다. 모니터링 강화 권장.'
        else:
            if consec:
                comment = '안부체크율 3주 연속 하락! 즉시 담당자 연락 권장.'
            else:
                comment = '즉시 조치가 필요합니다.'
        r['인사이트'] = f"{name}의 안부체크율은 {cr}%(전주 대비 {arrow} {abs(cd)}%p)로 {comment}"

        # AI 생활지원사 참여율
        if r['가입자수'] > 0:
            r['AI생활지원사_참여율'] = round(r['AI생활지원사_인트로'] / r['가입자수'] * 100, 1)
            r['AI생활지원사_완료율'] = round(r['AI생활지원사_프로그램완료'] / max(1, r['AI생활지원사_인트로']) * 100, 1)
        else:
            r['AI생활지원사_참여율'] = 0
            r['AI생활지원사_완료율'] = 0

        # 맞고 1인당 플레이판수
        r['맞고1인당플레이'] = round(r['맞고플레이판수'] / max(1, r['맞고이용자수']), 1)

        # 상태 점수 (정렬용)
        score_map = {'집중관리': 1, '주의관리': 2, '정상': 3, '우수사례': 4}
        r['상태점수'] = score_map.get(r['종합상태'], 3)

        results.append(r)

    return pd.DataFrame(results)
