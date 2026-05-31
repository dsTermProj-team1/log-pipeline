#!/usr/bin/env python3
"""
results-simulator.py — /results/hourly 합성 데이터 생성기

박지호의 Kafka 시뮬레이터(원본 로그 -> Kafka)는 1차 집계 경로를 타므로
baseline(EWMA) 데모에는 쓸 수 없다. 이 스크립트는 MapReduce 1차 집계
(시간별 잡)가 냈을 법한 출력 계약을 그대로 따라, /results/hourly 파티션을
직접 만들어 둔다.

출력 계약(AggregationResult, snake_case, 고정 순서):
  {"service": ..., "hour": 0~23, "requests": N,
   "count_4xx": N, "count_5xx": N, "error_rate": (4xx+5xx)/requests}

디렉토리 구조 (MapReduce 출력 모방):
  <out>/results/hourly/<YYYY-MM-DD-HH>/part-r-00000   (서비스당 1줄 JSONL)
  <out>/results/hourly/<YYYY-MM-DD-HH>/_SUCCESS

설계 의도:
  - 시간대 곡선: 새벽 저점 -> 15시 피크 -> 야간 하락  (발표 p.14 그래프 재현)
  - 날짜 추세: 최근일일수록 트래픽 상승 + 최근 2일 추가 부스트
              -> baseline 잡의 EWMA가 최근을 우대해 기준선이 위로 당겨지는
                 효과를 before/after로 보여줄 수 있게 한다.
"""

import argparse
import json
import os
import random
from datetime import date, timedelta

# ── 서비스별 피크 시간당 요청량(15시 기준 대략값) ──
SERVICES = {
    "streaming":      1200,   # 영상 재생 — 최대 트래픽
    "catalog":         600,   # 콘텐츠 탐색
    "recommendation":  450,
    "auth":            300,   # 로그인/토큰 발급
    "search":          250,
    "billing":          80,   # 결제 — 소량
}

# ── 시간대(0~23) 곡선: 15시=1.0(피크). p.14(12~18 피크, 15시 최대) 재현 ──
# 낮 시간대를 단계지게 설계 → baseline 구간 매핑이 1/2/4/3로 갈림
# (06~12 한산, 12~18 뾰족 피크, 18~24 저녁 시청 약간 높음 = OTT 현실 반영)
HOUR_SHAPE = {
    0: 0.09, 1: 0.06, 2: 0.05, 3: 0.05, 4: 0.06, 5: 0.09,    # 00~06 저점
    6: 0.18, 7: 0.26, 8: 0.34, 9: 0.40, 10: 0.44, 11: 0.48,  # 06~12 완만 상승
    12: 0.72, 13: 0.86, 14: 0.95, 15: 1.00, 16: 0.93, 17: 0.80,  # 12~18 피크
    18: 0.52, 19: 0.46, 20: 0.44, 21: 0.38, 22: 0.30, 23: 0.18,  # 18~24 저녁
}


def day_trend_factor(day_index, total_days, growth, boost_days, boost):
    """day_index: 0=가장 오래된 날, total_days-1=가장 최근 날.
    완만한 증가(growth) + 최근 boost_days일 추가 부스트(boost)."""
    span = max(total_days - 1, 1)
    factor = 1.0 + growth * (day_index / span)
    if day_index >= total_days - boost_days:
        factor *= (1.0 + boost)
    return factor


def gen_requests(base_peak, hour_factor, day_factor, jitter, rng):
    mean = base_peak * hour_factor * day_factor
    val = mean * rng.uniform(1.0 - jitter, 1.0 + jitter)
    return max(0, int(round(val)))


def gen_errors(requests, hour_factor, rng):
    """4xx: 평상시 ~1.5~2.5%. 5xx: 피크일수록 상승(혼잡 모사)."""
    if requests == 0:
        return 0, 0
    p4 = 0.015 + 0.010 * rng.random()
    p5 = 0.004 + 0.008 * hour_factor
    c4 = min(int(round(requests * p4)), requests)
    c5 = min(int(round(requests * p5)), requests - c4)
    return c4, c5


def build_record(service, hour, requests, c4, c5):
    err = round((c4 + c5) / requests, 4) if requests > 0 else 0.0
    # dict 삽입 순서 = 출력 계약 고정 순서 (Python 3.7+ 보장)
    return {
        "service": service,
        "hour": hour,
        "requests": requests,
        "count_4xx": c4,
        "count_5xx": c5,
        "error_rate": err,
    }


def main():
    ap = argparse.ArgumentParser(description="/results/hourly 합성 데이터 생성기")
    ap.add_argument("--days", type=int, default=7, help="생성할 일수 (lookback 상한과 맞춤)")
    ap.add_argument("--end-date", default=date.today().isoformat(),
                    help="가장 최근 날짜 YYYY-MM-DD (기본: 오늘)")
    ap.add_argument("--out", default="./staging", help="로컬 출력 루트")
    ap.add_argument("--seed", type=int, default=42, help="재현용 시드")
    ap.add_argument("--jitter", type=float, default=0.12, help="시간당 요청 ±지터 비율")
    ap.add_argument("--growth", type=float, default=0.45, help="오래된->최근 완만 증가율")
    ap.add_argument("--boost-days", type=int, default=2, help="추가 부스트 적용 최근 일수")
    ap.add_argument("--boost", type=float, default=0.20, help="최근일 추가 부스트 비율")
    args = ap.parse_args()

    rng = random.Random(args.seed)
    end = date.fromisoformat(args.end_date)
    start = end - timedelta(days=args.days - 1)

    root = os.path.join(args.out, "results", "hourly")
    os.makedirs(root, exist_ok=True)

    # 요약용: (newest day) 시간대별 전 서비스 합계
    newest_hour_totals = {h: 0 for h in range(24)}
    n_partitions = 0

    for d in range(args.days):
        cur = start + timedelta(days=d)
        day_factor = day_trend_factor(d, args.days, args.growth,
                                       args.boost_days, args.boost)
        for hour in range(24):
            hour_factor = HOUR_SHAPE[hour]
            part_dir = os.path.join(root, f"{cur.isoformat()}-{hour:02d}")
            os.makedirs(part_dir, exist_ok=True)
            lines = []
            for svc, peak in SERVICES.items():
                req = gen_requests(peak, hour_factor, day_factor, args.jitter, rng)
                c4, c5 = gen_errors(req, hour_factor, rng)
                lines.append(json.dumps(build_record(svc, hour, req, c4, c5),
                                        ensure_ascii=False))
                if d == args.days - 1:
                    newest_hour_totals[hour] += req
            with open(os.path.join(part_dir, "part-r-00000"), "w") as f:
                f.write("\n".join(lines) + "\n")
            open(os.path.join(part_dir, "_SUCCESS"), "w").close()
            n_partitions += 1

    # ── 요약 출력 ──
    print(f"[done] {start.isoformat()} ~ {end.isoformat()}  "
          f"days={args.days}  partitions={n_partitions}  "
          f"services={len(SERVICES)}")
    print(f"[out]  {os.path.abspath(root)}")
    peak_h = max(newest_hour_totals, key=newest_hour_totals.get)
    print(f"[curve] 최근일 시간대별 총 요청 (피크 hour={peak_h}):")
    mx = max(newest_hour_totals.values()) or 1
    for h in range(24):
        bar = "#" * int(40 * newest_hour_totals[h] / mx)
        print(f"  {h:02d}  {newest_hour_totals[h]:6d} {bar}")
    print("\n[hdfs] 업로드:")
    print("  hdfs dfs -mkdir -p /results")
    print(f"  hdfs dfs -put -f {os.path.join(args.out, 'results', 'hourly')} /results/")


if __name__ == "__main__":
    main()