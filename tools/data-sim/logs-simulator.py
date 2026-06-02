#!/usr/bin/env python3
"""
logs-simulator.py — /logs/msa-logs 원본 로그 합성기 (처리량 데모 전용)

목적: "3배 처리량" 데모용. baseline의 results-simulator.py와는 목적이 다르다.
  - results-simulator.py : 집계 '결과'(/results/hourly) 생성, EWMA 곡선 재현용
  - logs-simulator.py(이것) : 집계 '입력' 원본 로그(/logs/msa-logs) 생성, 분산 처리량 시연용

핵심 설계:
  - 데이터를 '충분히 크게' 만든다. MapReduce는 입력을 HDFS 블록(128MB) 단위로 split하므로,
    데이터가 작으면 split=1 → map=1 → 한 노드에서만 돈다(분산 안 보임).
  - --target-mb 로 목표 크기 지정. 200MB면 블록 2개, 400MB면 3~4개 → map 여러 개로 분산.
  - 출력 경로는 1차 잡(LogAggregationDriver, cronjob.yaml)이 기대하는 형식:
      /logs/msa-logs/year=YYYY/month=MM/day=DD/hour=HH/part-*.jsonl
  - 스키마는 common/src/main/resources/schema/log-schema.json 의 필수 7필드 준수.

사용 예:
  python3 logs-simulator.py --target-mb 400 --out ./logs-staging
  # → ./logs-staging/logs/msa-logs/year=2026/month=05/day=31/hour=15/part-*.jsonl
"""

import argparse
import json
import os
import random
from datetime import datetime, timedelta, timezone

SERVICES = ["streaming", "catalog", "recommendation", "auth", "search", "billing"]
METHODS = ["GET", "POST", "PUT", "DELETE", "PATCH"]
GENRES = ["action", "drama", "animation", "comedy"]

# 서비스별 URI 패턴 (현실감)
URI_TEMPLATES = {
    "streaming": "/api/streaming/play/movie_{cid}",
    "catalog": "/api/catalog/browse/{cid}",
    "recommendation": "/api/reco/for-you",
    "auth": "/api/auth/login",
    "search": "/api/search?q=item_{cid}",
    "billing": "/api/billing/charge",
}


def gen_line(rng, ts_iso):
    svc = rng.choice(SERVICES)
    cid = rng.randint(1, 500)
    # 정상 95% / 4xx 3.5% / 5xx 1.5% 정도
    r = rng.random()
    if r < 0.95:
        status = 200
    elif r < 0.985:
        status = rng.choice([400, 401, 403, 404])
    else:
        status = rng.choice([500, 502, 503])

    has_token = status != 401  # 401이면 토큰 무효 모사
    rec = {
        "timestamp": ts_iso,
        "service": svc,
        "ip": f"10.0.{rng.randint(0, 7)}.{rng.randint(1, 254)}",
        "method": rng.choice(METHODS),
        "uri": URI_TEMPLATES[svc].format(cid=cid),
        "status": status,
        "has_valid_token": has_token,
        # 리얼리티용(탐지 미사용, nullable)
        "user_id": f"user_{rng.randint(1, 9999):04d}" if has_token else None,
        "content_id": f"movie_{cid}" if svc in ("streaming", "catalog") else None,
        "response_time_ms": rng.randint(5, 250),
    }
    return json.dumps(rec, ensure_ascii=False)


def main():
    ap = argparse.ArgumentParser(description="/logs/msa-logs 원본 로그 합성기 (처리량 데모)")
    ap.add_argument("--target-mb", type=int, default=400,
                    help="목표 총 크기(MB). 클수록 블록↑ map task↑ 분산↑ (기본 400)")
    ap.add_argument("--date", default="2026-05-31", help="로그 날짜 YYYY-MM-DD")
    ap.add_argument("--hour", type=int, default=15, help="로그 시각 0~23 (단일 파티션)")
    ap.add_argument("--out", default="./logs-staging", help="로컬 출력 루트")
    ap.add_argument("--seed", type=int, default=42, help="재현용 시드")
    ap.add_argument("--files", type=int, default=4,
                    help="파티션 내 파일 분할 수 (병렬 쓰기 모사, split 다양화)")
    args = ap.parse_args()

    rng = random.Random(args.seed)
    d = datetime.strptime(args.date, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    part_dir = os.path.join(
        args.out, "logs", "msa-logs",
        f"year={d.year}", f"month={d.month:02d}", f"day={d.day:02d}", f"hour={args.hour:02d}",
    )
    os.makedirs(part_dir, exist_ok=True)

    target_bytes = args.target_mb * 1024 * 1024
    bytes_per_file = target_bytes // args.files

    total_lines = 0
    base_ts = d.replace(hour=args.hour)

    for fi in range(args.files):
        path = os.path.join(part_dir, f"part-sim{fi:02d}.jsonl")
        written = 0
        with open(path, "w") as f:
            while written < bytes_per_file:
                # 같은 hour 내 랜덤 초/밀리초
                ts = base_ts + timedelta(seconds=rng.randint(0, 3599),
                                         milliseconds=rng.randint(0, 999))
                ts_iso = ts.strftime("%Y-%m-%dT%H:%M:%S.") + f"{ts.microsecond // 1000:03d}Z"
                line = gen_line(rng, ts_iso) + "\n"
                f.write(line)
                written += len(line.encode("utf-8"))
                total_lines += 1

    total_mb = sum(
        os.path.getsize(os.path.join(part_dir, fn)) for fn in os.listdir(part_dir)
    ) / (1024 * 1024)

    print(f"[done] {part_dir}")
    print(f"[size] {total_mb:.1f} MB  lines={total_lines:,}  files={args.files}")
    print(f"[hint] HDFS 128MB 블록 기준 예상 블록 수 ≈ {max(1, int(total_mb // 128) + 1)}")
    print()
    print("[hdfs] 업로드:")
    print(f"  NN=$(kubectl get pod -n hadoop -l app=hdfs-namenode -o jsonpath='{{.items[0].metadata.name}}')")
    print(f"  kubectl cp {os.path.join(args.out, 'logs')} hadoop/$NN:/tmp/logs")
    print(f"  kubectl exec -n hadoop $NN -- hdfs dfs -mkdir -p /logs")
    print(f"  kubectl exec -n hadoop $NN -- hdfs dfs -put -f /tmp/logs/msa-logs /logs/")


if __name__ == "__main__":
    main()
