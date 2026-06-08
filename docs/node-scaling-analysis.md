# 노드 1개 vs 2개 결과 차이 분석 (2026-06-08)

## 질문
노드 1개일 때와 2개일 때 throughput 벤치(3.2GB, 24 map / 3 reduce) 결과(real time)가
차이가 안 나는 이유. 리소스도 겹치는 게 많지 않고 locality도 맞췄는데 왜?

## 측정 데이터

### 업로드된 기존 run (1-*, 2-* 라벨)
- 카운터 거의 동일: Launched map=24, Data-local map=24, HDFS read=3,221,280,804,
  Map input records=13,140,419, CPU time ~176~180s
- real: 2m17s ~ 2m35s (1노드/2노드 라벨 무관하게 동일 범위)

### 클린 2노드 run (라이브 모니터링, 양쪽 NM 4컨테이너씩 확인)
- 양쪽 노드 모두 일함: `k3s-worker2=4c, ubuntu-vm(k3s.test.com)=4c`
- map 진행 중 CPU:
  - worker2: 32~66% (평균 ~40%)
  - ubuntu-vm: 11~26% (평균 ~15%)
- real = **3m15.988s** (기존보다 오히려 느림)
- CPU time=194,870ms(~195s), Total map task time=371,320ms, vCPU 총 6개
- 디스크 %util: ubuntu-vm 기준 0~1.5% (거의 idle)

## 결론

### 1. 이 job은 CPU 바운드가 아니다 (I/O/오버헤드 바운드)
- map 전체 구간에서 6 vCPU 사용률이 합쳐서 ~30~40%. 절반 이상이 계속 idle.
- CPU time 195s ÷ 벽시계 196s = 평균 약 1코어만 사용.
- map task가 슬롯을 371s 점유했지만 CPU는 195s만 사용 → 약 47%는 I/O 대기.
- 디스크도 거의 idle → 순수 디스크 대역폭보다는 HDFS 읽기/네트워크/태스크
  기동·스케줄 오버헤드 + 낮은 동시성(노드당 4컨테이너, AM 제외 동시 map 3~7개)이 지배.
- => 연산이 병목이 아니므로 노드(코어)를 늘려도 빨라지지 않음.

### 2. 2노드가 오히려 느린 이유 — ubuntu-vm이 약한 노드라 straggler 발생
- CPU 분포가 비대칭: worker2 ~40~66% 바쁜데 ubuntu-vm 11~26%로 거의 놀음.
- ubuntu-vm은 control-plane(etcd/apiserver) + NameNode + AM + NM을 vCPU 3개로 같이 돌림.
- 여기 배정된 map task는 자원 경합으로 느리게 돎 → straggler(꼴찌 태스크).
- MapReduce는 가장 느린 map이 끝나야 진행되므로 약한 노드 하나가 전체를 끌어내림.
- map당 평균: 기존 10.7s → 이번 15.5s 로 증가, 전체 2m20s → 3m16s.

### 3. locality는 병목이 아니었다
- dfs.replication=2 + DN 2개라 모든 블록이 두 노드에 복제됨 → 컨테이너를 한 노드에
  몰아도 Data-local=24가 찍힘. 카운터가 같은 건 입력으로 결정되는 값이라 당연.
- 병목은 CPU·디스크 둘 다 한가한 상태의 I/O/오버헤드 + straggler.

## 노드 스케일링 효과를 실제로 보려면
1. control-plane(ubuntu-vm)에 map 안 돌리기: taint 줘서 NameNode/RM 전용으로.
   워커는 **물리적으로 분리된 머신**으로 구성 (예: 노트북 1 + 데스크탑 1).
2. 두 DataNode가 서로 다른 물리 디스크를 쓰게 해야 읽기 I/O가 병렬화됨.
3. 워크로드를 더 크게/CPU 무겁게 (수십 GB or 연산 무거운 job) 해야 스케일링이 드러남.

## 검증에 쓴 명령
```bash
# RM이 보는 노드/컨테이너
kubectl -n hadoop exec deploy/yarn-resourcemanager -- yarn node -list
# 노드별 실시간 CPU
kubectl top nodes
# map 진행률
kubectl -n hadoop logs -l job-name=throughput-bench --tail=40 | grep -E "map .*reduce"
```
