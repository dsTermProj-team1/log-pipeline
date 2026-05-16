# Strimzi Operator 설치 메모

Strimzi Operator는 외부 URL에서 받기 때문에 매니페스트 파일이 아닌 명령어로 관리합니다.

## 설치

```bash
# 네임스페이스 생성 (namespace.yaml 적용)
kubectl apply -f namespace.yaml

# Strimzi Operator 설치
kubectl create -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka

# Operator Pod Ready 대기
kubectl wait --for=condition=Ready pod -l name=strimzi-cluster-operator -n kafka --timeout=300s
```

## 버전 확인

```bash
kubectl get deployment strimzi-cluster-operator -n kafka \
  -o jsonpath='{.spec.template.spec.containers[0].image}'
```

## 제거 (필요시)

```bash
kubectl delete -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
```
