# Lakehouse CDC 검증 도구

CDC(Change Data Capture)를 통해 소스 데이터베이스의 변경 사항을 Iceberg 테이블로 전달하는 데이터 파이프라인의 무결성을 검증하는 도구입니다.

## 개요

데이터 레이크하우스 아키텍처에서 CDC는 원본 데이터베이스의 변경 사항을 캡처하여 데이터 레이크로 복제하는 중요한 역할을 합니다. 하지만 이 과정에서 데이터 누락, 중복, 또는 불일치 문제가 발생할 수 있습니다. 이 도구는 CDC 파이프라인의 다양한 단계에서 데이터 무결성을 검증하고, 문제가 발견되면 자동으로 복구할 수 있는 기능을 제공합니다.

## 주요 기능

### CDC 데이터 검증
- **시퀀스 검증**: CDC 이벤트의 LSN(Log Sequence Number) 순서를 검사하여 누락된 이벤트를 탐지
- **중복 제거**: 네트워크 문제나 재시도로 인한 중복 이벤트 필터링
- **Kafka 완전성 검증**: Kafka 오프셋 연속성 검사를 통한 메시지 누락 탐지

### CDC와 Iceberg 데이터 일치성 검증
- **레코드 수 검증**: CDC 최종 상태와 Iceberg 테이블 간 레코드 수 비교
- **내용 일치 검증**: 체크섬을 통한 데이터 내용 일치 여부 확인
- **트랜잭션 무결성 검증**: 트랜잭션 단위로 데이터가 정확히 반영되었는지 검증

### 실시간 모니터링 및 복구
- **실시간 검증**: 스트리밍 방식으로 지속적인 데이터 검증 수행
- **자동 알림**: 불일치 발견 시 이메일, Slack 등으로 알림 발송
- **데이터 복구**: 소스 데이터베이스에서 직접 추출하여 Iceberg 테이블 복구

## 설치 방법

### 요구 사항
- Apache Spark 3.x
- Python 3.7 이상
- 필요한 Python 패키지: pyspark, delta-spark (또는 iceberg-spark), kafka-python

### 설치
```bash
git clone https://github.com/username/lakehouse-cdc.git
cd lakehouse-cdc
pip install -r requirements.txt
```

## 사용 방법

### 1. 설정 파일 준비
설정 파일은 JSON 형식으로 다음과 같은 정보를 포함해야 합니다:

```json
{
  "source_type": "batch",
  "kafka_brokers": "broker1:9092,broker2:9092",
  "cdc_topic": "postgres.public.users",
  "schema_path": "/path/to/schema.json",
  "iceberg_catalog": "iceberg",
  "iceberg_schema": "db",
  "iceberg_table": "users",
  "primary_key_columns": ["id"],
  "transaction_id_column": "txid"
}
```

### 2. 검증 실행
다양한 검증 유형에 따라 명령어를 실행할 수 있습니다:

```bash
# CDC 시퀀스 검증
python src/validation/main.py --config /path/to/config.json --validation-type sequence

# CDC와 Iceberg 데이터 일치성 검증
python src/validation/main.py --config /path/to/config.json --validation-type cdc_to_iceberg

# 트랜잭션 무결성 검증
python src/validation/main.py --config /path/to/config.json --validation-type transaction_integrity

# 실시간 검증 스트림 시작
python src/validation/main.py --config /path/to/config.json --validation-type streaming

# 종합 검증 파이프라인 설정
python src/validation/main.py --config /path/to/config.json --validation-type pipeline
```

## 예제

### CDC와 Iceberg 데이터 일치성 검증
```python
from pyspark.sql import SparkSession
from validation.utils.cdc2iceberg import validate_cdc_to_iceberg

# Spark 세션 초기화
spark = SparkSession.builder \
    .appName("CDC-Iceberg Validation") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.iceberg.type", "hadoop") \
    .config("spark.sql.catalog.iceberg.warehouse", "/tmp/iceberg/warehouse") \
    .getOrCreate()

# CDC 이벤트와 Iceberg 테이블 로드
cdc_events = spark.table("cdc_events_table")
iceberg_table = spark.read.format("iceberg").load("iceberg.db.table")

# 기본 키 지정
pk_cols = ["id"]

# 일치성 검증 수행
mismatches = validate_cdc_to_iceberg(cdc_events, iceberg_table, pk_cols)

# 결과 확인
if mismatches.count() > 0:
    print(f"CDC와 Iceberg 간 불일치 발견: {mismatches.count()}개")
    # 불일치 레코드 표시
    mismatches.show(10, truncate=False)
else:
    print("모든 데이터가 일치합니다.")
```

### 트랜잭션 무결성 검증
```python
from validation.utils.cdc2iceberg import validate_transaction_integrity

# 트랜잭션 무결성 검증 수행
tx_result = validate_transaction_integrity(
    spark,
    cdc_events,
    iceberg_table,
    "txid",  # 트랜잭션 ID 컬럼
    ["id"]   # 기본 키 컬럼
)

# 결과 확인
if tx_result['integrity_maintained']:
    print("트랜잭션 무결성이 유지됩니다.")
else:
    print(f"트랜잭션 무결성 위반: {tx_result['incomplete_transactions']}개 트랜잭션")
    
    # 상세 정보가 있는 경우 표시
    if tx_result['details'] is not None:
        tx_result['details'].show(10, truncate=False)
```

## 라이센스

이 프로젝트는 MIT 라이센스에 따라 배포됩니다. 자세한 내용은 [LICENSE](LICENSE) 파일을 참조하세요.
