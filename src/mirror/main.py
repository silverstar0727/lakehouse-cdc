#!/usr/bin/env python3
"""
PyIceberg를 사용하여 Sink Connector로 생성된 테이블을 mirror table로 활용하고
실제 사용할 별도의 테이블을 생성하는 스크립트
"""

import os
import sys
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.transforms import day, identity
from pyiceberg.partitioning import PartitionSpec
from pyiceberg.table import TableProperties
import time

# 환경 변수에서 필요한 값 가져오기
TABLE_NAMESPACE = os.getenv("TABLE_NAMESPACE", "fastapi_db")
TABLE_NAME_ONLY = os.getenv("TABLE_NAME_ONLY", "items")
MIRROR_TABLE_NAME = f"{TABLE_NAME_ONLY}_mirror"  # mirror 테이블 이름
TARGET_TABLE_NAME = f"{TABLE_NAME_ONLY}_production"  # 실제 사용할 테이블 이름

# Rest Catalog 설정
EXTERNAL_IP = os.getenv("EXTERNAL_IP")
if not EXTERNAL_IP:
    print("오류: EXTERNAL_IP 환경 변수가 설정되지 않았습니다.")
    sys.exit(1)

REST_CATALOG_URL = os.getenv("REST_CATALOG_URL", f"http://iceberg-rest-catalog.{EXTERNAL_IP}.nip.io")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://rook-ceph-rgw-my-store.rook-ceph.svc:80")
WAREHOUSE_LOCATION = os.getenv("WAREHOUSE_LOCATION", "s3a://iceberg-warehouse/")

# S3 자격 증명
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")

def initialize_catalog():
    """PyIceberg 카탈로그 초기화"""
    catalog_properties = {
        "uri": REST_CATALOG_URL,
        "warehouse": WAREHOUSE_LOCATION,
        "type": "rest",
        "s3.endpoint": S3_ENDPOINT,
        "s3.access-key-id": AWS_ACCESS_KEY,
        "s3.secret-access-key": AWS_SECRET_KEY,
        "s3.path-style-access": "true",
        "io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
    }
    
    try:
        catalog = load_catalog("rest_catalog", **catalog_properties)
        print(f"카탈로그 '{catalog.name}' 로드 완료")
        return catalog
    except Exception as e:
        print(f"카탈로그 초기화 오류: {e}")
        sys.exit(1)

def get_source_table(catalog):
    """Sink Connector에 의해 생성된 원본 테이블 가져오기"""
    try:
        source_table = catalog.load_table(f"{TABLE_NAMESPACE}.{TABLE_NAME_ONLY}")
        print(f"원본 테이블 '{TABLE_NAMESPACE}.{TABLE_NAME_ONLY}' 로드 완료")
        return source_table
    except NoSuchTableError:
        print(f"원본 테이블 '{TABLE_NAMESPACE}.{TABLE_NAME_ONLY}'이(가) 존재하지 않습니다.")
        print("Sink Connector가 테이블을 생성했는지 확인하세요.")
        sys.exit(1)
    except Exception as e:
        print(f"원본 테이블 로드 오류: {e}")
        sys.exit(1)

def create_mirror_table(catalog, source_table):
    """원본 테이블을 기반으로 mirror 테이블 생성"""
    try:
        # mirror 테이블이 이미 존재하는지 확인
        try:
            catalog.load_table(f"{TABLE_NAMESPACE}.{MIRROR_TABLE_NAME}")
            print(f"Mirror 테이블 '{TABLE_NAMESPACE}.{MIRROR_TABLE_NAME}'이(가) 이미 존재합니다.")
            print("Mirror 테이블 재설정을 위해 삭제합니다...")
            catalog.drop_table(f"{TABLE_NAMESPACE}.{MIRROR_TABLE_NAME}")
            print(f"Mirror 테이블 '{TABLE_NAMESPACE}.{MIRROR_TABLE_NAME}' 삭제 완료")
        except NoSuchTableError:
            pass  # 테이블이 없으면 계속 진행
        
        # 원본 테이블의 스키마와 설정 가져오기
        source_schema = source_table.schema()
        source_spec = source_table.spec()
        source_props = source_table.properties
        
        # mirror 테이블 생성
        print(f"Mirror 테이블 '{TABLE_NAMESPACE}.{MIRROR_TABLE_NAME}' 생성 중...")
        mirror_props = {
            TableProperties.DEFAULT_FILE_FORMAT: "parquet",
            TableProperties.FORMAT_VERSION: "2",
            "comment": f"Mirror table for {TABLE_NAMESPACE}.{TABLE_NAME_ONLY}"
        }
        
        # 원본 테이블 속성 중 필요한 것 추가
        for key, value in source_props.items():
            if key.startswith("write.") or key.startswith("read."):
                mirror_props[key] = value
        
        mirror_table = catalog.create_table(
            identifier=f"{TABLE_NAMESPACE}.{MIRROR_TABLE_NAME}",
            schema=source_schema,
            partition_spec=source_spec,
            properties=mirror_props
        )
        
        print(f"Mirror 테이블 '{TABLE_NAMESPACE}.{MIRROR_TABLE_NAME}' 생성 완료")
        return mirror_table
    except Exception as e:
        print(f"Mirror 테이블 생성 오류: {e}")
        sys.exit(1)

def create_production_table(catalog, mirror_table):
    """실제 사용할 Production 테이블 생성"""
    try:
        # production 테이블이 이미 존재하는지 확인
        try:
            catalog.load_table(f"{TABLE_NAMESPACE}.{TARGET_TABLE_NAME}")
            print(f"Production 테이블 '{TABLE_NAMESPACE}.{TARGET_TABLE_NAME}'이(가) 이미 존재합니다.")
            print("기존 테이블을 유지하고 다음 단계로 진행합니다.")
            return catalog.load_table(f"{TABLE_NAMESPACE}.{TARGET_TABLE_NAME}")
        except NoSuchTableError:
            pass  # 테이블이 없으면 새로 생성
        
        # mirror 테이블의 스키마 가져오기
        mirror_schema = mirror_table.schema()
        
        # 필요에 따라 스키마 수정 가능 (예: 추가 필드, 변환 등)
        # 아래 코드는 필요에 따라 수정하세요
        
        # 파티션 명세 생성 - 예시: created_at 필드로 일별 파티셔닝
        # 참고: 원본 스키마에 created_at이 있다고 가정합니다. 없으면 적절히 수정하세요.
        try:
            has_created_at = any(field.name == "created_at" for field in mirror_schema.fields)
            has_updated_at = any(field.name == "updated_at" for field in mirror_schema.fields)
            has_timestamp = any(field.name == "timestamp" for field in mirror_schema.fields)
            
            if has_created_at:
                partition_spec = PartitionSpec.builder().add(day("created_at")).build()
            elif has_updated_at:
                partition_spec = PartitionSpec.builder().add(day("updated_at")).build()
            elif has_timestamp:
                partition_spec = PartitionSpec.builder().add(day("timestamp")).build()
            else:
                # 적절한 날짜/시간 필드가 없으면 기본 파티션 없음
                partition_spec = PartitionSpec.unpartitioned()
        except Exception as e:
            print(f"파티션 명세 생성 중 오류 발생: {e}")
            print("기본 파티션 없이 계속 진행합니다.")
            partition_spec = PartitionSpec.unpartitioned()
        
        # Production 테이블 생성
        print(f"Production 테이블 '{TABLE_NAMESPACE}.{TARGET_TABLE_NAME}' 생성 중...")
        prod_props = {
            TableProperties.DEFAULT_FILE_FORMAT: "parquet",
            TableProperties.FORMAT_VERSION: "2",
            "comment": f"Production table based on {TABLE_NAMESPACE}.{MIRROR_TABLE_NAME}",
            # 필요한 테이블 속성 추가
            "write.target-file-size-bytes": "134217728",  # 128MB
            "write.distribution-mode": "hash"
        }
        
        production_table = catalog.create_table(
            identifier=f"{TABLE_NAMESPACE}.{TARGET_TABLE_NAME}",
            schema=mirror_schema,
            partition_spec=partition_spec,
            properties=prod_props
        )
        
        print(f"Production 테이블 '{TABLE_NAMESPACE}.{TARGET_TABLE_NAME}' 생성 완료")
        return production_table
    except Exception as e:
        print(f"Production 테이블 생성 오류: {e}")
        sys.exit(1)

def setup_data_mirroring(catalog, source_table, mirror_table, production_table):
    """원본 테이블에서 mirror 테이블로, 그리고 production 테이블로 데이터 복사"""
    # 참고: 실제 구현은 사용 사례에 따라 다를 수 있습니다.
    # 여기서는 개념적인 단계만 제시합니다.
    
    print("데이터 미러링 설정을 위한 단계 시작...")
    
    # 1. 초기 데이터 마이그레이션
    # 실제 환경에서는 Spark 또는 Flink와 같은 엔진을 사용하여 데이터를 복사하는 것이 좋습니다.
    # 여기서는 간단한 SQL 쿼리 예시만 제공합니다.
    
    copy_sql = f"""
    -- 실제 Spark SQL 또는 Trino와 같은 엔진에서 실행할 쿼리
    INSERT INTO {TABLE_NAMESPACE}.{MIRROR_TABLE_NAME}
    SELECT * FROM {TABLE_NAMESPACE}.{TABLE_NAME_ONLY}
    """
    
    print(f"초기 데이터 마이그레이션 쿼리 예시:")
    print(copy_sql)
    
    # 2. 정기적인 데이터 동기화 설정 방법 안내
    print("\n정기적인 데이터 동기화를 위한 권장 방법:")
    print("1. Spark Structured Streaming을 사용한 주기적 작업 설정")
    print("2. 증분 스냅샷 복사를 위한 마지막 스냅샷 ID 추적")
    print("3. Airflow와 같은 워크플로우 도구로 동기화 작업 예약")
    
    # 3. Production 테이블에 데이터 복사 (필터링 또는 변환 가능)
    prod_copy_sql = f"""
    -- 실제 Spark SQL 또는 Trino와 같은 엔진에서 실행할 쿼리
    -- 필요한 변환이나 필터링 적용 가능
    INSERT INTO {TABLE_NAMESPACE}.{TARGET_TABLE_NAME}
    SELECT * FROM {TABLE_NAMESPACE}.{MIRROR_TABLE_NAME}
    -- WHERE 조건이 필요한 경우 추가
    """
    
    print("\nProduction 테이블 데이터 복사 쿼리 예시:")
    print(prod_copy_sql)
    
    print("\nPyIceberg 테이블 설정 및 데이터 미러링 안내 완료")
    print("참고: 실제 데이터 복사는 Spark, Flink 또는 Trino와 같은 별도의 쿼리 엔진이 필요합니다.")

def show_table_info(table, table_name):
    """테이블 정보 출력"""
    print(f"\n{table_name} 테이블 정보:")
    print(f"  식별자: {table.identifier}")
    print(f"  위치: {table.location()}")
    print(f"  현재 스냅샷 ID: {table.current_snapshot_id if table.current_snapshot_id else '없음'}")
    
    print("\n  스키마:")
    for field in table.schema().fields:
        print(f"    - {field.name}: {field.type}")
    
    print("\n  파티션 명세:")
    if table.spec().fields:
        for field in table.spec().fields:
            print(f"    - {field.name}: {field.transform}")
    else:
        print("    (파티션 없음)")
    
    print("\n  주요 속성:")
    for key, value in table.properties.items():
        if key in [TableProperties.DEFAULT_FILE_FORMAT, TableProperties.FORMAT_VERSION, "comment"]:
            print(f"    - {key}: {value}")

def main():
    """메인 함수"""
    print("PyIceberg를 사용한 미러 테이블 및 프로덕션 테이블 설정 시작")
    
    # 1. 카탈로그 초기화
    catalog = initialize_catalog()
    
    # 2. 원본 테이블 로드 (Sink Connector에 의해 생성됨)
    source_table = get_source_table(catalog)
    show_table_info(source_table, "원본")
    
    # 3. Mirror 테이블 생성
    mirror_table = create_mirror_table(catalog, source_table)
    show_table_info(mirror_table, "Mirror")
    
    # 4. Production 테이블 생성
    production_table = create_production_table(catalog, mirror_table)
    show_table_info(production_table, "Production")
    
    # 5. 데이터 미러링 설정
    setup_data_mirroring(catalog, source_table, mirror_table, production_table)
    
    print("\n작업 완료! 모든 테이블이 성공적으로 설정되었습니다.")

if __name__ == "__main__":
    main()