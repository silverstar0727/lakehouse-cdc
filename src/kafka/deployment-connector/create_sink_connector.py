#!/usr/bin/env python3
"""
Iceberg Sink Connector creation script for Kafka Connect
"""

import requests
import json
import sys
import os
import time

# 환경 변수에서 필요한 값 가져오기
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "postgres-connector.public.items")
# 네임스페이스와 테이블 이름 분리
TABLE_NAMESPACE = os.getenv("TABLE_NAMESPACE", "fastapi_db")
TABLE_NAME_ONLY = os.getenv("TABLE_NAME_ONLY", "items")
TABLE_FULL_NAME = f"{TABLE_NAMESPACE}.{TABLE_NAME_ONLY}"

EXTERNAL_IP = os.getenv("EXTERNAL_IP")
if not EXTERNAL_IP:
    print("오류: EXTERNAL_IP 환경 변수가 설정되지 않았습니다.")
    sys.exit(1)

# Rest Catalog 설정
REST_CATALOG_URL = os.getenv("REST_CATALOG_URL", f"http://iceberg-rest-catalog.{EXTERNAL_IP}.nip.io")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://rook-ceph-rgw-my-store.rook-ceph.svc:80")
WAREHOUSE_LOCATION = os.getenv("WAREHOUSE_LOCATION", "s3a://iceberg-warehouse/")

# S3 자격 증명
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_KEY")

# Kafka Connect URL 설정
CONNECT_URL = os.getenv("CONNECT_URL", f"http://kafka-connect.{EXTERNAL_IP}.nip.io")

# Connector 이름
CONNECTOR_NAME = os.getenv("CONNECTOR_NAME", "iceberg-sink")

CONNECTOR_CONFIG = {
    "name": CONNECTOR_NAME,
    "config": {
        "connector.class": "io.tabular.iceberg.connect.IcebergSinkConnector",
        "tasks.max": "1",
        "topics": KAFKA_TOPIC,
        "iceberg.tables": TABLE_FULL_NAME,
        
        # REST 카탈로그 설정
        "iceberg.catalog.type": "rest",
        "iceberg.catalog.uri": REST_CATALOG_URL,
        "iceberg.catalog.warehouse": WAREHOUSE_LOCATION,
        
        # S3 파일 IO 설정
        "iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "iceberg.catalog.s3.endpoint": S3_ENDPOINT,
        "iceberg.catalog.s3.path-style-access": "true",
        "iceberg.catalog.s3.access-key-id": AWS_ACCESS_KEY,
        "iceberg.catalog.s3.secret-access-key": AWS_SECRET_KEY,
        "iceberg.catalog.s3.region": "us-east-1",
        
        # 테이블 설정
        "iceberg.tables.auto-create-enabled": "true",
        
        # ID 열 설정 - 중요: 이 부분이 핵심 문제였습니다
        "iceberg.tables.default-id-columns": "id",  # 테이블의 ID 컬럼 지정
        
        # 컨버터 설정
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "true",
        "value.converter.schemas.enable": "true",

        # 추가 옵션
        "consumer.auto.offset.reset": "earliest",
        
        # DELETE 처리 설정
        "iceberg.tables.cdc-field": "_deleted",  # CDC 필드 설정
        "iceberg.tables.upsert-mode-enabled": "true",  # 업서트 모드 활성화
        
        # # 변환 설정
        # "transforms": "RenameField",
        # "transforms.RenameField.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
        # "transforms.RenameField.renames": "__deleted:_deleted",
        # # Add this to your CONNECTOR_CONFIG in the transforms section
        "transforms": "RenameField,ConvertDeleted",
        "transforms.RenameField.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
        "transforms.RenameField.renames": "__deleted:_deleted",
        "transforms.ConvertDeleted.type": "org.apache.kafka.connect.transforms.Cast$Value",
        "transforms.ConvertDeleted.spec": "_deleted:boolean"
    }
}

def create_connector():
    """
    Iceberg Sink 커넥터를 생성하는 함수
    """
    headers = {"Content-Type": "application/json"}
    
    print(f"Iceberg Sink 커넥터를 생성합니다. URL: {CONNECT_URL}")
    print("커넥터 설정:")
    print(json.dumps(CONNECTOR_CONFIG, indent=2))
    
    try:
        # 기존 커넥터 확인 및 삭제
        try:
            response = requests.get(f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}")
            if response.status_code == 200:
                print(f"커넥터 '{CONNECTOR_NAME}'가 이미 존재합니다. 삭제 후 재생성합니다.")
                delete_response = requests.delete(f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}")
                print(f"삭제 응답: HTTP {delete_response.status_code}")
                # 삭제 후 잠시 대기
                time.sleep(2)
        except Exception as e:
            print(f"커넥터 확인 중 예외 발생 (무시됨): {e}")
        
        # 커넥터 생성 요청
        response = requests.post(
            f"{CONNECT_URL}/connectors",
            headers=headers,
            data=json.dumps(CONNECTOR_CONFIG)
        )
        
        # 응답 확인
        if response.status_code in [201, 200]:
            print(f"Iceberg Sink 커넥터 생성 성공!")
            print(json.dumps(response.json(), indent=2))
            return 0
        else:
            print(f"Iceberg Sink 커넥터 생성 실패: HTTP {response.status_code}")
            print(response.text)
            return 1
    
    except requests.exceptions.RequestException as e:
        print(f"API 요청 중 오류 발생: {e}")
        return 1

def check_connector_status():
    """
    생성된 커넥터의 상태를 확인하는 함수
    """
    try:
        response = requests.get(f"{CONNECT_URL}/connectors/{CONNECTOR_NAME}/status")
        if response.status_code == 200:
            status = response.json()
            print("커넥터 상태:")
            print(json.dumps(status, indent=2))
            
            # 태스크 실패 시 상세 디버깅 정보
            tasks = status.get("tasks", [])
            for task in tasks:
                if task.get("state") == "FAILED":
                    print("\n태스크 실패 감지:")
                    print(f"Trace: {task.get('trace')}")
                    print("\n가능한 해결책:")
                    print("1. 카탈로그 설정이 올바른지 확인하세요.")
                    print("2. REST 카탈로그 엔드포인트가 접근 가능한지 확인하세요.")
                    print("3. S3 자격 증명과 엔드포인트가 정확한지 확인하세요.")
            
            return 0
        else:
            print(f"커넥터 상태 확인 실패: HTTP {response.status_code}")
            print(response.text)
            return 1
    except requests.exceptions.RequestException as e:
        print(f"상태 확인 중 오류 발생: {e}")
        return 1

if __name__ == "__main__":
    # 커넥터 생성 실행
    exit_code = create_connector()
    
    if exit_code == 0:
        print("커넥터 생성 후 상태 확인 중...")
        time.sleep(5)  # 커넥터가 시작될 때까지 잠시 대기
        check_connector_status()
    
    sys.exit(exit_code)