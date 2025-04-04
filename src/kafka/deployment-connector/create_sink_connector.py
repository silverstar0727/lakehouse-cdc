#!/usr/bin/env python3
import requests
import json
import sys
import os


#    export EXTERNAL_IP=$(kubectl get service -n ingress-nginx nginx-ingress-ingress-nginx-controller -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
#    export OBC_ACCESS_KEY=$(kubectl get secret -n iceberg iceberg-warehouse-bucket -o jsonpath='{.data.AWS_ACCESS_KEY_ID}' | base64 --decode)
#    export OBC_SECRET_KEY=$(kubectl get secret -n iceberg iceberg-warehouse-bucket -o jsonpath='{.data.AWS_SECRET_ACCESS_KEY}' | base64 --decode)


# 환경 변수에서 EXTERNAL_IP 가져오기
EXTERNAL_IP = os.getenv("EXTERNAL_IP")
OBC_ACCESS_KEY = os.getenv("OBC_ACCESS_KEY")
OBC_SECRET_KEY = os.getenv("OBC_SECRET_KEY")
if not EXTERNAL_IP or not OBC_ACCESS_KEY or not OBC_SECRET_KEY:
    print("Error: EXTERNAL_IP, OBC_ACCESS_KEY, 또는 OBC_SECRET_KEY 환경 변수가 설정되지 않았습니다.")
    sys.exit(1)

# Kafka Connect URL 설정
CONNECT_URL = f"http://kafka-connect.{EXTERNAL_IP}.nip.io"

# Iceberg Sink 커넥터 설정
CONNECTOR_CONFIG = {
    "name": "iceberg-sink6",
    "config": {
        "connector.class": "io.tabular.iceberg.connect.IcebergSinkConnector",
        "tasks.max": "1",
        "topics": "postgres-connector.public.items",
        
        # 컨버터 설정
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "true",
        "value.converter.schemas.enable": "true",
        
        # 테이블 설정
        "iceberg.tables": "itemsdemo6",
        "iceberg.table.namespace": "fastapi_db",
        "iceberg.table.auto-create": "true",
        "iceberg.table.create-mode": "create-or-replace",
        
        # 카탈로그 설정
        "iceberg.catalog": "rest",
        "iceberg.catalog.type": "rest",
        "iceberg.catalog.uri": "http://iceberg-rest-catalog.35.226.15.228.nip.io",
        "iceberg.catalog.warehouse": "s3://iceberg-warehouse/",
        
        # S3 설정
        "iceberg.catalog.credential": "static",
        "iceberg.catalog.credential.static.access-key": OBC_ACCESS_KEY,
        "iceberg.catalog.credential.static.secret-key": OBC_SECRET_KEY,
        "iceberg.catalog.s3.endpoint-url": "http://ceph.35.226.15.228.nip.io",
        "iceberg.catalog.s3.path-style-access": "true",
        "iceberg.catalog.client.region": "us-east-1",
        
        # 커밋 설정
        "iceberg.control.commit-interval-ms": "30000",
        "iceberg.control.commit-timeout-ms": "300000",
        "iceberg.commit-all": "true",
        "iceberg.control.properties.delete-mode": "copy-on-write",
        "iceberg.control.properties.upsert-mode": "copy-on-write"
    }
}


def create_connector():
    headers = {"Content-Type": "application/json"}
    
    print(f"Iceberg 싱크 커넥터를 생성합니다. URL: {CONNECT_URL}")
    
    try:
        # Check existing connectors (to prevent name conflicts)
        try:
            response = requests.get(f"{CONNECT_URL}/connectors/{CONNECTOR_CONFIG['name']}")
            if response.status_code == 200:
                print(f"Connector '{CONNECTOR_CONFIG['name']}' already exists. Deleting and recreating.")
                # Delete existing connector
                requests.delete(f"{CONNECT_URL}/connectors/{CONNECTOR_CONFIG['name']}")
        except:
            pass  # Ignore errors during verification and continue
        
        # Request connector creation
        response = requests.post(
            f"{CONNECT_URL}/connectors",
            headers=headers,
            data=json.dumps(CONNECTOR_CONFIG)
        )
        
        # Check response
        if response.status_code == 201 or response.status_code == 200:
            print(f"Iceberg 싱크 커넥터 생성 성공!")
            print(json.dumps(response.json(), indent=2))
            return 0
        else:
            print(f"Iceberg 싱크 커넥터 생성 실패: HTTP {response.status_code}")
            print(response.text)
            return 1
    
    except requests.exceptions.RequestException as e:
        print(f"API 요청 중 오류 발생: {e}")
        return 1

if __name__ == "__main__":
    # Execute script
    exit_code = create_connector()
    sys.exit(exit_code)