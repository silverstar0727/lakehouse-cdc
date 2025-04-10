#!/usr/bin/env python3
"""
Hard-coded PostgreSQL Debezium Connector creation script
"""

import requests
import json
import sys

import os

EXTERNAL_IP = os.getenv("EXTERNAL_IP")

# All configuration settings hard-coded
CONNECT_URL = f"http://kafka-connect.{EXTERNAL_IP}.nip.io"  # Kafka Connect URL
CONNECTOR_CONFIG = {
    "name": "postgres-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "backend-postgres.default.svc.cluster.local",
        "database.port": "5432",
        "database.user": "postgres",
        "database.password": "12341234",
        "database.dbname": "postgres",
        "database.server.name": "backend-postgres-server",
        "topic.prefix": "postgres-connector",
        "plugin.name": "pgoutput",
        "publication.autocreate.mode": "filtered",
        "slot.name": "debezium_slot",
        "table.include.list": "public.items",
        "snapshot.mode": "initial",
        
        # 변환 체인 정의
        "transforms": "unwrap,extractFields",
        
        # Debezium 이벤트 unwrap - 변경된 데이터만 추출
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.drop.tombstones": "false",
        "transforms.unwrap.delete.handling.mode": "rewrite",
        "transforms.unwrap.add.fields": "op,ts_ms", # 작업 유형과 타임스탬프 포함
        
        # 필드 이름 변경
        "transforms.extractFields.type": "org.apache.kafka.connect.transforms.ReplaceField$Value",
        "transforms.extractFields.renames": "__deleted:is_iceberg_deleted,op:operation_type,ts_ms:event_timestamp",
        
        # 성능 및 안정성 설정
        "max.batch.size": "2048",
        "max.queue.size": "8192",
        "poll.interval.ms": "100",
        "heartbeat.interval.ms": "5000",
        
        # 스키마 역사 추적
        # "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
        # "schema.history.internal.kafka.topic": "schema-changes.inventory",
        
        # 헤더 설정
        "tombstones.on.delete": "false"
    }
}

# Connector creation function
def create_connector():
    headers = {"Content-Type": "application/json"}
    
    print(f"PostgreSQL 소스 커넥터를 생성합니다. URL: {CONNECT_URL}")
    
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
            print(f"PostgreSQL 소스 커넥터 생성 성공!")
            print(json.dumps(response.json(), indent=2))
            return 0
        else:
            print(f"PostgreSQL 소스 커넥터 생성 실패: HTTP {response.status_code}")
            print(response.text)
            return 1
    
    except requests.exceptions.RequestException as e:
        print(f"API 요청 중 오류 발생: {e}")
        return 1

if __name__ == "__main__":
    # Execute script
    exit_code = create_connector()
    sys.exit(exit_code)