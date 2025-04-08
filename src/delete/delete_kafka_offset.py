#!/usr/bin/env python3

import json
import requests
import time
import sys
import os

EXTERNAL_IP = os.getenv("EXTERNAL_IP")
BOOTSTRAP_SERVER_URL = os.getenv("BOOTSTRAP_SERVER_URL")
# 환경 설정 (필요에 따라 수정)
KAFKA_CONNECT_URL = f"http://kafka-connect.{EXTERNAL_IP}.nip.io"  # 환경에 맞게 수정
CONNECTOR_NAME = "iceberg-sink"
TOPIC = "postgres-connector.public.items"
CONSUMER_GROUPS = ["connect-iceberg-sink", "cg-control-iceberg-sink", "spark-iceberg-consumer"]

def delete_connector():
    """커넥터 삭제"""
    try:
        print(f"커넥터 '{CONNECTOR_NAME}' 삭제 중...")
        response = requests.delete(f"{KAFKA_CONNECT_URL}/connectors/{CONNECTOR_NAME}")
        if response.status_code in [204, 200]:
            print(f"커넥터 '{CONNECTOR_NAME}' 삭제 성공")
        elif response.status_code == 404:
            print(f"커넥터 '{CONNECTOR_NAME}'가 존재하지 않습니다.")
        else:
            print(f"커넥터 삭제 실패: HTTP {response.status_code}")
            print(response.text)
    except Exception as e:
        print(f"커넥터 삭제 중 오류 발생: {e}")
        
# 컨슈머 그룹 삭제 함수
def delete_consumer_group():
    try:
        # 관리자 클라이언트 설정
        from confluent_kafka.admin import AdminClient
        admin_config = {
            'bootstrap.servers': BOOTSTRAP_SERVER_URL + ":9094"
        }
        admin_client = AdminClient(admin_config)
        
        for group in CONSUMER_GROUPS:
            print(f"\n{group} 그룹 삭제 중...")
            result = admin_client.delete_consumer_groups([group])
            
            for group_name, future in result.items():
                try:
                    future.result()  # 결과 확인
                    print(f"{group_name} 그룹 삭제 성공")
                except Exception as e:
                    print(f"{group_name} 그룹 삭제 실패: {e}")
    
    except Exception as e:
        print(f"컨슈머 그룹 삭제 중 오류 발생: {e}")

if __name__ == "__main__":
    # 커넥터 삭제
    delete_connector()
    
    # 잠시 대기 (커넥터 삭제 완료까지)
    print("\n커넥터 삭제 완료 대기 중...")
    time.sleep(3)
    
    # 오프셋 초기화
    delete_consumer_group()
    
    print("\n작업 완료")