#!/usr/bin/env python3

import json
import requests
import time
import sys
import os

EXTERNAL_IP = os.getenv("EXTERNAL_IP")
# 환경 설정 (필요에 따라 수정)
KAFKA_CONNECT_URL = f"http://kafka-connect.{EXTERNAL_IP}.nip.io"  # 환경에 맞게 수정
CONNECTOR_NAME = "iceberg-sink"
TOPIC = "postgres-connector.public.items"
CONSUMER_GROUPS = ["connect-iceberg-sink", "cg-control-iceberg-sink"]

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

def reset_offsets():
    """REST API를 통해 컨슈머 그룹 오프셋 초기화"""
    try:
        # Kafka Connect REST API를 통한 오프셋 초기화
        for group in CONSUMER_GROUPS:
            print(f"\n{group} 그룹 오프셋 초기화 중...")
            
            # 그룹 정보 조회
            response = requests.get(f"{KAFKA_CONNECT_URL}/admin/consumers/{group}/offsets")
            if response.status_code == 404:
                print(f"그룹 '{group}'이 존재하지 않습니다. 계속 진행합니다.")
                continue
                
            # 오프셋 초기화 요청
            reset_data = {
                "topics": [{"topic": TOPIC}]
            }
            
            response = requests.delete(
                f"{KAFKA_CONNECT_URL}/admin/consumers/{group}/offsets",
                headers={"Content-Type": "application/json"},
                data=json.dumps(reset_data)
            )
            
            if response.status_code in [200, 204]:
                print(f"{group} 그룹 오프셋 초기화 성공")
            else:
                print(f"{group} 그룹 오프셋 초기화 실패: HTTP {response.status_code}")
                print(response.text)
                
    except Exception as e:
        print(f"오프셋 초기화 중 오류 발생: {e}")

if __name__ == "__main__":
    # 커넥터 삭제
    delete_connector()
    
    # 잠시 대기 (커넥터 삭제 완료까지)
    print("\n커넥터 삭제 완료 대기 중...")
    time.sleep(3)
    
    # 오프셋 초기화
    reset_offsets()
    
    print("\n작업 완료")