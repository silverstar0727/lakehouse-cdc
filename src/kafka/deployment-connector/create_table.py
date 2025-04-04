#!/usr/bin/env python3
"""
테이블 생성 스크립트 - 수정된 스키마
"""

import requests
import json
import sys
import os

# 환경 변수에서 CATALOG_URL 가져오기
CATALOG_URL = os.getenv("CATALOG_URL", "http://iceberg-rest-catalog.35.226.15.228.nip.io")
if not CATALOG_URL:
    print("Error: CATALOG_URL 환경 변수가 설정되지 않았습니다.")
    sys.exit(1)

def create_table():
    print("\n=== itemsdemo3 테이블 생성 시도 ===\n")
    
    # 테이블 스키마 정의
    TABLE_SCHEMA = {
        "name": "itemsdemo3",
        "schema": {
            "type": "struct",
            "fields": [
                {"id": 1, "name": "id", "type": "int", "required": True},
                {"id": 2, "name": "title", "type": "string", "required": False},
                {"id": 3, "name": "description", "type": "string", "required": False},
                {"id": 4, "name": "owner_id", "type": "int", "required": False}
            ]
        },
        "properties": {
            "write.metadata.compression-codec": "gzip",
            "write.metadata.delete-after-commit.enabled": "true"
        },
        "format-version": 2
    }

    headers = {"Content-Type": "application/json"}
    
    print("테이블 스키마:")
    print(json.dumps(TABLE_SCHEMA, indent=2))
    
    try:
        # 테이블 생성 요청
        response = requests.post(
            f"{CATALOG_URL}/v1/namespaces/fastapi_db/tables",
            headers=headers,
            data=json.dumps(TABLE_SCHEMA)
        )
        
        # 응답 확인
        print("테이블 생성 응답:")
        print(response.text)
        
        if response.status_code == 200 or response.status_code == 201:
            print(f"테이블 생성 성공! 상태 코드: {response.status_code}")
            return 0
        else:
            print(f"테이블 생성 실패: HTTP {response.status_code}")
            return 1
    
    except requests.exceptions.RequestException as e:
        print(f"API 요청 중 오류 발생: {e}")
        return 1

if __name__ == "__main__":
    # 스크립트 실행
    exit_code = create_table()
    sys.exit(exit_code)