# # #!/usr/bin/env python3
# # import boto3
# # import os
# # import sys
# # from botocore.exceptions import ClientError

# # def list_ceph_buckets():
# #     """
# #     Ceph S3 호환 스토리지에 접근하여 버킷 리스트를 조회합니다.
# #     환경 변수에서 필요한 인증 정보와 엔드포인트를 가져옵니다.
# #     """
# #     # 환경 변수에서 필요한 정보 가져오기
# #     access_key = os.getenv("OBC_ACCESS_KEY")
# #     secret_key = os.getenv("OBC_SECRET_KEY")
# #     external_ip = os.getenv("EXTERNAL_IP")
    
# #     # 필수 환경 변수 확인
# #     if not access_key or not secret_key:
# #         print("Error: OBC_ACCESS_KEY 또는 OBC_SECRET_KEY 환경 변수가 설정되지 않았습니다.")
# #         sys.exit(1)
    
# #     # EXTERNAL_IP가 설정되지 않은 경우 직접 입력 요청
# #     if not external_ip:
# #         print("Warning: EXTERNAL_IP 환경 변수가 설정되지 않았습니다.")
# #         external_ip = input("EXTERNAL_IP를 입력하세요: ")
# #         if not external_ip:
# #             print("Error: EXTERNAL_IP가 필요합니다.")
# #             sys.exit(1)
    
# #     # Ceph S3 엔드포인트 URL 구성
# #     endpoint_url = f"http://ceph.{external_ip}.nip.io"
# #     print(f"Ceph S3 엔드포인트에 접속: {endpoint_url}")
    
# #     try:
# #         # S3 클라이언트 생성
# #         s3_client = boto3.client(
# #             's3',
# #             endpoint_url=endpoint_url,
# #             aws_access_key_id=access_key,
# #             aws_secret_access_key=secret_key,
# #             region_name='us-east-1'  # 리전은 Ceph에서는 중요하지 않지만 필요한 파라미터
# #         )
        
# #         # 버킷 리스트 가져오기
# #         response = s3_client.list_buckets()
        
# #         # 결과 출력
# #         if 'Buckets' in response:
# #             buckets = response['Buckets']
# #             if buckets:
# #                 print("\n== 버킷 리스트 ==")
# #                 for bucket in buckets:
# #                     print(f"- {bucket['Name']} (생성일: {bucket['CreationDate']})")
                
# #                     first_bucket = bucket['Name']
# #                     print(f"\n== '{first_bucket}' 버킷 내용 ==")
# #                     objects = s3_client.list_objects_v2(Bucket=first_bucket)
# #                     if 'Contents' in objects:
# #                         for obj in objects['Contents']:
# #                             print(f"- {obj['Key']} (크기: {obj['Size']} bytes, 수정일: {obj['LastModified']})")
# #                     else:
# #                         print(f"'{first_bucket}' 버킷이 비어 있습니다.")
# #             else:
# #                 print("버킷이 없습니다.")
# #         else:
# #             print("버킷 정보를 가져올 수 없습니다.")
            
# #     except ClientError as e:
# #         print(f"Error: Ceph S3 접근 중 오류 발생: {e}")
# #         sys.exit(1)
# #     except Exception as e:
# #         print(f"Error: 예상치 못한 오류 발생: {e}")
# #         sys.exit(1)

# # if __name__ == "__main__":
# #     list_ceph_buckets()


# #!/usr/bin/env python3
# import requests
# import json
# import sys
# import os

# # 환경 변수에서 필요한 정보 가져오기
# EXTERNAL_IP = os.getenv("EXTERNAL_IP")
# if not EXTERNAL_IP:
#     print("Error: EXTERNAL_IP 환경 변수가 설정되지 않았습니다.")
#     sys.exit(1)

# # Iceberg REST 카탈로그 URL
# CATALOG_URL = f"http://iceberg-rest-catalog.{EXTERNAL_IP}.nip.io"

# def check_and_create_namespace():
#     headers = {"Content-Type": "application/json"}
    
#     print(f"Iceberg REST 카탈로그에 접속: {CATALOG_URL}")
    
#     # 1. 먼저 네임스페이스 목록 확인
#     try:
#         print("모든 네임스페이스 목록 조회 중...")
#         response = requests.get(f"{CATALOG_URL}/v1/namespaces")
        
#         if response.status_code == 200:
#             namespaces = response.json()
#             print(f"네임스페이스 목록: {json.dumps(namespaces, indent=2)}")
            
#             # fastapi_db 네임스페이스가 있는지 확인
#             fastapi_db_exists = False
#             if "namespaces" in namespaces:
#                 for ns in namespaces["namespaces"]:
#                     if len(ns) == 1 and ns[0] == "fastapi_db":
#                         fastapi_db_exists = True
#                         print("'fastapi_db' 네임스페이스가 이미 존재합니다.")
#                         break
            
#             if not fastapi_db_exists:
#                 print("'fastapi_db' 네임스페이스가 존재하지 않습니다. 생성을 시도합니다.")
                
#                 # 2. fastapi_db 네임스페이스 생성
#                 create_response = requests.post(
#                     f"{CATALOG_URL}/v1/namespaces",
#                     headers=headers,
#                     data=json.dumps({"namespace": ["fastapi_db"]})
#                 )
                
#                 if create_response.status_code in [200, 201]:
#                     print("'fastapi_db' 네임스페이스 생성 성공!")
#                 else:
#                     print(f"'fastapi_db' 네임스페이스 생성 실패: HTTP {create_response.status_code}")
#                     print(create_response.text)
#         else:
#             print(f"네임스페이스 목록 조회 실패: HTTP {response.status_code}")
#             print(response.text)
    
#     except requests.exceptions.RequestException as e:
#         print(f"API 요청 중 오류 발생: {e}")
#         return False
    
#     # 3. 테이블 목록 확인 (fastapi_db 네임스페이스 내)
#     try:
#         print("\n'fastapi_db' 네임스페이스의 테이블 목록 조회 중...")
        
#         tables_response = requests.get(f"{CATALOG_URL}/v1/namespaces/fastapi_db/tables")
        
#         if tables_response.status_code == 200:
#             tables = tables_response.json()
#             print(f"테이블 목록: {json.dumps(tables, indent=2)}")
            
#             # 테이블 존재 여부에 따라 새 커넥터 생성 방법 안내
#             if "identifiers" in tables and len(tables["identifiers"]) > 0:
#                 print("\n테이블이 이미 존재합니다. 커넥터 설정 시 다음 사항을 확인하세요:")
#                 print("1. 이미 존재하는 테이블을 사용하려면 'iceberg.table.create-mode'를 'create-or-keep'으로 설정하세요.")
#                 print("2. 테이블을 교체하려면 'iceberg.table.create-mode'를 'create-or-replace'로 설정하세요.")
#                 print("3. 완전히 새로운 테이블명을 사용하세요 (예: 'fastapi_db.itemsdemo').")
#             else:
#                 print("\n해당 네임스페이스에 테이블이 없습니다. 새 테이블을 자동 생성하도록 설정되어 있다면, 문제없이 생성될 것입니다.")
#                 print("커넥터 설정에서 다음을 확인하세요:")
#                 print("1. iceberg.table.auto-create: true")
#                 print("2. iceberg.table.create-mode: create-or-replace")
#         else:
#             print(f"테이블 목록 조회 실패: HTTP {tables_response.status_code}")
#             print(tables_response.text)
    
#     except requests.exceptions.RequestException as e:
#         print(f"API 요청 중 오류 발생: {e}")
#         return False
    
#     return True

# if __name__ == "__main__":
#     if check_and_create_namespace():
#         print("\n네임스페이스 확인 및 생성 작업이 완료되었습니다.")
#         print("이제 Iceberg 싱크 커넥터를 생성해보세요.")
#     else:
#         print("\n네임스페이스 확인 중 오류가 발생했습니다.")
#         sys.exit(1)


#!/usr/bin/env python3
"""
Iceberg 테이블 목록 조회 스크립트
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

def list_tables():
    print("\n=== fastapi_db 네임스페이스의 테이블 목록 조회 ===\n")
    
    headers = {"Content-Type": "application/json"}
    
    try:
        # 테이블 목록 조회 요청
        response = requests.get(
            f"{CATALOG_URL}/v1/namespaces/fastapi_db/tables",
            headers=headers
        )
        
        # 응답 확인
        print("테이블 목록 응답:")
        if response.status_code == 200:
            tables = response.json()
            print(json.dumps(tables, indent=2))
            print(f"테이블 수: {len(tables['identifiers']) if 'identifiers' in tables else 0}")
            return 0
        else:
            print(f"테이블 목록 조회 실패: HTTP {response.status_code}")
            print(response.text)
            return 1
    
    except requests.exceptions.RequestException as e:
        print(f"API 요청 중 오류 발생: {e}")
        return 1

if __name__ == "__main__":
    # 스크립트 실행
    exit_code = list_tables()
    sys.exit(exit_code)