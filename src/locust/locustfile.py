import json
import random
import time
from locust import HttpUser, task, between

class ItemsAPIUser(HttpUser):
    # 사용자가 작업 사이에 1~5초간 대기
    wait_time = between(1, 5)
    
    # 클래스 변수로 아이템 ID 범위 지정 (사용자 간 공유)
    min_id = 1
    max_id = 5
    created_items = set()
    
    def on_start(self):
        """
        테스트가 시작될 때 실행되는 메서드
        초기 테스트 데이터 설정 (아이템 생성)
        """
        # 동시 생성 충돌을 방지하기 위해 사용자마다 시작 타임스탬프를 다르게 설정
        user_id = int(time.time() * 1000) % 10000
        
        # 테스트용 아이템 생성
        for i in range(1, 6):
            item_data = {
                "name": f"Test Item {user_id}-{i}",
                "description": f"Description for test item {user_id}-{i}",
                "price": i * 1000,
                "on_offer": i % 2 == 0,  # 짝수 번호는 할인 중
            }
            
            with self.client.post("/items", json=item_data, name="Init: Create Item", catch_response=True) as response:
                if response.status_code == 201:
                    try:
                        # 응답에서 생성된 아이템 ID 저장
                        created_item = response.json()
                        if 'id' in created_item:
                            # 클래스 변수에 ID 추가
                            self.__class__.created_items.add(created_item['id'])
                    except json.JSONDecodeError:
                        pass

    @task(3)
    def get_all_items(self):
        """
        모든 아이템 목록 조회 (가중치: 3)
        """
        with self.client.get("/items", name="Get All Items", catch_response=True) as response:
            if response.status_code != 200:
                response.failure(f"Failed to get items: {response.status_code}")

    @task(4)
    def get_single_item(self):
        """
        단일 아이템 조회 (가중치: 4)
        """
        # 생성된 아이템이 있는 경우, 그 중에서 선택
        if self.__class__.created_items:
            item_id = random.choice(list(self.__class__.created_items))
        else:
            # 아직 생성된 아이템이 없으면 기본 범위에서 선택
            item_id = random.randint(self.__class__.min_id, self.__class__.max_id)
        
        with self.client.get(f"/item/{item_id}", name="Get Single Item", catch_response=True) as response:
            if response.status_code != 200:
                # 404 Not Found는 정상적인 결과로 간주 (삭제된 아이템일 수 있음)
                if response.status_code != 404:
                    response.failure(f"Failed to get item {item_id}: {response.status_code}")

    @task(2)
    def create_item(self):
        """
        새 아이템 생성 (가중치: 2)
        """
        # 타임스탬프 기반 유니크한 값 생성
        timestamp = int(time.time() * 1000)
        random_suffix = random.randint(1000, 9999)
        unique_id = f"{timestamp}-{random_suffix}"
        
        item_data = {
            "name": f"New Item {unique_id}",
            "description": f"Description for new item {unique_id}",
            "price": random.randint(500, 10000),
            "on_offer": random.choice([True, False]),
        }
        
        with self.client.post("/items", json=item_data, name="Create Item", catch_response=True) as response:
            if response.status_code == 201:
                try:
                    # 응답에서 생성된 아이템 ID 저장
                    created_item = response.json()
                    if 'id' in created_item:
                        # 클래스 변수에 ID 추가
                        self.__class__.created_items.add(created_item['id'])
                except json.JSONDecodeError:
                    response.failure("Invalid JSON response")
            else:
                response.failure(f"Failed to create item: {response.status_code}")

    @task(2)
    def update_item(self):
        """
        아이템 업데이트 (가중치: 2)
        """
        # 생성된 아이템이 있는 경우, 그 중에서 선택
        if self.__class__.created_items:
            item_id = random.choice(list(self.__class__.created_items))
            
            updated_data = {
                "name": f"Updated Item {item_id}-{int(time.time())}",
                "description": f"Updated description for item {item_id}",
                "price": random.randint(500, 10000),
                "on_offer": random.choice([True, False]),
            }
            
            with self.client.put(f"/item/{item_id}", json=updated_data, name="Update Item", catch_response=True) as response:
                if response.status_code != 200:
                    # 아이템이 존재하지 않으면 set에서 제거
                    if response.status_code == 404:
                        self.__class__.created_items.discard(item_id)
                    response.failure(f"Failed to update item {item_id}: {response.status_code}")

    @task(1)
    def delete_item(self):
        """
        아이템 삭제 (가중치: 1)
        """
        # 생성된 아이템이 있는 경우, 그 중에서 선택
        if self.__class__.created_items:
            item_id = random.choice(list(self.__class__.created_items))
            
            with self.client.delete(f"/item/{item_id}", name="Delete Item", catch_response=True) as response:
                # 아이템이 성공적으로 삭제되었거나 이미 존재하지 않는 경우
                if response.status_code in (200, 404):
                    # 아이템 ID를 set에서 제거
                    self.__class__.created_items.discard(item_id)
                else:
                    response.failure(f"Failed to delete item {item_id}: {response.status_code}")