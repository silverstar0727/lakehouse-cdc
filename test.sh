
# 테이블 생성 - 수정된 스키마
echo -e "\n=== itemsdemo2 테이블 생성 시도 ===\n"
TABLE_SCHEMA='{
  "name": "itemsdemo2",
  "schema": {
    "type": "struct",
    "fields": [
      {"id": 1, "name": "id", "type": "int", "required": true},
      {"id": 2, "name": "title", "type": "string", "required": false},
      {"id": 3, "name": "description", "type": "string", "required": false},
      {"id": 4, "name": "owner_id", "type": "int", "required": false}
    ]
  },
  "properties": {
    "write.metadata.compression-codec": "gzip",
    "write.metadata.delete-after-commit.enabled": "true"
  },
  "format-version": 2
}'

echo "테이블 스키마:"
echo "$TABLE_SCHEMA"

abc=$(curl -s -X POST \
  -H "Content-Type: application/json" \
  -d "$TABLE_SCHEMA" \
  "${CATALOG_URL}/v1/namespaces/fastapi_db/tables")

TABLE_STATUS=$?

echo "테이블 생성 응답:"
echo "$abc"