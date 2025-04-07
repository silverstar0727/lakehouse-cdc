

export S3_ACCESS_KEY_B64=$(kubectl -n rook-ceph get secret rook-ceph-object-user-my-store-my-user -o jsonpath='{.data.AccessKey}')
export S3_SECRET_KEY_B64=$(kubectl -n rook-ceph get secret rook-ceph-object-user-my-store-my-user -o jsonpath='{.data.SecretKey}')

kubectl apply -f <(sed -e 's|AWS_ACCESS_KEY_ID:.*|AWS_ACCESS_KEY_ID: '"$S3_ACCESS_KEY_B64"'|' -e 's|AWS_SECRET_ACCESS_KEY:.*|AWS_SECRET_ACCESS_KEY: '"$S3_SECRET_KEY_B64"'|' kafka-deployment.yaml)

export AWS_ACCESS_KEY=$(kubectl get secret -n iceberg iceberg-warehouse-bucket -o jsonpath='{.data.AWS_ACCESS_KEY_ID}' | base64 --decode)
export AWS_SECRET_KEY=$(kubectl get secret -n iceberg iceberg-warehouse-bucket -o jsonpath='{.data.AWS_SECRET_ACCESS_KEY}' | base64 --decode)

python create_source_connector.py
python create_sink_connector.py

curl -X GET http://kafka-connect.$EXTERNAL_IP.nip.io/connectors/iceberg-sink/status

# curl -X POST -H "Content-Type: application/json" \
#   http://iceberg-rest-catalog.$EXTERNAL_IP.nip.io/v1/namespaces \
#   -d '{"namespace": ["fastapi_db"]}'

# curl -X POST -H "Content-Type: application/json" \
#   http://iceberg-rest-catalog.$EXTERNAL_IP.nip.io/v1/namespaces/fastapi_db/tables \
#   -d '{
#     "name": "test-items",
#     "schema": {
#       "type": "struct",
#       "fields": [
#         {"id": 1, "name": "id", "type": "int", "required": true},
#         {"id": 2, "name": "name", "type": "string", "required": false},
#         {"id": 3, "name": "description", "type": "string", "required": false},
#         {"id": 4, "name": "price", "type": "int", "required": false},
#         {"id": 5, "name": "on_offer", "type": "boolean", "required": false},
#         {"id": 6, "name": "_deleted", "type": "string", "required": false}
#       ]
#     },
#     "properties": {
#       "write.format.default": "parquet"
#     }
#   }'