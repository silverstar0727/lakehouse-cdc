kubectl apply -f postgres.yaml
kubectl apply -f bucket.yaml

export S3_ACCESS_KEY_B64=$(kubectl get secret -n iceberg iceberg-warehouse-bucket -o jsonpath='{.data.AWS_ACCESS_KEY_ID}')
export S3_SECRET_KEY_B64=$(kubectl get secret -n iceberg iceberg-warehouse-bucket -o jsonpath='{.data.AWS_SECRET_ACCESS_KEY}')


# Update the secret in the catalog.yaml file
kubectl apply -f <(sed -e 's|AWS_ACCESS_KEY_ID:.*|AWS_ACCESS_KEY_ID: '"$S3_ACCESS_KEY_B64"'|' -e 's|AWS_SECRET_ACCESS_KEY:.*|AWS_SECRET_ACCESS_KEY: '"$S3_SECRET_KEY_B64"'|' rest-catalog.yaml)


export EXTERNAL_IP=$(kubectl get svc nginx-ingress-ingress-nginx-controller -n ingress-nginx -o jsonpath='{.status.loadBalancer.ingress[0].ip}')

# test if the iceberg-rest-catalog is running
curl -X POST -H "Content-Type: application/json" \
  http://iceberg-rest-catalog.34.173.195.18.nip.io/v1/namespaces \
  -d '{"namespace": ["example_namespace"]}'

curl -X POST -H "Content-Type: application/json" \
  http://iceberg-rest-catalog.34.173.195.18.nip.io/v1/namespaces/example_namespace/tables \
  -d '{
    "name": "simple_table2",
    "schema": {
      "type": "struct",
      "fields": [
        {"id": 1, "name": "id", "type": "int", "required": true},
        {"id": 2, "name": "name", "type": "string", "required": false}
      ]
    },
    "properties": {
      "write.format.default": "parquet"
    }
  }'