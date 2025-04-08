
export OBC_ACCESS_KEY=$(kubectl get secret -n iceberg iceberg-warehouse-bucket -o jsonpath='{.data.AWS_ACCESS_KEY_ID}' | base64 --decode)
export OBC_SECRET_KEY=$(kubectl get secret -n iceberg iceberg-warehouse-bucket -o jsonpath='{.data.AWS_SECRET_ACCESS_KEY}' | base64 --decode)
export BOOTSTRAP_SERVER_URL=$(kubectl get service -n strimzi-kafka kraft-cluster-kafka-external-bootstrap -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
export EXTERNAL_IP=$(kubectl get service -n ingress-nginx nginx-ingress-ingress-nginx-controller -o jsonpath='{.status.loadBalancer.ingress[0].ip}')

python delete_kafka_offset.py 
python delete_table.py