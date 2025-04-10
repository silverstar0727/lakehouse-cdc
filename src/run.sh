cd ceph
kubectl create clusterrole rook-endpointslice-role --verb=get,list,watch,create,update,delete --resource=endpointslices.discovery.k8s.io
kubectl create clusterrolebinding rook-endpointslice-binding --clusterrole=rook-endpointslice-role --serviceaccount=rook-ceph:rook-ceph-system

kubectl apply -f crds.yaml -f common.yaml -f operator.yaml
kubectl apply -f cluster-gke.yaml
kubectl apply -f object-gke.yaml

helm install nginx-ingress ingress-nginx/ingress-nginx --namespace ingress-nginx --create-namespace

cd ../service
export IMAGE_REGISTRY=silverstar456
skaffold run --default-repo=$IMAGE_REGISTRY

cd ../ingress
export EXTERNAL_IP=$(kubectl get svc nginx-ingress-ingress-nginx-controller -n ingress-nginx -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
sed "s/\${EXTERNAL_IP}/$EXTERNAL_IP/g" nginx-ingress.yaml | kubectl apply -f -

cd ../locust
skaffold run --default-repo=$IMAGE_REGISTRY


helm install strimzi-kafka-operator strimzi/strimzi-kafka-operator --version 0.44.0 \
  --namespace strimzi-kafka --create-namespace

cd ../kafka
kubectl apply -f strimzi.yaml
kubectl apply -f kafdrop.yaml

cd ../iceberg
kubectl apply -f postgres.yaml
kubectl apply -f bucket.yaml

cd ../kafka/deployment-connector
export S3_ACCESS_KEY_B64=$(kubectl -n rook-ceph get secret rook-ceph-object-user-my-store-my-user -o jsonpath='{.data.AccessKey}')
export S3_SECRET_KEY_B64=$(kubectl -n rook-ceph get secret rook-ceph-object-user-my-store-my-user -o jsonpath='{.data.SecretKey}')

kubectl apply -f <(sed -e 's|AWS_ACCESS_KEY_ID:.*|AWS_ACCESS_KEY_ID: '"$S3_ACCESS_KEY_B64"'|' -e 's|AWS_SECRET_ACCESS_KEY:.*|AWS_SECRET_ACCESS_KEY: '"$S3_SECRET_KEY_B64"'|' kafka-deployment.yaml)


cd ../../iceberg
export S3_ACCESS_KEY_B64=$(kubectl get secret -n iceberg iceberg-warehouse-bucket -o jsonpath='{.data.AWS_ACCESS_KEY_ID}')
export S3_SECRET_KEY_B64=$(kubectl get secret -n iceberg iceberg-warehouse-bucket -o jsonpath='{.data.AWS_SECRET_ACCESS_KEY}')

# Update the secret in the catalog.yaml file
kubectl apply -f <(sed -e 's|AWS_ACCESS_KEY_ID:.*|AWS_ACCESS_KEY_ID: '"$S3_ACCESS_KEY_B64"'|' -e 's|AWS_SECRET_ACCESS_KEY:.*|AWS_SECRET_ACCESS_KEY: '"$S3_SECRET_KEY_B64"'|' rest-catalog.yaml)

cd ../kafka/deployment-connector

sleep 300

export AWS_ACCESS_KEY=$(kubectl get secret -n iceberg iceberg-warehouse-bucket -o jsonpath='{.data.AWS_ACCESS_KEY_ID}' | base64 --decode)
export AWS_SECRET_KEY=$(kubectl get secret -n iceberg iceberg-warehouse-bucket -o jsonpath='{.data.AWS_SECRET_ACCESS_KEY}' | base64 --decode)
export EXTERNAL_IP=$(kubectl get svc nginx-ingress-ingress-nginx-controller -n ingress-nginx -o jsonpath='{.status.loadBalancer.ingress[0].ip}')

python create_source_connector.py
python create_sink_connector.py