# ws-stack-dados-k8s

helm repo add apache-airflow https://airflow.apache.org
helm repo add elastic https://helm.elastic.co
helm repo update


### kubernetes cluster
```shell
```

### argocd [gitops]
```shell
```

### minio [deepstorage]
```shell
kubectl apply -f app-manifests/deepstorage/minio-operator.yaml
kubectl apply -f app-manifests/deepstorage/minio-tenant.yaml
```

### hive metastore [metastore]
```shell
kubectl apply -f app-manifests/metastore/hive-metastore.yaml
```

### trino [warehouse]
```shell
kubectl apply -f app-manifests/warehouse/trino.yaml
```

### airflow [orchestrator]
```shell
kubectl apply -f git-credentials-secret.yaml
kubectl apply -f app-manifests/orchestrator/airflow.yaml
```

### efk [logging]
```shell
helm pull elastic/elasticsearch
```