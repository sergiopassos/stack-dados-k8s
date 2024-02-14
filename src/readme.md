# ws-stack-dados-k8s

### kubernetes cluster
```shell
```

### argocd [gitops]
```shell
```

### infrastructure
```shell
kubectl apply -f app-manifests/deepstorage/minio-operator.yaml
kubectl apply -f app-manifests/deepstorage/minio-tenant.yaml
kubectl get secret console-sa-secret --namespace deepstorage -o jsonpath="{.data.token}" | base64 --decode
```