```shell
mc alias set orion-minio-dev http://4.153.0.204 data-lake 12620ee6-2162-11ee-be56-0242ac120002
mc ls orion-minio-dev/landing
```

```shell
python cli.py all
python cli.py mssql
python cli.py postgres
python cli.py mongodb
python cli.py redis
```