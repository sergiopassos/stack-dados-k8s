# docker 
```sh
docker build . -t owshq-apache-airflow:latest
docker tag owshq-apache-airflow:latest owshq/owshq-apache-airflow:latest
docker push owshq/owshq-apache-airflow:latest
```