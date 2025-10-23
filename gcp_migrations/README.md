

### Debug docker-compose for Airflow
``` bash
sudo docker compose down
sudo docker builder prune -f
sudo docker compose up -d --build airflow-init
sudo docker compose up -d --build
``` 