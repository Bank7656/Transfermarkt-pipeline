AIRFLOW_CONTAINER_PATH := airflow/docker-compose.yaml


start:
	@docker compose -f ${AIRFLOW_CONTAINER_PATH} up

stop:
	@docker compose -f ${AIRFLOW_CONTAINER_PATH} down

restart: stop start

fclean:
	@docker compose -f ${AIRFLOW_CONTAINER_PATH} down --volumes --remove-orphans --rmi all