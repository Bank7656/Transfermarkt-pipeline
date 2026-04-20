AIRFLOW_CONTAINER_PATH := airflow/docker-compose.yaml

start:
	@docker compose -f ${AIRFLOW_CONTAINER_PATH} --env-file ./secrets/.env up -d --build

stop:
	@docker compose -f ${AIRFLOW_CONTAINER_PATH} down

infra-up:
	cd terraform && terraform init && terraform apply
	cd ..

infra-down:
	cd terraform && terraform destroy
	cd ..

restart: stop start

fclean:
	@docker compose -f ${AIRFLOW_CONTAINER_PATH} down --volumes --remove-orphans --rmi all