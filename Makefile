start-kafka:
	docker compose -f 'docker-compose.yml' up -d --build 
	@echo "Kafka started"