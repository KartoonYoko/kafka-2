# Выполнение 2 практической работы

## Запуск
Для запуска достаточно команды:
```sh
docker compose up -d
```

## Описание поднимаемых сервисов
[Docker compose файл](./docker-compose.yml) содержит [скрипт](./create-topic.sh) для создания топика, который выполняется аналог следующих команд по очереди:
```sh
docker compose exec kafka1 kafka-topics --create --topic my-topic --partitions 3 --replication-factor 2 --bootstrap-server kafka1:29092
docker compose exec kafka1 kafka-topics --describe --topic my-topic --bootstrap-server kafka1:29092
```

После данного скрипта стартует [сервис app](./docker-compose.yml#L92-L130) в двух экземплярах. В каждом экземпляре одновременно работают:
- продюсер
- консьюмер
- пакетный консьюмер, который опирается на настройки "fetch.min.bytes", "fetch.wait.max.ms", чтобы получать ~10 сообщений за раз
- пакетный консьюмер, который опирается на ручной коммит офсета

## Описание приложения 
Приложение может стартовать вышеуказанные стратегии независимо друг от друга. Для это используются фича флаги, которые можно найти [тут](./kafka-task-2/internal/app/config.go). В docker compose все четыре стратегии включены в одном сервисе, а `deploy.replicas: 2` поднимает два экземпляра этого сервиса.

## Посмотреть Kafka UI
Kafka UI оступен по адресу http://localhost:8080/