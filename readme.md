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

После данного скрипта стартуют 4 сервиса:
- [Продюсер](./docker-compose.yml#L105-L120)
- [Косьюмер](./docker-compose.yml#L122-L133)
- [Пакетный косьюмер](./docker-compose.yml#L135-L146), который опирается на настройки "fetch.min.bytes", "fetch.wait.max.ms", чтобы полчать ~10 сообщений за раз
- [Пакетный косьюмер](./docker-compose.yml#L148-L161), который опирается на ручной коммит офсета

## Описание приложения 
Приложение может стартовать вышеуказанные стратегии независимо друг от друга. Для это используются фича флаги, которые можно найти [тут](./kafka-task-2/internal/app/config.go). Для удобства отслеживания логов в docker compose 4 стратегии запущены отдельно.

## Посмотреть Kafka UI
Kafka UI оступен по адресу http://localhost:8080/