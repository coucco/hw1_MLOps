# Real-Time Fraud Detection System

## Формат сообщения в топике scoring

```
{
  "transaction_id": "d6b0f7a0-8e1a-4a3c-9b2d-5c8f9d1e2f3a",
  "score": 0.995,
  "fraud_flag": 1
}
```

## Требования

- Docker 20.10+
- Docker Compose 2.0+
- свободные порты 2181, 8080, 8501 и 9095

1. клонируйте репозиторий:
```bash
   git clone git@github.com:coucco/hw1_MLOps.git
   cd hw1_MLOps
```

2. Убедитесь, что обучающая выборка лежит в `fraud_detector/train_data/train.csv.gz`. Она нужна препроцессингу при старте сервиса и входит в репозиторий в сжатом виде. Если файла нет, скачайте `train.csv` со страницы соревнования, сожмите его и положите по этому пути:

   ```bash
   gzip -k train.csv
   mv train.csv.gz fraud_detector/train_data/
   ```

3. Соберите и запустите контейнеры:

   ```bash
   docker compose up --build
   ```

   Первая сборка занимает несколько минут

4. Дождитесь готовности `fraud_detector`. В его логах должна появиться строка `Starting Kafka ML scoring service...`:

   ```bash
   docker compose logs -f fraud_detector
   ```

   Загрузка и обработка `train.csv` при старте занимает от нескольких секунд до пары минут

5. Проверьте, что все контейнеры работают:

   ```bash
   docker compose ps
   ```

   Контейнер `kafka-setup` после создания топиков завершается со статусом `Exited (0)`, а остальные должны быть в статусе `running`

## Проверка работоспособности

### 1. Отправка транзакций

1. Откройте http://localhost:8501
2. Загрузите CSV-файл формата `test.csv`. Для быстрой проверки используйте первые 100-200 строк, на большом файле обработка идёт дольше, так как каждая транзакция скорится отдельно
3. Нажмите кнопку `Отправить <имя файла>`

### 2. Сообщения в Kafka

1. Откройте http://localhost:8080 (Kafka UI), раздел Topics
2. В топике `transactions` должны быть отправленные транзакции
3. В топике `scoring` должны появляться сообщения со скором и флагом фрода

### 3. Данные в PostgreSQL

```bash
docker compose exec postgres psql -U fraud -d fraud -c "SELECT count(*) FROM scores;"
docker compose exec postgres psql -U fraud -d fraud -c "SELECT * FROM scores ORDER BY id DESC LIMIT 5;"
```

Количество строк в `scores` должно совпадать с числом обработанных транзакций

### 4. Просмотр результатов в интерфейсе

1. На странице http://localhost:8501 прокрутите вниз до раздела "Результаты скоринга"
2. Нажмите "Посмотреть результаты"
3. Должны появиться:
   - таблица с 10 последними транзакциями с `fraud_flag = 1` (если таких нет, будет информационное сообщение)
   - гистограмма распределения скоров последних 100 транзакций (если в базе меньше 100 записей, строится по имеющимся)

Модель использует порог 0.98, поэтому на небольшой выборке фродовых транзакций может не быть. Для проверки таблицы отправьте больше данных

## Логи

```bash
docker compose logs <сервис>
```

Имена сервисов: `fraud_detector`, `db_writer`, `interface`, `postgres`, `kafka`, `kafka-setup`.

Лог скоринга также пишется в файл `/app/logs/service.log` внутри контейнера `fraud_detector`

## Остановка

```bash
docker compose down
```

Чтобы дополнительно удалить данные PostgreSQL:

```bash
docker compose down -v
```