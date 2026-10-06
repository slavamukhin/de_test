# ЗАДАЧА 1

### Логика ETL
1. Получить текущее значение watermark
  Из хранилища состояния получить `last_watermark`.
  В нашем случае watermark — поле `updated_ts`.
2. Прочитать инкремент из источника
  Выбрать только записи, у которых `updated_ts > last_watermark`.
  Выбирать только поля, указанные в schema.
  Отсортировать записи по `updated_ts`.
  Ограничить количество записей размером `batch_size = 10000`.
3. Подготовить batch к загрузке
  Использовать переданную schema для определения списка полей.
  Для каждого поля учитывать соответствующий тип данных.
  Загрузить полученный batch во временную staging таблицу.
4. Идемпотентно загрузить данные в приёмник
  Использовать event_id как уникальный ключ.
  Если `event_id` ещё отсутствует в приёмнике — выполнить `INSERT`.
  Если `event_id` уже существует — выполнить `UPDATE`.
  Для этого использовать MERGE или аналогичный `UPSERT`.
  Повторная загрузка одного и того же batch не должна создавать дубликаты.
5. Обновить watermark
  После успешной загрузки найти максимальное значение `updated_ts` в обработанном batch.
  Это значение становится новым `last_watermark`.
6. Вернуть результат
  Функция `run_etl_once`() возвращает `new_last_watermark`.
  Если новых записей нет — вернуть прежний `last_watermark`.


```python
from datetime import datetime

from destination_connect import dst_conn
from source_connect import src_conn


# ============================================================
# Входные параметры
# ============================================================

SRC_TABLE = "src_events"
DST_TABLE = "dst_events"

SCHEMA = [
    ("event_id", "bigint"),
    ("event_ts", "timestamp"),
    ("payload", "text"),
    ("updated_ts", "timestamp"),
]

WATERMARK_FIELD = "updated_ts"
BATCH_SIZE = 10000

# Временная таблица для staging.
STAGING_TABLE = "staging_dst_events"


# ============================================================
# Вспомогательные функции
# ============================================================

def quote_sql_value(value):
    """
    Преобразует Python-значение в SQL literal.

    Так как по условию execute() принимает только готовую SQL-строку
    и не предоставляет параметров вида execute(sql, params),
    значения приходится экранировать самостоятельно.

    Для production-кода предпочтительнее использовать
    параметризованные запросы драйвера.
    """

    if value is None:
        return "NULL"

    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"

    if isinstance(value, (int, float)):
        return str(value)

    # Экранируем одинарную кавычку:
    # O'Reilly -> 'O''Reilly'
    value = str(value).replace("'", "''")

    return f"'{value}'"


def build_staging_ddl(staging_table, schema):
    """
    Формирует SQL для создания staging-таблицы
    на основании переданной схемы.

    Например:

    CREATE TEMP TABLE staging_dst_events (
        event_id BIGINT,
        event_ts TIMESTAMP,
        payload TEXT,
        updated_ts TIMESTAMP
    );
    """

    columns = ", ".join(
        f"{field} {field_type.upper()}"
        for field, field_type in schema
    )

    return f"""
        CREATE TEMP TABLE {staging_table} (
            {columns}
        );
    """


# ============================================================
# 1. extract_incremental
# ============================================================

def extract_incremental(
    src_table,
    schema,
    watermark_field,
    last_watermark,
    batch_size,
):
    """
    Читает очередной инкремент из источника.

    Логика:
    1. Берём только поля из schema.
    2. Выбираем записи, которые появились/изменились
       после last_watermark.
    3. Сортируем по watermark.
    4. Ограничиваем batch_size.

    Возвращает:
        List[Dict] | None
    """

    # Получаем список полей из schema.
    fields = ", ".join(
        field
        for field, _ in schema
    )

    # Формируем SQL.
    sql = f"""
        SELECT
            {fields}
        FROM {src_table}
        WHERE {watermark_field} > {quote_sql_value(last_watermark)}
        ORDER BY {watermark_field} ASC, event_id ASC
        LIMIT {batch_size};
    """

    # Выполняем запрос к источнику.
    return src_conn().execute(sql)


# ============================================================
# 2. upsert_batch
# ============================================================

def upsert_batch(
    dst_table,
    schema,
    batch,
    staging_table=STAGING_TABLE,
):
    """
    Идемпотентно загружает batch в приёмник.

    Логика:

    1. Создаём staging-таблицу по schema.
    2. Загружаем batch в staging.
    3. Выполняем MERGE staging -> destination.
    4. event_id используется как уникальный ключ.

    Если event_id отсутствует в destination:
        INSERT

    Если event_id уже существует:
        UPDATE

    Поэтому повторная загрузка одного batch
    не создаёт дубликаты.
    """

    # Если batch пустой, ничего делать не нужно.
    if not batch:
        return

    conn = dst_conn()

    # --------------------------------------------------------
    # Шаг 1. Создаём staging
    # --------------------------------------------------------

    ddl_sql = build_staging_ddl(
        staging_table=staging_table,
        schema=schema,
    )

    conn.execute(ddl_sql)

    # --------------------------------------------------------
    # Шаг 2. Загружаем batch в staging
    # --------------------------------------------------------

    fields = [
        field
        for field, _ in schema
    ]

    columns_sql = ", ".join(fields)

    values_sql = []

    for row in batch:
        values = ", ".join(
            quote_sql_value(row.get(field))
            for field in fields
        )

        values_sql.append(
            f"({values})"
        )

    insert_staging_sql = f"""
        INSERT INTO {staging_table} (
            {columns_sql}
        )
        VALUES
            {", ".join(values_sql)};
    """

    conn.execute(insert_staging_sql)

    # --------------------------------------------------------
    # Шаг 3. MERGE staging -> destination
    # --------------------------------------------------------

    # Все поля кроме event_id обновляем при совпадении.
    update_fields = [
        field
        for field, _ in schema
        if field != "event_id"
    ]

    update_sql = ", ".join(
        f"{field} = src.{field}"
        for field in update_fields
    )

    insert_columns_sql = ", ".join(fields)

    insert_values_sql = ", ".join(
        f"src.{field}"
        for field in fields
    )

    merge_sql = f"""
        MERGE INTO {dst_table} AS dst
        USING {staging_table} AS src
        ON dst.event_id = src.event_id

        WHEN MATCHED
             AND src.updated_ts > dst.updated_ts
        THEN
            UPDATE SET
                {update_sql}

        WHEN NOT MATCHED
        THEN
            INSERT (
                {insert_columns_sql}
            )
            VALUES (
                {insert_values_sql}
            );
    """

    conn.execute(merge_sql)


# ============================================================
# 3. run_etl_once
# ============================================================

def run_etl_once(
    src_table,
    dst_table,
    schema,
    watermark_field,
    batch_size,
    last_watermark,
):
    """
    Выполняет одну итерацию ETL.

    Последовательность:

    1. Получает last_watermark.
    2. Читает batch из источника.
    3. Если batch пустой — возвращает старый watermark.
    4. Загружает batch идемпотентно.
    5. После успешной загрузки вычисляет MAX(watermark).
    6. Возвращает новый watermark.
    """

    # --------------------------------------------------------
    # Шаг 1-2. Читаем очередной batch
    # --------------------------------------------------------

    batch = extract_incremental(
        src_table=src_table,
        schema=schema,
        watermark_field=watermark_field,
        last_watermark=last_watermark,
        batch_size=batch_size,
    )

    # --------------------------------------------------------
    # Если новых записей нет
    # --------------------------------------------------------

    if not batch:
        return last_watermark

    # --------------------------------------------------------
    # Шаг 3. Идемпотентно загружаем batch
    # --------------------------------------------------------

    upsert_batch(
        dst_table=dst_table,
        schema=schema,
        batch=batch,
    )

    # --------------------------------------------------------
    # Шаг 4. Вычисляем новый watermark
    # --------------------------------------------------------

    watermark_values = [
        row[watermark_field]
        for row in batch
        if row.get(watermark_field) is not None
    ]

    # Если в batch нет ни одного значения watermark,
    # оставляем старый watermark.
    if not watermark_values:
        return last_watermark

    new_last_watermark = max(watermark_values)

    return new_last_watermark


# ============================================================
# Пример запуска
# ============================================================

if __name__ == "__main__":

    # В реальном ETL этот watermark обычно читается
    # из отдельной таблицы состояния / metadata table.
    last_watermark = datetime(2026, 10, 1, 0, 0, 0)

    new_last_watermark = run_etl_once(
        src_table=SRC_TABLE,
        dst_table=DST_TABLE,
        schema=SCHEMA,
        watermark_field=WATERMARK_FIELD,
        batch_size=BATCH_SIZE,
        last_watermark=last_watermark,
    )

    print(
        f"ETL completed. "
        f"New watermark: {new_last_watermark}"
    )
```
---

# ЗАДАНИЕ 2

### 1. Логика решения

#### a. Как делаете инкремент / формируете batch

Для инкремента использую `updated_ts`, так как запись может изменяться после создания.

Последний успешно обработанный watermark храню в служебной таблице `etl_state`.

Использую пару `(updated_ts, event_id)`, чтобы корректно обрабатывать записи с одинаковым `updated_ts`.

Размер batch ограничиваю параметром `BATCH_SIZE`.

```sql
SELECT
    event_id,
    event_ts,
    payload,
    updated_ts
FROM src_events
WHERE
    updated_ts > :last_updated_ts
    OR (
        updated_ts = :last_updated_ts
        AND event_id > :last_event_id
    )
ORDER BY updated_ts, event_id
LIMIT :batch_size;
```

Watermark обновляется только после успешной загрузки всего batch и `COMMIT`.

#### b. Как обеспечиваете идемпотентность в MS SQL

В `dst_events` `event_id` является первичным ключом.

Загрузку выполняю через staging-таблицу и `MERGE`:

* если записи с таким `event_id` нет — выполняется `INSERT`;
* если запись уже существует и `updated_ts` источника новее — выполняется `UPDATE`;
* если запись уже существует и данные не новее — ничего не меняется.

Поэтому повторный запуск одного и того же batch безопасен и не приводит к дублированию данных.


#### c. Как реализуете логгирование

Основные параметры каждого batch вывожу в стандартный Airflow log:

* `batch_id`;
* количество прочитанных записей;
* watermark до и после загрузки;
* статус;
* время выполнения;
* текст ошибки при неудаче.

Airflow сохраняет эти логи для каждого запуска и retry.

#### d. Как настроите поведение Airflow при недоступности MS SQL

Для временной недоступности MS SQL использую `retries` и `exponential backoff`.

Например, задаю 6 повторных попыток с максимальной задержкой 30 минут.

При ошибке task завершается с ошибкой и Airflow запускает retry.

Watermark до успешного `COMMIT` не изменяется, поэтому повторная попытка обрабатывает тот же batch.

`max_active_runs=1` не позволяет одновременно выполнять несколько экземпляров DAG.

#### e. Как проверите качество данных

После загрузки проверяю:

* количество записей в полученном batch;
* что все записи batch присутствуют в `dst_events`;
* отсутствие дублей обеспечивается `PRIMARY KEY (event_id)`;
* `event_id` и `updated_ts` не могут быть `NULL`;
* watermark обновился только после успешного batch;
* в логах фиксируется размер batch и новый watermark.

Для контроля отставания загрузки дополнительно можно мониторить максимальный `updated_ts` в источнике и приемнике.

---

### 2. Что будете делать, если недоступность произошла во время загрузки

Загрузку в MS SQL выполняю в одной транзакции:

```text
PostgreSQL
    ↓
batch
    ↓
staging_events
    ↓
MERGE → dst_events
    ↓
update watermark
    ↓
COMMIT
```

Если MS SQL становится недоступен до `COMMIT`, транзакция откатывается целиком, включая загрузку в staging, изменения `dst_events` и watermark.

Airflow выполняет retry и повторно загружает весь batch.

Если `COMMIT` уже произошел, но из-за сетевого сбоя Airflow не получил подтверждение и task завершился ошибкой, retry также повторит весь batch. В этом случае `MERGE` безопасно обработает уже загруженные записи благодаря `event_id` и не создаст дубли.

Таким образом, batch можно безопасно выполнять повторно, а watermark двигается только после успешной фиксации данных.

### 3. Код

```python
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook


POSTGRES_CONN_ID = "postgres_source"
MSSQL_CONN_ID = "mssql_target"

BATCH_SIZE = 10_000


def load_batch():
    """
    PostgreSQL -> MS SQL

    Порядок:
    1. Получаем последний watermark.
    2. Читаем следующий batch из PostgreSQL.
    3. Загружаем batch в staging MS SQL.
    4. Выполняем идемпотентный MERGE.
    5. Проверяем результат.
    6. Обновляем watermark.
    7. Делаем COMMIT.
    """

    pg = PostgresHook(
        postgres_conn_id=POSTGRES_CONN_ID
    )

    mssql = MsSqlHook(
        mssql_conn_id=MSSQL_CONN_ID
    )

    # ---------------------------------------------------------
    # 1. Получаем последний успешно обработанный watermark
    # ---------------------------------------------------------

    state = mssql.get_first(
        """
        SELECT
            last_updated_ts,
            last_event_id
        FROM etl_state
        WHERE pipeline_name = 'src_events_to_dst_events'
        """
    )

    if not state:
        raise RuntimeError(
            "Watermark for pipeline was not found"
        )

    last_updated_ts, last_event_id = state

    print(
        f"Current watermark: "
        f"updated_ts={last_updated_ts}, "
        f"event_id={last_event_id}"
    )

    # ---------------------------------------------------------
    # 2. Получаем следующий batch из PostgreSQL
    # ---------------------------------------------------------

    source_sql = """
        SELECT
            event_id,
            event_ts,
            payload,
            updated_ts
        FROM src_events
        WHERE
            updated_ts > %(last_updated_ts)s
            OR (
                updated_ts = %(last_updated_ts)s
                AND event_id > %(last_event_id)s
            )
        ORDER BY updated_ts, event_id
        LIMIT %(batch_size)s
    """

    rows = pg.get_records(
        source_sql,
        parameters={
            "last_updated_ts": last_updated_ts,
            "last_event_id": last_event_id,
            "batch_size": BATCH_SIZE,
        },
    )

    # Новых данных нет
    if not rows:
        print("No new records")
        return

    print(
        f"Read {len(rows)} records from PostgreSQL"
    )

    # Так как SELECT отсортирован по
    # updated_ts, event_id, последняя запись
    # является новым watermark.
    new_updated_ts = rows[-1][3]
    new_event_id = rows[-1][0]

    batch_id = (
        f"{new_updated_ts}_{new_event_id}"
    )

    print(f"Batch ID: {batch_id}")

    # ---------------------------------------------------------
    # 3. Открываем транзакцию в MS SQL
    # ---------------------------------------------------------

    conn = mssql.get_conn()
    conn.autocommit = False

    cursor = conn.cursor()

    try:

        # -----------------------------------------------------
        # 4. Загружаем batch в staging
        # -----------------------------------------------------

        cursor.execute(
            """
            TRUNCATE TABLE staging_events;
            """
        )

        cursor.executemany(
            """
            INSERT INTO staging_events (
                event_id,
                event_ts,
                payload,
                updated_ts
            )
            VALUES (?, ?, ?, ?)
            """,
            rows,
        )

        # Проверяем, что весь batch попал в staging
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM staging_events
            """
        )

        staging_count = cursor.fetchone()[0]

        if staging_count != len(rows):
            raise RuntimeError(
                f"Staging validation failed: "
                f"expected={len(rows)}, "
                f"actual={staging_count}"
            )

        print(
            f"Staging validation passed: "
            f"{staging_count} rows"
        )

        # -----------------------------------------------------
        # 5. Идемпотентный MERGE в dst_events
        # -----------------------------------------------------

        cursor.execute(
            """
            MERGE dst_events AS target
            USING staging_events AS source
                ON target.event_id = source.event_id

            WHEN MATCHED
                AND source.updated_ts > target.updated_ts
            THEN
                UPDATE SET
                    target.event_ts = source.event_ts,
                    target.payload = source.payload,
                    target.updated_ts = source.updated_ts

            WHEN NOT MATCHED BY TARGET
            THEN
                INSERT (
                    event_id,
                    event_ts,
                    payload,
                    updated_ts
                )
                VALUES (
                    source.event_id,
                    source.event_ts,
                    source.payload,
                    source.updated_ts
                );
            """
        )

        # -----------------------------------------------------
        # 6. Проверяем, что все записи batch есть в target
        # -----------------------------------------------------

        cursor.execute(
            """
            SELECT COUNT(*)
            FROM staging_events s
            INNER JOIN dst_events d
                ON d.event_id = s.event_id
            """
        )

        target_count = cursor.fetchone()[0]

        if target_count != len(rows):
            raise RuntimeError(
                f"Target validation failed: "
                f"expected={len(rows)}, "
                f"actual={target_count}"
            )

        print(
            f"Target validation passed: "
            f"{target_count} rows"
        )

        # -----------------------------------------------------
        # 7. Обновляем watermark
        #
        # Важно: до этого момента watermark
        # не изменяется.
        # -----------------------------------------------------

        cursor.execute(
            """
            UPDATE etl_state
            SET
                last_updated_ts = ?,
                last_event_id = ?
            WHERE pipeline_name = 'src_events_to_dst_events'
            """,
            new_updated_ts,
            new_event_id,
        )

        # -----------------------------------------------------
        # 8. Фиксируем всю транзакцию
        # -----------------------------------------------------

        conn.commit()

        print(
            f"Batch successfully loaded. "
            f"batch_id={batch_id}, "
            f"rows={len(rows)}, "
            f"new_watermark="
            f"({new_updated_ts}, {new_event_id})"
        )

    except Exception as error:

        # Любая ошибка до COMMIT:
        # staging, MERGE и watermark откатываются.
        conn.rollback()

        print(
            f"Batch failed. "
            f"batch_id={batch_id}. "
            f"Transaction rolled back. "
            f"Error: {error}"
        )

        # Передаем ошибку Airflow,
        # чтобы сработал retry.
        raise

    finally:

        cursor.close()
        conn.close()


# =============================================================
# Airflow DAG
# =============================================================

default_args = {
    "owner": "data-engineering",

    # Повторяем task при временной ошибке
    "retries": 6,

    # Увеличиваем интервал между retry
    "retry_exponential_backoff": True,

    # Максимальная задержка между retry
    "max_retry_delay": timedelta(minutes=30),

    # Начальная задержка
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="postgres_to_mssql_events",

    # Запуск каждые 20 минут
    schedule="*/20 * * * *",

    start_date=datetime(2026, 1, 1),

    # Не запускаем старые пропущенные интервалы.
    # Watermark сам определит, какие данные еще не загружены.
    catchup=False,

    # Не допускаем параллельный запуск двух экземпляров DAG
    max_active_runs=1,

    default_args=default_args,

    tags=[
        "etl",
        "postgres",
        "mssql",
    ],
) as dag:

    load_events = PythonOperator(
        task_id="load_events",
        python_callable=load_batch,
    )
```

### Таблицы в MS SQL

#### Целевая таблица

```sql
CREATE TABLE dst_events (
    event_id BIGINT NOT NULL PRIMARY KEY,
    event_ts DATETIME2 NOT NULL,
    payload NVARCHAR(MAX),
    updated_ts DATETIME2 NOT NULL
);
```

#### Staging

```sql
CREATE TABLE staging_events (
    event_id BIGINT NOT NULL,
    event_ts DATETIME2 NOT NULL,
    payload NVARCHAR(MAX),
    updated_ts DATETIME2 NOT NULL
);
```

#### Таблица watermark

```sql
CREATE TABLE etl_state (
    pipeline_name VARCHAR(100) PRIMARY KEY,
    last_updated_ts DATETIME2 NOT NULL,
    last_event_id BIGINT NOT NULL
);
```

#### Начальное состояние

```sql
INSERT INTO etl_state (
    pipeline_name,
    last_updated_ts,
    last_event_id
)
VALUES (
    'src_events_to_dst_events',
    '2000-01-01 00:00:00',
    0
);
```
---

# ЗАДАЧА 3
### Логика решения

1. Сначала нумеруем клиентов детерминированно с помощью `ROW_NUMBER() OVER (ORDER BY client_id)`.
2. По полученному номеру назначаем `split_id` от 1 до 6 по кругу: `((row_num - 1) % 6) + 1`.
3. Благодаря сортировке по уникальному `client_id` результат не зависит от случайности и при одинаковых входных данных будет одинаковым.
4. Поле `is_contact` формируем на основании `split_id`: для `1, 4, 6` устанавливаем `true`, для `2, 3, 5` — `false`.
5. После формирования групп сортируем клиентов внутри каждой группы по `client_score` по убыванию.
6. В качестве дополнительного ключа сортировки используем `client_id`, чтобы порядок был детерминированным и при одинаковом `client_score`.
7. Каждый клиент получает ровно один `split_id`, а количество клиентов в split'ах отличается не более чем на одну строку.

### SQL

```sql
WITH numbered AS (
    SELECT
        client_id,
        client_score,
        time_zone,

        ROW_NUMBER() OVER (
            ORDER BY client_id
        ) AS row_num

    FROM clients
),

assigned AS (
    SELECT
        client_id,
        client_score,
        time_zone,

        ((row_num - 1) % 6) + 1 AS split_id

    FROM numbered
)

SELECT
    client_id,
    client_score,
    time_zone,
    split_id,

    CASE
        WHEN split_id IN (1, 4, 6)
            THEN TRUE
        ELSE FALSE
    END AS is_contact

FROM assigned

ORDER BY
    is_contact,
    client_score DESC,
    client_id;
```
---

# ЗАДАЧА 4
### Подход

Для каждого `time_zone` формирую независимую детерминированную очередь клиентов.

1. Клиентов внутри каждого `time_zone` сортирую по `client_score DESC`, а при одинаковом score — по `client_id`.
2. С помощью `ROW_NUMBER() OVER (PARTITION BY time_zone ORDER BY client_score DESC, client_id)` получаю стабильный порядковый номер клиента.
3. Распределяю клиентов по часовым слотам циклически: `((row_number - 1) % 6) + 1`.
4. В каждом часовом запуске DAG выбирает клиентов соответствующего часового слота.
5. Самые приоритетные клиенты получают первые слоты, поэтому высокий `client_score` в среднем приводит к более ранней выдаче.
6. Дополнительная сортировка по `client_id` делает результат полностью детерминированным.
7. Для каждого `time_zone` распределение выполняется независимо, поэтому наличие большого количества клиентов одного timezone не влияет на другие timezone.
8. Если в timezone не менее 6 клиентов, каждый из шести часовых слотов гарантированно содержит хотя бы одного клиента.
9. Если клиентов меньше 6, обеспечить непустой каждый час без повторной выдачи невозможно. Если повторная выдача допустима, используется циклическая очередь с сохранением текущей позиции для каждого `time_zone`.
10. При каждом новом запуске позиция очереди и правила сортировки должны быть воспроизводимыми, а состояние ротации храниться в отдельной таблице состояния DAG.

### Пример SQL

```sql
WITH ranked AS (
    SELECT
        client_id,
        client_score,
        time_zone,

        ROW_NUMBER() OVER (
            PARTITION BY time_zone
            ORDER BY
                client_score DESC,
                client_id
        ) AS rn

    FROM clients
),

scheduled AS (
    SELECT
        client_id,
        client_score,
        time_zone,

        ((rn - 1) % 6) + 1 AS hour_slot

    FROM ranked
)

SELECT
    client_id,
    client_score,
    time_zone,
    hour_slot
FROM scheduled
WHERE hour_slot = :current_hour_slot
ORDER BY
    time_zone,
    client_score DESC,
    client_id;
```

Таким образом, назначение детерминировано, распределение внутри `time_zone` равномерное, а приоритет по `client_score` сохраняется в среднем. Для timezone с количеством клиентов меньше числа часовых слотов необходимо либо разрешить повторную выдачу клиентов, либо явно зафиксировать это как ограничение бизнес-требования.
