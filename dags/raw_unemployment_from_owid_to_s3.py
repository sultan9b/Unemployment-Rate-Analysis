import logging

import duckdb
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

# Конфигурация DAG
OWNER = "sonador"
DAG_ID = "raw_unemployment_from_owid_to_s3"

# Используемые таблицы в DAG
LAYER = "raw"
SOURCE = "unemployment_owid"

# S3 / MinIO
ACCESS_KEY = Variable.get("access_key")
SECRET_KEY = Variable.get("secret_key")

# Конфигурация датасета Our World in Data
OWID_DATASET = "unemployment-rate"

# Страны для загрузки (можно добавить любые, разделяя ~)
# Примеры: USA, CHN, IND, RUS, GBR, DEU, FRA, JPN, BRA, KAZ
COUNTRIES = "USA~CHN~IND~RUS~GBR~DEU~FRA~JPN~BRA~KAZ"

LONG_DESCRIPTION = """
# Unemployment Rate Data Pipeline

Этот DAG загружает данные об уровне безработицы из Our World in Data 
и сохраняет их в MinIO/S3 в формате Parquet.

## Источник данных
- Источник: Our World in Data
- Датасет: Unemployment Rate
- Первоисточник: International Labour Organization (ILO)
- Страны: США, Китай, Индия, Россия, Великобритания, Германия, Франция, Япония, Бразилия, Казахстан

## Поля данных
- Entity: название страны
- Code: код страны (ISO)
- Year: год
- Unemployment rate (%): уровень безработицы в процентах

## Выходные данные
- Формат: Parquet (сжатие GZIP)
- Расположение: s3://prod/raw/unemployment_owid/YYYY-MM-DD/

## Преимущества
- ✅ Полностью бесплатно
- ✅ Без API ключа
- ✅ Без лимитов запросов
- ✅ Качественные данные от ILO
- ✅ Исторические данные с 1991 года
"""

SHORT_DESCRIPTION = "Загрузка данных об уровне безработицы из Our World in Data в S3"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(1991, 1, 1, tz="Asia/Almaty"),
    "catchup": False,  # Не нужен catchup - всегда загружаем полную историю
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}


def get_dates(**context) -> tuple[str, str]:
    """Получить даты из контекста Airflow"""
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")
    return start_date, end_date


def get_and_transfer_api_data_to_s3(**context):
    """
    Загрузить данные об уровне безработицы из Our World in Data CSV API
    и сохранить в MinIO/S3 в формате Parquet
    """

    start_date, end_date = get_dates(**context)
    logging.info(f"💻 Start load for date: {start_date}")

    # Построить URL для Our World in Data CSV API
    csv_url = (
        f"https://ourworldindata.org/grapher/{OWID_DATASET}.csv"
        f"?country={COUNTRIES}"
    )

    logging.info(f"📡 Fetching unemployment data from Our World in Data")
    logging.info(f"🔗 CSV URL: {csv_url}")

    # Подключиться к DuckDB
    con = duckdb.connect()

    try:
        # Путь в S3
        s3_path = f"s3://prod/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet"

        logging.info(f"📤 Processing and uploading data to S3: {s3_path}")

        # DuckDB читает CSV из OWID и сохраняет в S3
        con.sql(f"""
            SET TIMEZONE='UTC';
            INSTALL httpfs;
            LOAD httpfs;
            SET s3_url_style = 'path';
            SET s3_endpoint = 'minio:9000';
            SET s3_access_key_id = '{ACCESS_KEY}';
            SET s3_secret_access_key = '{SECRET_KEY}';
            SET s3_use_ssl = FALSE;

            COPY (
                SELECT
                    '{start_date}' as load_date,
                    *
                FROM read_csv_auto('{csv_url}')
            ) TO '{s3_path}'
            (FORMAT PARQUET, COMPRESSION GZIP);
        """)

        logging.info(f"✅ Data successfully saved to S3: {s3_path}")

        # Показать статистику загруженных данных
        result = con.sql(f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(DISTINCT Entity) as countries,
                MIN(Year) as min_year,
                MAX(Year) as max_year,
                ROUND(AVG("Unemployment, total (% of total labor force) (modeled ILO estimate)"), 2) as avg_unemployment
            FROM read_csv_auto('{csv_url}')
        """).fetchone()

        logging.info(f"📊 Unemployment Data Statistics:")
        logging.info(f"   Total rows: {result[0]}")
        logging.info(f"   Countries: {result[1]}")
        logging.info(f"   Year range: {result[2]} - {result[3]}")
        logging.info(f"   Average unemployment rate: {result[4]}%")

        # Показать последние данные по каждой стране
        latest_data = con.sql(f"""
            SELECT 
                Entity,
                Year,
                ROUND("Unemployment, total (% of total labor force) (modeled ILO estimate)", 2) as unemployment_rate
            FROM read_csv_auto('{csv_url}')
            WHERE Year = (SELECT MAX(Year) FROM read_csv_auto('{csv_url}'))
            ORDER BY unemployment_rate DESC
        """).fetchall()

        logging.info(f"📈 Latest unemployment rates:")
        for row in latest_data:
            logging.info(f"   {row[0]}: {row[2]}% ({row[1]})")

    except Exception as e:
        logging.error(f"❌ Error processing data: {str(e)}")
        raise
    finally:
        con.close()

    logging.info(f"✅ Download for date success: {start_date}")


# Определение DAG
with DAG(
        dag_id=DAG_ID,
        schedule_interval="@monthly",  # Раз в месяц (данные обновляются редко)
        default_args=args,
        tags=["s3", "raw", "unemployment", "owid", "ilo", "economics"],
        description=SHORT_DESCRIPTION,
        concurrency=1,
        max_active_tasks=1,
        max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id="start",
    )

    get_and_transfer_api_data_to_s3_task = PythonOperator(
        task_id="get_and_transfer_api_data_to_s3",
        python_callable=get_and_transfer_api_data_to_s3,
    )

    end = EmptyOperator(
        task_id="end",
    )

    start >> get_and_transfer_api_data_to_s3_task >> end