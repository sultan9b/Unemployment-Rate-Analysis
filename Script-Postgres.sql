/*создаем схемы*/
create schema stg;
create schema ods;
create schema dm;
 
/*создаем таблицу с ощищенным данными*/
CREATE TABLE IF NOT EXISTS ods.fct_unemployment (
                load_date DATE,
                entity VARCHAR(255),
                code VARCHAR(10),
                year INTEGER,
                unemployment_rate DECIMAL(10,6),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );

select count(*) from ods.fct_unemployment;




/* создаем структуру таблицы для витрины данных*/
CREATE TABLE IF NOT EXISTS dm.unemployment_analysis (
    record_id BIGSERIAL PRIMARY KEY,
    entity VARCHAR(255) NOT NULL,
    code VARCHAR(10),
    year INTEGER NOT NULL,
    unemployment_rate DECIMAL(10, 6),
    unemployment_category VARCHAR(20),
    prev_year_rate DECIMAL(5, 2),
    year_over_year_change DECIMAL(5, 2),
    trend_direction VARCHAR(35),
    global_avg_year DECIMAL(5, 2),
    diff_from_global_avg DECIMAL(5, 2),
    rank_by_year INTEGER,
    percentile_by_year DECIMAL(5, 3),
    source_load_date DATE,
    dm_refresh_date DATE DEFAULT CURRENT_DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_entity_year_source UNIQUE (entity, year, source_load_date)
);

/* создадим индексы для ускорения выборок*/
create index if not exists idx_unemp_analysis_entity_year on dm.unemployment_analysis(entity, year desc);
create index if not exists idx_unemp_analysis_refresh_date on dm.unemployment_analysis(dm_refresh_date);
comment on table dm.unemployment_analysis is 'Аналитическая витрина по безработице. Данные обновляются по расписанию через DAG.';

