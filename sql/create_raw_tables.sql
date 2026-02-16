-- Raw tables DDL for Snowflake
-- Run once per environment to initialize landing tables

CREATE TABLE IF NOT EXISTS {database}.raw.claims_raw (
    claim_id        VARCHAR,
    patient_id      VARCHAR,
    provider_npi    VARCHAR,
    claim_type      VARCHAR,
    service_date    DATE,
    billed_amount   NUMBER(12,2),
    primary_diagnosis VARCHAR,
    diagnosis_count INTEGER,
    processing_date DATE,
    _loaded_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS {database}.raw.providers_raw (
    provider_npi          VARCHAR,
    provider_name         VARCHAR,
    specialty             VARCHAR,
    provider_type         VARCHAR,
    state                 VARCHAR,
    zip_code              VARCHAR,
    network_status        VARCHAR,
    is_accepting_patients BOOLEAN,
    effective_date        DATE,
    termination_date      DATE,
    _loaded_at            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS {database}.raw.eligibility_raw (
    patient_id            VARCHAR,
    first_name            VARCHAR,
    last_name             VARCHAR,
    date_of_birth         DATE,
    gender                VARCHAR,
    state                 VARCHAR,
    zip_code              VARCHAR,
    insurance_plan_id     VARCHAR,
    enrollment_start_date DATE,
    enrollment_end_date   DATE,
    _loaded_at            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS {database}.raw.vendor_claims_raw (
    claim_id              VARCHAR,
    patient_id            VARCHAR,
    provider_npi          VARCHAR,
    claim_type            VARCHAR,
    service_date          DATE,
    billed_amount_cents   INTEGER,
    diagnosis_codes       VARCHAR,
    procedure_code        VARCHAR,
    _loaded_at            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- File formats
CREATE FILE FORMAT IF NOT EXISTS {database}.raw.json_format
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE
    COMPRESSION = 'AUTO';

CREATE FILE FORMAT IF NOT EXISTS {database}.raw.csv_format
    TYPE = 'CSV'
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null', '')
    COMPRESSION = 'AUTO';

CREATE FILE FORMAT IF NOT EXISTS {database}.raw.parquet_format
    TYPE = 'PARQUET'
    COMPRESSION = 'SNAPPY';
