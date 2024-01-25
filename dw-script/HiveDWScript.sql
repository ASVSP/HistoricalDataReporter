CREATE TABLE IF NOT EXISTS continent (
    code VARCHAR(3),
    name VARCHAR(255),
    CONSTRAINT continent_pk PRIMARY KEY (code) DISABLE NOVALIDATE
);

CREATE TABLE IF NOT EXISTS gas (
    id             BIGINT,
    name           VARCHAR(255),
    chemical_label VARCHAR(255),
    CONSTRAINT gas_pk PRIMARY KEY (id) DISABLE NOVALIDATE
);

CREATE TABLE IF NOT EXISTS year (
    id                 BIGINT,
    value              INTEGER,
    total_gas_emission DOUBLE,
    CONSTRAINT year_pk PRIMARY KEY (id) DISABLE NOVALIDATE
);

CREATE TABLE IF NOT EXISTS month (
    id        BIGINT,
    name      VARCHAR(255),
    num_value INTEGER,
    year_id   BIGINT,
    CONSTRAINT month_pk PRIMARY KEY (id) DISABLE NOVALIDATE,
    CONSTRAINT month_year_fk
        FOREIGN KEY (year_id) REFERENCES year(id) DISABLE NOVALIDATE RELY
);

CREATE TABLE IF NOT EXISTS region (
    code               VARCHAR(3),
    name               VARCHAR(255),
    total_gas_emission DOUBLE,
    continent_code     VARCHAR(3),
    CONSTRAINT region_pk PRIMARY KEY (code) DISABLE NOVALIDATE,
    CONSTRAINT region_continent_fk
        FOREIGN KEY (continent_code) REFERENCES continent(code) DISABLE NOVALIDATE RELY
);

CREATE TABLE IF NOT EXISTS country (
    code               VARCHAR(5),
    name               VARCHAR(255),
    alpha_3            VARCHAR(3),
    total_gas_emission DOUBLE,
    region_code        VARCHAR(3),
    alpha_2            VARCHAR(2),
    CONSTRAINT country_pk PRIMARY KEY (code) DISABLE NOVALIDATE,
    CONSTRAINT country_region_fk FOREIGN KEY (region_code) REFERENCES region(code) DISABLE NOVALIDATE RELY
);

CREATE TABLE IF NOT EXISTS gas_emission (
    id           INTEGER,
    quantity     DOUBLE,
    country_code VARCHAR(5),
    month_id     BIGINT,
    gas_id       BIGINT,
    CONSTRAINT gas_emission_pk PRIMARY KEY (id) DISABLE NOVALIDATE,
    CONSTRAINT ge_country_fk
        FOREIGN KEY (country_code) REFERENCES country(code) DISABLE NOVALIDATE RELY,
    CONSTRAINT ge_gas_fk
        FOREIGN KEY (gas_id) REFERENCES gas(id) DISABLE NOVALIDATE RELY,
    CONSTRAINT ge_month_fk
        FOREIGN KEY (month_id) REFERENCES month(id) DISABLE NOVALIDATE RELY
);
