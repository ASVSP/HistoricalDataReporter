CREATE TABLE continent (
    code VARCHAR2(3) NOT NULL,
    name VARCHAR2(255 CHAR)
);

ALTER TABLE continent ADD CONSTRAINT continent_pk PRIMARY KEY ( code );

CREATE TABLE country (
    code               VARCHAR2(5) NOT NULL,
    name               VARCHAR2(255 CHAR),
    alpha_3            VARCHAR2(3 CHAR),
    total_gas_emission NUMBER,
    region_code        VARCHAR2(3),
    alpha_2            VARCHAR2(2 CHAR)
);

ALTER TABLE country ADD CONSTRAINT country_pk PRIMARY KEY ( code );

CREATE TABLE gas (
    id             INTEGER NOT NULL,
    name           VARCHAR2(255 CHAR),
    chemical_label VARCHAR2(255 CHAR)
);

ALTER TABLE gas ADD CONSTRAINT gas_pk PRIMARY KEY ( id );

CREATE TABLE gasemission (
    id           INTEGER NOT NULL,
    quantity     NUMBER,
    country_code VARCHAR2(5),
    month_id     INTEGER,
    gas_id       INTEGER
);

ALTER TABLE gasemission ADD CONSTRAINT gasemission_pk PRIMARY KEY ( id );

CREATE TABLE month (
    id        BIGINT NOT NULL,
    name      VARCHAR2(255 CHAR),
    num_value INTEGER,
    year_id   INTEGER
);

ALTER TABLE month ADD CONSTRAINT month_pk PRIMARY KEY ( id );

CREATE TABLE region (
    code               VARCHAR2(3) NOT NULL,
    name               VARCHAR2(255 CHAR),
    total_gas_emission NUMBER,
    continent_code     VARCHAR2(3)
);

ALTER TABLE region ADD CONSTRAINT region_pk PRIMARY KEY ( code );

CREATE TABLE year (
    id                 BIGINT NOT NULL,
    value              INTEGER,
    total_gas_emission NUMBER
);

ALTER TABLE year ADD CONSTRAINT year_pk PRIMARY KEY ( id );

ALTER TABLE country
    ADD CONSTRAINT country_region_fk FOREIGN KEY ( region_code )
        REFERENCES region ( code );

ALTER TABLE gasemission
    ADD CONSTRAINT ge_country_fk FOREIGN KEY ( country_code )
        REFERENCES country ( code );

ALTER TABLE gasemission
    ADD CONSTRAINT ge_gas_fk FOREIGN KEY ( gas_id )
        REFERENCES gas ( id );

ALTER TABLE gasemission
    ADD CONSTRAINT ge_month_fk FOREIGN KEY ( month_id )
        REFERENCES month ( id );

ALTER TABLE month
    ADD CONSTRAINT month_year_fk FOREIGN KEY ( year_id )
        REFERENCES year ( id );

ALTER TABLE region
    ADD CONSTRAINT region_continent_fk FOREIGN KEY ( continent_code )
        REFERENCES continent ( code );