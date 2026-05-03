/*
-- database
CREATE DATABASE stations_db;
*/

-- schema
CREATE SCHEMA IF NOT EXISTS facttable;

-- Fact Table
CREATE TABLE IF NOT EXISTS facttable.fact (
    station INTEGER NOT NULL,
    trains VARCHAR NOT NULL,
    date VARCHAR NOT NULL,
    time VARCHAR NOT NULL,
    PRIMARY KEY (station, trains, date, time)
);

-- Type 1 (needed for the ril100Identifier type)
CREATE TYPE IF NOT EXISTS geographic_coordinates AS (
    type VARCHAR,
    coordinates DOUBLE PRECISION[]
);

-- Type 2 (needed for the Station table)
CREATE TYPE IF NOT EXISTS ril100Identifier AS (
    rilIdentifier VARCHAR,
    isMain BOOLEAN,
    hasSteamPermission BOOLEAN,
    steamPermission VARCHAR,
    geographicCoordinates geographic_coordinates[],
    primaryLocationCode VARCHAR
);

-- Station Table 
CREATE TABLE IF NOT EXISTS facttable.station (
    number INTEGER NOT NULL PRIMARY KEY,
    ifopt VARCHAR(18) NOT NULL CHECK (ifopt ~ '^de:11000:900[0-9]{6}$'),
    name VARCHAR NOT NULL,

    mailingAddress_city VARCHAR NOT NULL CHECK(mailingAddress_city = 'Berlin'),
    mailingAddress_zipcode VARCHAR(5) NOT NULL CHECK(mailingAddress_zipcode ~ '^[0-9]{5}$'),
    mailingAddress_street VARCHAR NOT NULL,

    category SMALLINT NOT NULL,
    priceCategory SMALLINT NOT NULL,
    hasParking BOOLEAN NOT NULL,
    hasBicycleParking BOOLEAN NOT NULL,
    hasLocalPublicTransport BOOLEAN NOT NULL,
    hasPublicFacilities BOOLEAN NOT NULL,
    hasLockerSystem BOOLEAN NOT NULL,
    hasTaxiRank BOOLEAN NOT NULL,
    hasTravelNecessities BOOLEAN NOT NULL,
    hasSteplessAccess VARCHAR NOT NULL CHECK(hasSteplessAccess IN ('yes', 'partial', 'no')),
    hasMobilityService VARCHAR NOT NULL,
    hasWiFi BOOLEAN NOT NULL,
    hasTravelCenter BOOLEAN NOT NULL,
    hasRailwayMission BOOLEAN NOT NULL,
    hasDBLounge BOOLEAN NOT NULL,
    hasLostAndFound BOOLEAN NOT NULL,
    hasCarRental BOOLEAN NOT NULL,
    federalState VARCHAR NOT NULL CHECK(federalState = 'Berlin'),

    regionalbereich_number SMALLINT NOT NULL,
    regionalbereich_name VARCHAR NOT NULL CHECK(regionalbereich_name = 'RB Ost'),
    regionalbereich_shortName VARCHAR NOT NULL CHECK(regionalbereich_shortName = 'RB Ost'),

    aufgabentraeger_shortName VARCHAR NOT NULL CHECK(aufgabentraeger_shortName = 'VBB Berlin'),
    aufgabentraeger_name VARCHAR NOT NULL CHECK(aufgabentraeger_name = 'Verkehrsverbund Berlin-Brandenburg GmbH für Berlin'),

    DBinformation_availability_monday_fromTime TIME,
    DBinformation_availability_monday_toTime TIME,
    DBinformation_availability_tuesday_fromTime TIME,
    DBinformation_availability_tuesday_toTime TIME,
    DBinformation_availability_wednesday_fromTime TIME,
    DBinformation_availability_wednesday_toTime TIME,
    DBinformation_availability_thursday_fromTime TIME,
    DBinformation_availability_thursday_toTime TIME,
    DBinformation_availability_friday_fromTime TIME,
    DBinformation_availability_friday_toTime TIME,
    DBinformation_availability_saturday_fromTime TIME,
    DBinformation_availability_saturday_toTime TIME,
    DBinformation_availability_sunday_fromTime TIME,
    DBinformation_availability_sunday_toTime TIME,
    DBinformation_availability_holiday_fromTime TIME,
    DBinformation_availability_holiday_toTime TIME,

    CHECK(DBinformation_availability_monday_fromTime < DBinformation_availability_monday_toTime),
    CHECK(DBinformation_availability_tuesday_fromTime < DBinformation_availability_tuesday_toTime),
    CHECK(DBinformation_availability_wednesday_fromTime < DBinformation_availability_wednesday_toTime),
    CHECK(DBinformation_availability_thursday_fromTime < DBinformation_availability_thursday_toTime),
    CHECK(DBinformation_availability_friday_fromTime < DBinformation_availability_friday_toTime),
    CHECK(DBinformation_availability_saturday_fromTime < DBinformation_availability_saturday_toTime),
    CHECK(DBinformation_availability_sunday_fromTime < DBinformation_availability_sunday_toTime),
    CHECK(DBinformation_availability_holiday_fromTime < DBinformation_availability_holiday_toTime),

    localServiceStaff_availability_monday_fromTime TIME,
    localServiceStaff_availability_monday_toTime TIME,
    localServiceStaff_availability_tuesday_fromTime TIME,
    localServiceStaff_availability_tuesday_toTime TIME,
    localServiceStaff_availability_wednesday_fromTime TIME,
    localServiceStaff_availability_wednesday_toTime TIME,
    localServiceStaff_availability_thursday_fromTime TIME,
    localServiceStaff_availability_thursday_toTime TIME,
    localServiceStaff_availability_friday_fromTime TIME,
    localServiceStaff_availability_friday_toTime TIME,
    localServiceStaff_availability_saturday_fromTime TIME,
    localServiceStaff_availability_saturday_toTime TIME,
    localServiceStaff_availability_sunday_fromTime TIME,
    localServiceStaff_availability_sunday_toTime TIME,
    localServiceStaff_availability_holiday_fromTime TIME,
    localServiceStaff_availability_holiday_toTime TIME,

    CHECK(localServiceStaff_availability_monday_fromTime < localServiceStaff_availability_monday_toTime),
    CHECK(localServiceStaff_availability_tuesday_fromTime < localServiceStaff_availability_tuesday_toTime),
    CHECK(localServiceStaff_availability_wednesday_fromTime < localServiceStaff_availability_wednesday_toTime),
    CHECK(localServiceStaff_availability_thursday_fromTime < localServiceStaff_availability_thursday_toTime),
    CHECK(localServiceStaff_availability_friday_fromTime < localServiceStaff_availability_friday_toTime),
    CHECK(localServiceStaff_availability_saturday_fromTime < localServiceStaff_availability_saturday_toTime),
    CHECK(localServiceStaff_availability_sunday_fromTime < localServiceStaff_availability_sunday_toTime),
    CHECK(localServiceStaff_availability_holiday_fromTime < localServiceStaff_availability_holiday_toTime),

    timeTableOffice_email VARCHAR NOT NULL,
    timeTableOffice_name VARCHAR NOT NULL,

    szentrale_number SMALLINT NOT NULL,
    szentrale_publicPhoneNumber VARCHAR NOT NULL,
    szentrale_name VARCHAR NOT NULL,

    stationManagement_number INTEGER NOT NULL,
    stationManagement_name VARCHAR NOT NULL,

    evaNumbers_number INTEGER NOT NULL,
    evaNumbers_name VARCHAR NOT NULL,

    ril100Identifiers ril100Identifier[],

    productLine_productLine VARCHAR NOT NULL,
    productLine_segment VARCHAR NOT NULL,
    wirelessLan VARCHAR,

    FOREIGN KEY (number) REFERENCES facttable.fact(station)
);

-- Trains Table 
CREATE TABLE IF NOT EXISTS facttable.trains (
    train_id VARCHAR PRIMARY KEY,
    eva INTEGER,
    m_c INTEGER,
    m_cat VARCHAR,
    m_del INTEGER,
    m_dm_int VARCHAR,
    m_dm_n VARCHAR,
    m_dm_t VARCHAR,
    m_dm_ts VARCHAR,
    m_ec VARCHAR,
    m_elnk VARCHAR,
    m_ext VARCHAR,
    m_from VARCHAR,
    m_id VARCHAR,
    m_int VARCHAR,
    m_o VARCHAR,
    m_pr VARCHAR,
    m_t VARCHAR,
    m_tl_c VARCHAR,
    m_tl_f VARCHAR,
    m_tl_n VARCHAR,
    m_tl_o VARCHAR,
    m_tl_t VARCHAR,
    m_to VARCHAR,
    m_ts VARCHAR,
    s_ar_cde VARCHAR,
    s_ar_clt VARCHAR,
    s_ar_cp VARCHAR,
    s_ar_cpth VARCHAR,
    s_ar_cs VARCHAR,
    s_ar_ct VARCHAR,
    s_ar_dc INTEGER,
    s_ar_hi INTEGER,
    s_ar_l VARCHAR,
    s_ar_m VARCHAR,
    s_ar_pde VARCHAR,
    s_ar_pp VARCHAR,
    s_ar_ppth VARCHAR,
    s_ar_ps VARCHAR,
    s_ar_pt VARCHAR,
    s_ar_tra VARCHAR,
    s_ar_wings VARCHAR,
    s_conn_cs VARCHAR,
    s_conn_eva INTEGER,
    s_conn_id VARCHAR,
    s_conn_ref VARCHAR,
    s_conn_s VARCHAR,
    s_conn_ts VARCHAR,
    s_dp VARCHAR,
    s_eva INTEGER,
    s_hd_ar VARCHAR,
    s_hd_cod VARCHAR,
    s_hd_dp VARCHAR,
    s_hd_src VARCHAR,
    s_hd_ts VARCHAR,
    s_hpc_ar VARCHAR,
    s_hpc_cot VARCHAR,
    s_hpc_dp VARCHAR,
    s_hpc_ts VARCHAR,
    s_id VARCHAR,
    s_m VARCHAR,
    s_ref_rt VARCHAR,
    s_ref_tl VARCHAR,
    s_rtr_rt_c BOOLEAN,
    s_rtr_rt_ea_eva INTEGER,
    s_rtr_rt_ea_i INTEGER,
    s_rtr_rt_ea_n VARCHAR,
    s_rtr_rt_ea_pt VARCHAR,
    s_rtr_rt_id VARCHAR,
    s_rtr_rt_rtl_c VARCHAR,
    s_rtr_rt_rtl_n VARCHAR,
    s_rtr_rt_sd VARCHAR,
    s_rtr_rts VARCHAR,
    s_tl VARCHAR,
    station VARCHAR,

    FOREIGN KEY (train_id) REFERENCES facttable.fact(trains)
);

-- Date Dimension
CREATE TABLE IF NOT EXISTS facttable.date (
    date_id VARCHAR PRIMARY KEY,
    full_date DATE NOT NULL,
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    day INTEGER NOT NULL,

    FOREIGN KEY (date_id) REFERENCES facttable.fact(date)
); 

-- Time Dimension
CREATE TABLE IF NOT EXISTS facttable.time (
    time_id VARCHAR PRIMARY KEY,
    hour INTEGER NOT NULL,
    minute INTEGER NOT NULL,
    is_peak BOOLEAN,

    FOREIGN KEY (time_id) REFERENCES facttable.fact(time)
); 
