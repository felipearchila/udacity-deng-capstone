class SqlQueries:

    create_dim_date = """CREATE TABLE IF NOT EXISTS dim_date
    (
        date_key INT PRIMARY KEY,
        calendar_date DATE,
        year_number INT,
        month_number INT,
        day_number INT,
        quarter_number INT,
        day_of_the_week VARCHAR(10),
        is_weekday BOOLEAN,
        is_holiday BOOLEAN
    );"""

    create_dim_time = """CREATE TABLE IF NOT EXISTS dim_time
    (
        time_key INT PRIMARY KEY,
        time_code VARCHAR(10),
        display_time TIMESTAMP,
        hour_number INT,
        minute_number INT,
        am_pm CHAR(2),
        military_time_hour_number INT
    );"""

    create_dim_borough = """CREATE TABLE IF NOT EXISTS dim_borough
    (
        borough_key INT PRIMARY KEY,
        borough_name VARCHAR(255)
    );"""

    create_dim_vehicle = """CREATE TABLE IF NOT EXISTS dim_vehicle
    (
        vehicle_key INT IDENTITY(1,1) PRIMARY KEY,
        vehicle_code VARCHAR(5),
        make VARCHAR(50),
        body_style VARCHAR(50),
        color_code VARCHAR(50),
        color_description VARCHAR(50)
    );"""

    create_dim_registrationstate = """CREATE TABLE IF NOT EXISTS dim_registrationstate
    (
        registration_state_key CHAR(2) PRIMARY KEY,
        state_name VARCHAR(50),
        postal_code CHAR(2)
    );"""

    create_dim_violation = """CREATE TABLE IF NOT EXISTS dim_violation
    (
        violation_key INT PRIMARY KEY,
        violation_description VARCHAR(255),
        fine_amount_96th_st_below INT,
        fine_amount_other INT
    );"""

    create_dim_precinct = """CREATE TABLE IF NOT EXISTS dim_precinct
    (
        precinct_key INT PRIMARY KEY,
        precinct_name VARCHAR(255),
        precinct_address VARCHAR(255),
        is_below_96th BOOLEAN
    );"""

    create_dim_issuingagency = """CREATE TABLE IF NOT EXISTS dim_issuingagency
    (
        issuing_agency_key CHAR(1) PRIMARY KEY,
        agency_name VARCHAR(255)
    );"""

    create_fact_parkingviolation = """CREATE TABLE IF NOT EXISTS fact_parkingviolation
    (
        parking_violation_key BIGINT PRIMARY KEY,
        summons_number BIGINT,
        plate_id VARCHAR(10),
        registration_state_key CHAR(2),
        plate_type CHAR(3),
        fine_amount INT,
        issue_date_key INT,
        violation_key INT,
        vehicle_key INT,
        issuing_agency_key CHAR(1),
        vehicle_expiration_date_key INT,
        violation_precinct_key INT,
        issuer_precinct_key INT,
        time_key INT,
        violation_address VARCHAR(255),
        is_unregistered_vehicle BOOLEAN,
        vehicle_year INT
    );"""

    truncate_sql_format = "TRUNCATE TABLE {}"

    drop_sql_format = "DROP TABLE IF EXISTS {}"

    create_stage_parkingviolation = """CREATE TABLE IF NOT EXISTS stage_parkingviolation
    (
        summons_number VARCHAR(255),
        plate_id VARCHAR(255),
        registration_state VARCHAR(255),
        plate_type VARCHAR(255),
        issue_date VARCHAR(255),
        violation_code VARCHAR(255),
        vehicle_body_type VARCHAR(255),
        vehicle_make VARCHAR(255),
        issuing_agency VARCHAR(255),
        street_code1 VARCHAR(255),
        street_code2 VARCHAR(255),
        street_code3 VARCHAR(255),
        vehicle_expiration_date VARCHAR(255),
        violation_location VARCHAR(255),
        violation_precinct VARCHAR(255),
        issuer_precinct VARCHAR(255),
        issuer_code VARCHAR(255),
        issuer_command VARCHAR(255),
        issuer_squad VARCHAR(255),
        violation_time VARCHAR(255),
        violation_county VARCHAR(255),
        violation_in_front_of_or_opposite VARCHAR(255),
        house_number VARCHAR(255),
        street_name VARCHAR(255),
        date_first_observed VARCHAR(255),
        law_section VARCHAR(255),
        sub_division VARCHAR(255),
        days_parking_in_effect VARCHAR(255),
        from_hours_in_effect VARCHAR(255),
        to_hours_in_effect VARCHAR(255),
        vehicle_color VARCHAR(255),
        unregistered_vehicle VARCHAR(255),
        vehicle_year VARCHAR(255),
        meter_number VARCHAR(255),
        feet_from_curb VARCHAR(255),
        intersecting_street VARCHAR(255),
        time_first_observed VARCHAR(255),
        violation_legal_code VARCHAR(255),
        violation_description VARCHAR(255)
    );"""

    create_stage_issuingagency = """CREATE TABLE IF NOT EXISTS stage_issuingagency
    (
        AgencyCode VARCHAR(255),
        Name VARCHAR(255)
    );
    """

    create_stage_precinct = """CREATE TABLE IF NOT EXISTS stage_precinct
    (
        PrecinctCode VARCHAR(255),
        Borough VARCHAR(255),
        Name VARCHAR(255),
        Address VARCHAR(255),
        FlagBelow96th VARCHAR(255)
    );"""

    create_stage_registrationstate = """CREATE TABLE IF NOT EXISTS stage_registrationstate
    (
        "Geographic Area" VARCHAR(255),
        "Postal Code" VARCHAR(255),
        "Total Resident Population" VARCHAR(255)
    );"""

    create_stage_vehicle = """CREATE TABLE IF NOT EXISTS stage_vehicle
    (
        "Record Type" VARCHAR(255),
        Make VARCHAR(255),
        "Body Type" VARCHAR(255),
        "Registration Class" VARCHAR(255)
    );"""

    create_stage_violation = """CREATE TABLE IF NOT EXISTS stage_violation
    (
        "VIOLATION CODE" VARCHAR(255),
        "VIOLATION DESCRIPTION" VARCHAR(255),
        FineAmount96thStBelow VARCHAR(255),
        FineAmountOther VARCHAR(255)
    );"""

    copy_sql_format = """COPY {} 
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    REGION 'us-east-1'
    FORMAT {}
    IGNOREHEADER {}
    COMPUPDATE OFF
    STATUPDATE OFF"""

    insert_dim_vehicle = """
    insert into dim_vehicle
    (
        vehicle_code,
        make,
        body_style,
        color_code,
        color_description
    )
    
    with color (code, description) as
    (
        select 'BK', 'Black'
        union
        select 'WH', 'White'
        union
        select 'GY', 'Gray'
        union
        select 'BL', 'Blue'
        union
        select 'BR', 'Brown'
        union
        select 'GL', 'Gold'
        union
        select 'MR', 'Maroon'
        union
        select 'OR', 'Orange'
        union
        select 'PK', 'Pink'
        union
        select 'PR', 'Purple'
        union
        select 'RD', 'Red'
        union
        select 'TN', 'Tan'
        union
        select 'YW', 'Yellow'
        union
        select 'OTH', 'Other/Unknown'
    ),
    
    body_style (vehicle_body_type) as
    (
        select distinct vehicle_body_type from stage_parkingviolation sp
        union
        select distinct "body type" from stage_vehicle sv
    ),
    
    vehicle_make (record_type, make) as
    (
        select distinct "record type", make from stage_vehicle sv
    ),
    
    cross_product (record_type, make, vehicle_body_type, code, description) as
    (
        select vehicle_make.record_type, vehicle_make.make, body_style.vehicle_body_type, color.code, color.description
        from vehicle_make
        cross join color, body_style
    )
    
    select cp.record_type, cp.make, cp.vehicle_body_type, cp.code, cp.description
    from cross_product cp
    left join dim_vehicle dv
        on cp.record_type = dv.vehicle_code
            and cp.make = dv.make
            and cp.vehicle_body_type = dv.body_style
            and cp.code = dv.color_code
    where dv.vehicle_key is null;
    """

    insert_dim_registrationstate = """
    insert into dim_registrationstate
    (
        registration_state_key,
        state_name,
        postal_code
    )
    
    select
        "postal code" as registration_state_key,
        "geographic area" as state_name,
        "postal code" as postal_code
    from stage_registrationstate sr
    where "postal code" <> '';
    """

    insert_dim_violation = """
    insert into dim_violation
    (
        violation_key,
        violation_description,
        fine_amount_96th_st_below,
        fine_amount_other
    )
    
    select
        cast("violation code" as int) as violation_key,
        "violation description" as violation_description,
        cast(fineamount96thstbelow as int) as fine_amount_96th_st_below,
        cast(fineamountother as int) as fine_amount_other
    from stage_violation sv;
    """

    insert_dim_borough = """
    insert into dim_borough
    (
        borough_key,
        borough_name 
    )
    
    with first_precincts
    as
    (
        select 
            min(cast(precinctcode as int)) as first_precinct, borough
        from stage_precinct sp
        group by borough
    )
    
    select
        rank() over (order by first_precinct) as borough_key,
        borough as borough_name
    from first_precincts;
    """

    insert_dim_precinct = """
    insert into dim_precinct
    (
        precinct_key,
        precinct_name,
        precinct_address,
        is_below_96th
    )
    
    select
        cast(precinctcode as int) as precinct_key,
        name as precinct_name,
        address as precinct_address,
        cast(cast(flagbelow96th as int) as boolean) as is_below_96th    
    from stage_precinct sp;
    """

    insert_dim_issuingagency = """
    insert into dim_issuingagency
    (
        issuing_agency_key,
        agency_name
    )
    
    select
        agencycode as issuing_agency_key,
        name as agency_name
    from stage_issuingagency si;
    """

    def get_sql_command(self, table_name, table_action):
        full_attr = f"{table_action}_{table_name}"
        try:
            sql = self.__getattribute__(full_attr)
            return sql
        except AttributeError as ae:
            print(f"An attribute of {full_attr} does not exist. Check the table and/or action name in your call.")
            raise
