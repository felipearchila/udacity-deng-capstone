class SqlQueries:

    create_dim_date = """CREATE TABLE IF NOT EXISTS dim_date
    (
        date_key INT PRIMARY KEY DISTKEY SORTKEY,
        calendar_date DATE,
        year_number INT,
        month_number INT,
        day_number INT,
        quarter_number INT,
        day_of_the_week VARCHAR(10),
        is_weekday BOOLEAN
    );"""

    create_dim_time = """CREATE TABLE IF NOT EXISTS dim_time
    (
        time_key INT PRIMARY KEY DISTKEY SORTKEY,
        time_timestamp TIMESTAMP,
        hour_number INT,
        minute_number INT,
        am_pm CHAR(2),
        military_display_time VARCHAR(10),
        military_time_hour_number INT
    );"""

    create_dim_borough = """CREATE TABLE IF NOT EXISTS dim_borough
    (
        borough_key INT PRIMARY KEY DISTKEY SORTKEY,
        borough_name VARCHAR(255)
    );"""

    create_dim_vehicle = """CREATE TABLE IF NOT EXISTS dim_vehicle
    (
        vehicle_key INT IDENTITY(1,1) PRIMARY KEY DISTKEY,
        vehicle_code VARCHAR(5),
        make VARCHAR(50),
        body_style VARCHAR(50),
        color_code VARCHAR(50),
        color_description VARCHAR(50)
    )
    COMPOUND SORTKEY (make, body_style, color_code);
    """

    create_dim_registrationstate = """CREATE TABLE IF NOT EXISTS dim_registrationstate
    (
        registration_state_key CHAR(2) PRIMARY KEY DISTKEY SORTKEY,
        state_name VARCHAR(50),
        postal_code CHAR(2)
    );"""

    create_dim_violation = """CREATE TABLE IF NOT EXISTS dim_violation
    (
        violation_key INT PRIMARY KEY DISTKEY SORTKEY,
        violation_description VARCHAR(255),
        fine_amount_96th_st_below INT,
        fine_amount_other INT
    );"""

    create_dim_precinct = """CREATE TABLE IF NOT EXISTS dim_precinct
    (
        precinct_key INT PRIMARY KEY DISTKEY SORTKEY,
        precinct_name VARCHAR(255),
        precinct_address VARCHAR(255),
        is_below_96th BOOLEAN
    );"""

    create_dim_issuingagency = """CREATE TABLE IF NOT EXISTS dim_issuingagency
    (
        issuing_agency_key CHAR(1) PRIMARY KEY DISTKEY SORTKEY,
        agency_name VARCHAR(255)
    );"""

    create_fact_parkingviolation = """CREATE TABLE IF NOT EXISTS fact_parkingviolation
    (
        parking_violation_key BIGINT PRIMARY KEY SORTKEY,
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
        borough_key INT,
        time_key INT,
        violation_address VARCHAR(255),
        vehicle_year INT
    );"""

    truncate_sql_format = "TRUNCATE TABLE {}"

    drop_sql_format = "DROP TABLE IF EXISTS {}"

    create_stage_parking_violations = """CREATE TABLE IF NOT EXISTS stage_parking_violations
    (
        summons_number BIGINT,
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
        violation_description VARCHAR(255),
        vehicle_color_standardized VARCHAR(3),
        year_number INT,
        month_number INT
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
        select distinct vehicle_body_type from stage_parking_violations sp
        union
        select distinct "body type" from stage_vehicle sv
    ),
    
    vehicle_make (record_type, make) as
    (
        select distinct "record type", make from stage_vehicle sv
        where "record type" = 'VEH'
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
    where "postal code" <> ''
    union
    select
        'UK' as registration_state_key,
        'Unknown' as state_name,
        'UK' as postal_code;
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

    insert_dim_time = """
    insert into dim_time
    (
        time_key,
        time_timestamp,
        hour_number,
        minute_number,
        am_pm,
        military_display_time,
        military_time_hour_number
    )
    
    with times (violation_time, hour_num, minute_num, ap, stringtime)
    as
    (
    select distinct  
        violation_time,
        left(regexp_replace(violation_time, '[a-zB-OQ-Z\s\. ]', '0'), 2),
        substring(regexp_replace(violation_time, '[a-zB-OQ-Z\s\. ]', '0'), 3, 2),
        case
            when right(violation_time, 1) similar to '[0-9]' and left(regexp_replace(violation_time, '[a-zB-OQ-Z\s\. ]', '0'), 2) >= 12
                then 'P'
            when right(violation_time, 1) similar to '[0-9]' and left(regexp_replace(violation_time, '[a-zB-OQ-Z\s\. ]', '0'), 2) < 12
                then 'A'
            when right(violation_time, 1) similar to '[AP]'
                then 
                right(violation_time, 1)
            else 'A'
            end,
        case
            when right(violation_time, 1) = 'P'
                then cast(cast(left(regexp_replace(violation_time, '[a-zB-OQ-Z\s\. ]', '0'), len(regexp_replace(violation_time, '[a-zB-OQ-Z\s\. ]', '0'))-1) as int) % 1200 + 1200 as varchar(10))
            when right(violation_time, 1) = 'A'
                then ltrim(to_char(cast(left(regexp_replace(violation_time, '[a-zB-OQ-Z\s\. ]', '0'), len(regexp_replace(violation_time, '[a-zB-OQ-Z\s\. ]', '0'))-1) as int) % 1200, '0999'))
            when right(violation_time, 1) similar to '[0-9]'
                then violation_time
            else '0000'
            end
    from stage_parking_violations spv
    ),
    
    alltimes (stringtime, timeval, military_display_time, hour_num, minute_num, ap)
    as
    (
    select distinct
        stringtime,
        cast('1900-01-01 ' + left(stringtime, 2) + ':' + right(stringtime, 2) + ':00.000' as timestamp),
        left(stringtime, 2) + ':' + right(stringtime, 2) as military_display_time,
        case
            when cast(left(stringtime, 2) as int) = 0 or cast(left(stringtime, 2) as int) > 12
                then abs(cast(left(stringtime, 2) as int) - 12)
            else cast(left(stringtime, 2) as int)
            end as hour_num,
        cast(right(stringtime, 2) as int),
        ap + 'M'
    from times
    )
    
    select
        datediff(minute, '1900-01-01 00:00:00', alltimes.timeval) as time_key,
        alltimes.timeval as time_timestamp,
        alltimes.hour_num as hour_number,
        alltimes.minute_num as minute_number,
        alltimes.ap as am_pm,
        alltimes.military_display_time,
        cast(left(alltimes.military_display_time, 2) as int) as military_time_hour_number
    from alltimes
    left join dim_time
        on  datediff(minute, '1900-01-01 00:00:00', alltimes.timeval) = dim_time.time_key
    where dim_time.time_key is null;
    """

    insert_dim_date = """
    insert into dim_date
    (
        date_key,
        calendar_date,
        year_number,
        month_number,
        day_number,
        quarter_number,
        day_of_the_week,
        is_weekday
    )
    
    with datesource(date_key, calendar_date)
    as
    (
        select distinct
            cast(to_char(cast(issue_date as timestamp), 'YYYYMMDD') as int),
            cast(issue_date as timestamp)
        from stage_parking_violations spv
        union
        select distinct
            cast(replace(left(replace(vehicle_expiration_date, '0E-8', '88880088.0'), len(replace(vehicle_expiration_date, '0E-8', '88880088.0'))-2), '88880088', '19000101') as int),
            to_date(replace(left(replace(vehicle_expiration_date, '0E-8', '88880088.0'), len(replace(vehicle_expiration_date, '0E-8', '88880088.0'))-2), '88880088', '19000101'), 'YYYYMMDD')
        from stage_parking_violations spv
    )
    
    select
        datesource.date_key,
        datesource.calendar_date,
        date_part(year, datesource.calendar_date) as year_number,
        date_part(month, datesource.calendar_date) as month_number,
        date_part(day, datesource.calendar_date) as day_number,
        date_part(quarter, datesource.calendar_date) as quarter_number,
        to_char(datesource.calendar_date, 'Day') as day_of_the_week,
        case
            when date_part(weekday, datesource.calendar_date) between 1 and 5
                then true
            else false
            end as is_weekday
    from datesource
    left join dim_date
        on datesource.date_key = dim_date.date_key
    where dim_date.date_key is null;
    """

    insert_fact_parkingviolation = """
    insert into fact_parkingviolation
    (
        parking_violation_key,
        summons_number,
        plate_id,
        registration_state_key,
        plate_type,
        fine_amount,
        issue_date_key,
        violation_key,
        vehicle_key,
        issuing_agency_key,
        vehicle_expiration_date_key,
        violation_precinct_key,
        issuer_precinct_key,
        borough_key,
        time_key,
        violation_address,
        vehicle_year
    )
    
    with processed_time
    as
    (select distinct
        violation_time,
        case
            when right(violation_time, 1) = 'P'
                then cast(cast(left(regexp_replace(violation_time, '[a-zB-OQ-Z\s\. ]', '0'), len(regexp_replace(violation_time, '[a-zB-OQ-Z\s\. ]', '0'))-1) as int) % 1200 + 1200 as varchar(10))
            when right(violation_time, 1) = 'A'
                then ltrim(to_char(cast(left(regexp_replace(violation_time, '[a-zB-OQ-Z\s\. ]', '0'), len(regexp_replace(violation_time, '[a-zB-OQ-Z\s\. ]', '0'))-1) as int) % 1200, '0999'))
            when right(violation_time, 1) similar to '[0-9]'
                then violation_time
            else '0000'
            end as stringtime
    from stage_parking_violations spv)
    
    select
        spv.summons_number as parking_violation_key,
        spv.summons_number,
        spv.plate_id,
        replace(spv.registration_state, '99', 'UK') as registration_state_key,
        replace(spv.plate_type, '999', 'UNK') as plate_type,
        case
            when sp.flagbelow96th = '1'
                then cast(sv.fineamount96thstbelow as int)
            else coalesce(cast(sv.fineamountother as int), 0)
            end as fine_amount,
        cast(to_char(cast(spv.issue_date as timestamp), 'YYYYMMDD') as int) as issue_date_key,
        cast(spv.violation_code as int) as violation_key,
        coalesce(dv.vehicle_key, 0) as vehicle_key,
        spv.issuing_agency as issuing_agency_key,
        cast(
            replace(
                left(
                    replace(spv.vehicle_expiration_date, '0E-8', '88880088.0'),
                    len(replace(spv.vehicle_expiration_date, '0E-8', '88880088.0')) - 2),
                '88880088', '19000101')
            as int)
            as vehicle_expiration_date_key,
        cast(spv.violation_precinct as int) as violation_precinct_key,
        cast(spv.issuer_precinct as int) as issuer_precinct_key,
        coalesce(db.borough_key, 0) as borough_key,
        datediff(minute, '1900-01-01 00:00:00', cast(
            '1900-01-01 ' + left(processed_time.stringtime, 2) + ':' + right(processed_time.stringtime, 2) + ':00.000'
                as timestamp)
            ) as time_key,
        coalesce(nullif(spv.house_number, 'NaN') + ' ', '') + nullif(spv.street_name, 'NaN') as violation_address,
        cast(spv.vehicle_year as int)
    from stage_parking_violations spv
    left join stage_violation sv
        on spv.violation_code = sv."violation code"
    left join stage_precinct sp
        on spv.violation_precinct = sp.precinctcode
    left join dim_borough db
        on sp.borough = db.borough_name
    left join dim_vehicle dv
        on spv.vehicle_make = dv.make
            and spv.vehicle_body_type = dv.body_style
            and spv.vehicle_color_standardized = dv.color_code
    join processed_time
        on spv.violation_time = processed_time.violation_time
    left join fact_parkingviolation f
        on spv.summons_number = f.parking_violation_key
    where f.parking_violation_key is null;
    """

    def get_sql_command(self, table_name, table_action):
        full_attr = f"{table_action}_{table_name}"
        try:
            sql = self.__getattribute__(full_attr)
            return sql
        except AttributeError as ae:
            print(f"An attribute of {full_attr} does not exist. Check the table and/or action name in your call.")
            raise
