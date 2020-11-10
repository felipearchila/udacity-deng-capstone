# <a name="top">Data Dictionary</a>

Data Dictionary outlining the City of New York Parking Violations data warehouse

## Dimension tables
- <a href="#d1">dim_borough</a>
- <a href="#d2">dim_date</a>
- <a href="#d3">dim_issuingagency</a>
- <a href="#d4">dim_precinct</a>
- <a href="#d5">dim_registrationstate</a>
- <a href="#d6">dim_time</a>
- <a href="#d7">dim_vehicle</a>
- <a href="#d8">dim_violation</a>

## Fact table
- <a href="#f1">fact_parkingviolation</a>

---
## <a name="d1">dim_borough</a>
### Source data: [Precincts - NYPD](https://www1.nyc.gov/site/nypd/bureaus/patrol/precincts-landing.page)
|Field Name|Data Type|Data Format|Field Size|Description|Transformation|Example|
|---|---|---|---|---|---|---|
|borough_key|INT|#|1|Primary Key value of a New York City Borough|Used RANK() function to assign key to Boroughs based on order in the Precinct list|1|
|borough_name|VARCHAR||255|Name of New York City Borough||Manhattan|

<a href="#top">Back to top</a>

## <a name="d2">dim_date</a>
### Source data: [Parking Violations Issued - Fiscal Year 2021](https://data.cityofnewyork.us/City-Government/Parking-Violations-Issued-Fiscal-Year-2021/pvqr-7yc4)
|Field Name|Data Type|Data Format|Field Size|Description|Transformation|Example|
|---|---|---|---|---|---|---|
|date_key|INT|YYYYMMDD|8|Primary Key: A formatted numeric value that uniquely identifies a date|cast(to_char(cast(issue_date as timestamp), 'YYYYMMDD') as int)|20201109|
|calendar_date|DATE|YYYY-MM-DD|10|A Date value representing a date||2020-11-09|
|year_number|INT|YYYY|4|A numeric value representing the Year of the date|date_part(month, datesource.calendar_date)|2020|
|month_number|INT|[M]M|1-2|A numeric value representing the Month of the date|date_part(year, datesource.calendar_date)|11 or 4|
|day_number|INT|[D]D|1-2|A numeric value representing the Day of the date|date_part(day, datesource.calendar_date)|9 or 23|
|quarter_number|INT|Q|1|A numeric value representing the Quarter of the date|date_part(quarter, datesource.calendar_date)|4|
|day_of_the_week|VARCHAR||10|The human-readable name of the day of the week for the date|to_char(datesource.calendar_date, 'Day')|Monday|
|is_weekday|BOOLEAN||5|True or False based on whether the given date is a weekday in the business working day sense. This assumes the work week is limited to Monday - Friday|case when date_part(weekday, datesource.calendar_date) between 1 and 5 then true else false end|true|

<a href="#top">Back to top</a>

## <a name="d3">dim_issuingagency</a>
### Source data: [Parking Violations Issued - Fiscal Year 2021](https://data.cityofnewyork.us/City-Government/Parking-Violations-Issued-Fiscal-Year-2021/pvqr-7yc4) - column description
|Field Name|Data Type|Data Format|Field Size|Description|Transformation|Example|
|---|---|---|---|---|---|---|
|issuing_agency_key|CHAR||1|Primary Key: Identifying character representing a City of New York Agency empowered to issue parking violations||P|
|agency_name|VARCHAR||255|Human-readable name identifying the issuing agency||Police Department|

<a href="#top">Back to top</a>

## <a name="d4">dim_precinct</a>
### Source data: [Precincts - NYPD](https://www1.nyc.gov/site/nypd/bureaus/patrol/precincts-landing.page)
|Field Name|Data Type|Data Format|Field Size|Description|Transformation|Example|
|---|---|---|---|---|---|---|
|precinct_key|INT||1-3|Primary Key: Identifying numeric value representing a Precinct in the City of New York||110|
|precinct_name|VARCHAR||255|Human-readable name of a Precinct||110th Precinct|
|precinct_address|VARCHAR||255|Street address of Precinct||94-41 43rd Avenue|
|is_below_96th|BOOLEAN||5|True or False based on whether Precinct is located at or below 96th street in Manhattan, which affects fine amounts for certain violations||False|

<a href="#top">Back to top</a>

## <a name="d5">dim_registrationstate</a>
### Source data: [USA states GeoJson | Kaggle](https://www.kaggle.com/pompelmo/usa-states-geojson?select=usa_population_2019.csv) and [NYC Finance | Plate Types & State Codes](http://www.nyc.gov/html/dof/html/pdf/faq/stars_codes.pdf)
|Field Name|Data Type|Data Format|Field Size|Description|Transformation|Example|
|---|---|---|---|---|---|---|
|registration_state_key|CHAR||2|Primary Key: Postal or Geographic Code identifying a region or governing body to which a vehicle is registered||GA|
|state_name|VARCHAR||50|Human-readable name identifying the state, region, or governing body to which a vehicle is registered||Georgia|
|postal_code|CHAR||2|Postal or Geographic Code identifying a region or governing body to which a vehicle is registered||GA|

<a href="#top">Back to top</a>

## <a name="d6">dim_time</a>
### Source data: [Parking Violations Issued - Fiscal Year 2021](https://data.cityofnewyork.us/City-Government/Parking-Violations-Issued-Fiscal-Year-2021/pvqr-7yc4)
|Field Name|Data Type|Data Format|Field Size|Description|Transformation|Example|
|---|---|---|---|---|---|---|
|time_key|INT||1-4|Primary Key: Numeric value uniquely representing every minute of a given day. Specifically, it is the number of minutes after midnight, with midnight representing zero (0)|datediff(minute, '1900-01-01 00:00:00', alltimes.timeval)|456|
|time_timestamp|TIMESTAMP|1900-01-01 HH:MM:SS||The Timestamp value representing the Time. All values are normalized to the date 1900-01-01 but the date has no significance|cast('1900-01-01 ' + left(stringtime, 2) + ':' + right(stringtime, 2) + ':00.000' as timestamp)|1900-01-01 07:36:00|
|hour_number|INT||1-2|The numeric value representing the hour of the Time using the 12-hour format. *To be used in conjunction with am_pm*|case when cast(left(stringtime, 2) as int) = 0 or cast(left(stringtime, 2) as int) > 12 then abs(cast(left(stringtime, 2) as int) - 12) else cast(left(stringtime, 2) as int) end|7|
|minute_number|INT||1-2|The numeric value representing the minute of the Time|cast(right(stringtime, 2)|36|
|am_pm|CHAR|AM\|PM|2|Two-character value representing AM or PM for the current 12-hour format Time. *To be used in conjunction with hour_number*||AM
|military_display_time|VARCHAR|HH:MM|10|Character value representing the Time in military or 24-hour format|left(stringtime, 2) + ':' + right(stringtime, 2)|07:36|
|military_time_hour_number|INT||1-2|The numeric value representing the hour of the Time using military or 24-hour format|cast(left(alltimes.military_display_time, 2) as int)|7|

<a href="#top">Back to top</a>

## <a name="d7">dim_vehicle</a>
### Source data: [Vehicle Makes and Body Types, Most Popular in New York State](https://data.ny.gov/Transportation/Vehicle-Makes-and-Body-Types-Most-Popular-in-New-Y/3pxy-wy2i) and data dictionary, and [Parking Violations Issued - Fiscal Year 2021](https://data.cityofnewyork.us/City-Government/Parking-Violations-Issued-Fiscal-Year-2021/pvqr-7yc4)
|Field Name|Data Type|Data Format|Field Size|Description|Transformation|Example|
|---|---|---|---|---|---|---|
|vehicle_key|INT||10|Primary Key: Numeric value that acts as a surrogate key representing a vehicle's distinct characteristics, which is the combination of make, body_style, and color_code||1234|
|vehicle_code|VARCHAR||5|Character code representing the type of vehicle. Only value as of release 1.00 is 'VEH' with possibility for future expansion||VEH|
|make|VARCHAR||50|Alphanumeric code representing the make of the vehicle. Code consists of abbreviations as set by New York State||TOYOT|
|body_style|VARCHAR||50|Alphanumeric code representing the body style of the vehicle. Code consists of abbreviations as set by New York State||4DSD|
|color_code|VARCHAR||50|Character code uniquely identifying the color of the vehicle||BK|
|color_description|VARCHAR||50|Human-readable description of the color of the vehicle||Black|

<a href="#top">Back to top</a>

## <a name="d8">dim_violation</a>
### Source data: Parking Violations Issued - Fiscal Year 2021 > [ParkingViolationCodes_January2020.xlsx](https://data.cityofnewyork.us/api/views/pvqr-7yc4/files/7875fa68-3a29-4825-9dfb-63ef30576f9e?download=true&filename=ParkingViolationCodes_January2020.xlsx)
|Field Name|Data Type|Data Format|Field Size|Description|Transformation|Example|
|---|---|---|---|---|---|---|
|violation_key|INT||1-3|Primary Key: Numeric value that uniquely identifies a parking violation type||34|
|violation_description|VARCHAR||255|Human-readable description of the parking violation type||Expired meter|
|fine_amount_96th_st_below|INT||2-3|The amount in US dollars levied for the violation if observed on 96th Street or below in Manhattan||65|
|fine_amount_other|INT||2-3|The amount in US dollars levied for the violation if observed anywhere else in New York City||35

<a href="#top">Back to top</a>

---

## <a name="f1">fact_parkingviolation</a>
### Source data: [Parking Violations Issued - Fiscal Year 2021](https://data.cityofnewyork.us/City-Government/Parking-Violations-Issued-Fiscal-Year-2021/pvqr-7yc4)
|Field Name|Data Type|Data Format|Field Size|Description|Transformation|Foreign Key Column|References|Example|
|---|---|---|---|---|---|---|---|---|
|parking_violation_key|BIGINT||10|Primary Key: Unique number assigned to each parking violation. A copy of summons_number||N||1471497410|
|summons_number|BIGINT||10|Unique number assigned to each parking violation||N||1471497410|
|plate_id|VARCHAR||10|Degenerate dimension: License plate number (if known) of vehicle in violation||N||ABC1234|
|registration_state_key|CHAR||2|Postal or Geographic Code identifying a region or governing body to which a vehicle is registered||Y|dim_registrationstate.registration_state_key|NY|
|plate_type|CHAR||3|Degenerate dimension: Character code identifying the type of license plate issued to vehicle in violation||N||PAS|
|fine_amount|INT||2-3|Measure: The amount in US dollars levied for the violation. This amount accounts for the location of the violation i.e. whether it occurred in Manhattan below 96th St|case when violationprecinct.flagbelow96th = '1' then cast(sv.fineamount96thstbelow as int) else coalesce(cast(sv.fineamountother as int), 0) end|N||115|
|issue_date_key|INT|YYYYMMDD|8|A formatted numeric value that uniquely identifies the date the violation was issued|cast(to_char(cast(spv.issue_date as timestamp), 'YYYYMMDD') as int)|Y|dim_date.date_key|20200831|
|violation_key|INT||1-3|Numeric value that uniquely identifies a parking violation type||Y|dim_violation.violation_key|34|
|vehicle_key|INT||10|Numeric value that acts as a surrogate key representing a vehicle's distinct characteristics, which is the combination of make, body_style, and color_code||Y|dim_vehicle.vehicle_key|1234|
|issuing_agency_key|CHAR||1|Identifying character representing a City of New York Agency empowered to issue parking violations||Y|dim_issuingagency.issuing_agency_key|P|
|vehicle_expiration_date_key|INT|YYYYMMDD|8|A formatted numeric value that uniquely identifies the expiration date of a vehicle's registration|cast(replace(left(replace(spv.vehicle_expiration_date, '0E-8', '88880088.0'), len(replace(spv.vehicle_expiration_date, '0E-8', '88880088.0')) - 2), '88880088', '19000101') as int)|Y|dim_date.date_key|20210228|
|violation_precinct_key|INT||1-3|Identifying numeric value representing a Precinct in the City of New York in which the violation occurred||Y|dim_precinct.precinct_key|60|
|issuer_precinct_key|INT||1-3|Identifying numeric value representing a Precinct in the City of New York from which the issuing agency is based||Y|dim_precinct.precinct_key|60|
|borough_key|INT||1|Numeric value representing a New York City Borough||Y|dim_borough.borough_key|1|
|time_key|INT||1-4|Numeric value uniquely representing every minute of a given day. Specifically, it is the number of minutes after midnight, with midnight representing zero (0)|datediff(minute, '1900-01-01 00:00:00', ('1900-01-01 ' + left(processed_time.stringtime, 2) + ':' + right(processed_time.stringtime, 2) + ':00.000'as timestamp))|Y|dim_time.time_key|1439|
|violation_address|VARCHAR||255|User-entered address at which the violation was observed|coalesce(nullif(spv.house_number, 'NaN') + ' ', '') + nullif(spv.street_name, 'NaN')|N||129 W 81ST|
|vehicle_year|INT||1-4|User-entered model year of vehicle in violation. If unknown, vehicle_year will be 0||N||2014|

<a href="#top">Back to top</a>
