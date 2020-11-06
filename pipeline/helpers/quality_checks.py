class DataQuality:
    """
    Provides a centralized repository for all data quality check SQL in the form of a list of Dictionaries. To add to
    the list, simply add a new Dictionary with the following key-value pairs:
    {
        'query': The SQL query you'd like to run. Usually, there will be logic to compare expected row counts.
        'test_description': A human-readable description of the test. This will be printed with the test run and result.
        'expected_number_of_rows': The number of rows you'd expect. If the actual row count returned is different, an
            alert is raised and the test fails.
    }
    """
    quality_check_dict_list = [
        {
            "query": """
                select count(1) from fact_parkingviolation fp
                union
                select count(1) from stage_parking_violations spv;""",
            "test_description": "Ensure grain hasn't changed and row counts match between fact and stage table",
            "expected_number_of_rows": 1
        },
        {
            "query": """select 1 from fact_parkingviolation
                left join dim_borough on fact_parkingviolation.borough_key = dim_borough.borough_key
                where dim_borough.borough_key is null limit 1;""",
            "test_description": "Verify no missing borough references",
            "expected_number_of_rows": 0
        },
        {
            "query": """select 1 from fact_parkingviolation
                left join dim_date on fact_parkingviolation.issue_date_key = dim_date.date_key
                where dim_date.date_key is null limit 1;""",
            "test_description": "Verify all issue dates exist",
            "expected_number_of_rows": 0
        },
        {
            "query": """select 1 from fact_parkingviolation
                left join dim_date on fact_parkingviolation.vehicle_expiration_date_key = dim_date.date_key
                where dim_date.date_key is null limit 1;""",
            "test_description": "Verify all vehicle expiration dates exist",
            "expected_number_of_rows": 0
        },
        {
            "query": """select 1 from fact_parkingviolation
                left join dim_issuingagency
                    on fact_parkingviolation.issuing_agency_key = dim_issuingagency.issuing_agency_key
                where dim_issuingagency.issuing_agency_key is null limit 1;""",
            "test_description": "Verify no missing issuing agencies",
            "expected_number_of_rows": 0
        },
        {
            "query": """select 1 from fact_parkingviolation
                left join dim_precinct on fact_parkingviolation.issuer_precinct_key = dim_precinct.precinct_key
                where dim_precinct.precinct_key is null limit 1;""",
            "test_description": "Verify no missing issuer precincts",
            "expected_number_of_rows": 0
        },
        {
            "query": """select 1 from fact_parkingviolation
                left join dim_precinct on fact_parkingviolation.violation_precinct_key = dim_precinct.precinct_key
                where dim_precinct.precinct_key is null limit 1;""",
            "test_description": "Verify no missing violation precincts",
            "expected_number_of_rows": 0
        },
        {
            "query": """select 1 from fact_parkingviolation
                left join dim_registrationstate
                    on fact_parkingviolation.registration_state_key = dim_registrationstate.registration_state_key
                where dim_registrationstate.registration_state_key is null limit 1;""",
            "test_description": "Verify no missing registration states",
            "expected_number_of_rows": 0
        },
        {
            "query": """select 1 from fact_parkingviolation
                left join dim_time on fact_parkingviolation.time_key = dim_time.time_key
                where dim_time.time_key is null limit 1;""",
            "test_description": "Verify all times are populated",
            "expected_number_of_rows": 0
        },
        {
            "query": """select 1 from fact_parkingviolation
                left join dim_vehicle on fact_parkingviolation.vehicle_key = dim_vehicle.vehicle_key
                where dim_vehicle.vehicle_key is null limit 1;""",
            "test_description": "Verify all vehicle keys are present",
            "expected_number_of_rows": 0
        },
        {
            "query": """select 1 from fact_parkingviolation
                left join dim_violation on fact_parkingviolation.violation_key = dim_violation.violation_key
                where dim_violation.violation_key is null limit 1;""",
            "test_description": "Verify no missing violation keys",
            "expected_number_of_rows": 0
        }
    ]
