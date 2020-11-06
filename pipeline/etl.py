import configparser
import datetime
from helpers.redshift_connection import initialize_connection
from helpers.sql_queries import SqlQueries
from helpers.quality_checks import DataQuality


quality_check_helper = DataQuality()
query_helper = SqlQueries()


def run_sql_copy_from_list(dw_connection, table_list, source_format, aws_key, aws_secret, csv_ignore_header=0):
    """
    Takes in a psycopg2 connection object, a list of stage table names, a source format, credentials, and an optional
    CSV ignoreheader param and runs a Redshift COPY on all given tables. The naming convention of the tables assume that
    a matching directory exists in the S3 bucket.
    :param dw_connection: The psycopg2 connection object, assumed to be Amazon Redshift
    :param table_list: A list of table names on which to iterate and run the COPY
    :param source_format: CSV or JSON
    :param aws_key: Your AWS Key to authorize the connection to S3
    :param aws_secret: Your AWS Secret to authorize the connection to S3
    :param csv_ignore_header: Optional. Number of rows to skip in CSV file, for example, if the first row is a header
    :return: None
    """
    with dw_connection.cursor() as crsr:
        for table in table_list:
            crsr.execute(query_helper.truncate_sql_format.format(table))
            dw_connection.commit()

        print("All {} Stage tables truncated and ready for COPY.".format(source_format))

        phase_start_time = datetime.datetime.now()

        for table in table_list:
            print("COPYing " + table)
            if source_format.upper() == 'CSV':
                source_format_final = 'CSV\r\nIGNOREHEADER {}'.format(csv_ignore_header)
            if source_format.upper() == 'JSON':
                source_format_final = 'JSON \'auto\''
            crsr.execute(query_helper.copy_sql_format.format(
                table,
                's3://farchila-udacity-final/' + table.replace('stage_', ''),
                aws_key,
                aws_secret,
                source_format_final))
            dw_connection.commit()

        print("{} table COPY phase complete. time taken: {} seconds".format(
            source_format.upper(), (datetime.datetime.now() - phase_start_time).seconds))


def run_sql_commands_from_list(dw_connection, table_list, command_type, truncate_target_first=False):
    """
    Takes in a psycopg2 connection object and a list of dim or fact table names and performs the command_type on each
    one, which is retrieved dynamically from the SqlQueries class.
    :param dw_connection: The psycopg2 connection object, assumed to be Amazon Redshift
    :param table_list: A list of table names on which to iterate and run the desired command
    :param command_type: 'insert' or 'zerosk'
    :param truncate_target_first: Optional: if True, the target table will be truncated first.
    :return: None
    """
    with dw_connection.cursor() as crsr:
        for table in table_list:
            status_message_format = "Performing {} on {}{}..."
            truncate_message = ""
            if truncate_target_first:
                truncate_message = " with TRUNCATE first"

            status_message = status_message_format.format(command_type, table, truncate_message)

            print(status_message)

            phase_start_time = datetime.datetime.now()

            if truncate_target_first:
                crsr.execute(query_helper.truncate_sql_format.format(table))

            crsr.execute(query_helper.get_sql_command(table, command_type))
            dw_connection.commit()

            print(table + " hydrated. Time taken: {} seconds".format(
                (datetime.datetime.now() - phase_start_time).seconds))


def run_data_quality_check(dw_connection, quality_check_dict):
    """
    Takes in a psycopg2 connection object and a quality check dictionary which contains the validation SQL, a
    description of the test, and the expected number of rows that should result.
    :param dw_connection: The psycopg2 connection object, assumed to be Amazon Redshift
    :param quality_check_dict: The quality check dictionary. The keys should be {'query', 'test_description', and 'expected_number_of_rows'}
    :return:
    """
    with dw_connection.cursor() as crsr:
        desc = quality_check_dict["test_description"]
        query = quality_check_dict["query"]
        passing_result = quality_check_dict["expected_number_of_rows"]

        print("Running data quality check: " + desc + "\r\n\r\nquery: " + query + "\r\n")

        crsr.execute(query)
        rows_returned = len(crsr.fetchall())

        if rows_returned != passing_result:
            print(
                "ALERT! Data quality check \"{}\" did not pass. Expected {} row(s) but got {}. Please investigate.".
                format(desc, passing_result, rows_returned))

        else:
            print("Data quality check \"{}\" passed!".format(desc))


def run_pipeline():
    """
    The main driver function in this script. This will run the entire data pipeline for the Parking Violations data
    warehouse, leveraging lists of tables that vary based on load requirements, and uses the SqlQueries() class to
    retrieve and execute SQL to load the data. This function will also report on time taken for each table to load, as
    well as overall time for the pipeline job to finish.
    :return: None
    """
    overall_start_time = datetime.datetime.now()
    print("Data load has started at {}".format(overall_start_time))

    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    redshift = initialize_connection(
        config['DWH']['DWH_ENDPOINT'],
        config['DWH']['DWH_PORT'],
        config['DWH']['DWH_DB'],
        config['DWH']['DWH_DB_USER'],
        config['DWH']['DWH_DB_PASSWORD'])

    # Get our lists set up based on whether tables are stage or permanent and what the load logic is...
    stage_tables_csv = ['stage_issuingagency', 'stage_precinct', 'stage_registrationstate', 'stage_vehicle',
                    'stage_violation']

    stage_tables_json = ['stage_parking_violations']

    dim_tables_zerosk = ['dim_vehicle']

    dim_tables_trunc = ['dim_registrationstate', 'dim_violation', 'dim_borough', 'dim_precinct', 'dim_issuingagency']

    dim_tables_append = ['dim_vehicle', 'dim_time', 'dim_date']

    fact_tables = ['fact_parkingviolation']

    # First phase: Copy our source data that's been staged in S3

    run_sql_copy_from_list(dw_connection=redshift, table_list=stage_tables_csv, source_format='CSV',
                           aws_key=config['AWS']['KEY'], aws_secret=config['AWS']['SECRET'], csv_ignore_header=1)

    run_sql_copy_from_list(dw_connection=redshift, table_list=stage_tables_json, source_format='JSON',
                           aws_key=config['AWS']['KEY'], aws_secret=config['AWS']['SECRET'])

    # Next, run the appropriate SQL commands to hydrate the dim and fact tables with desired business logic

    run_sql_commands_from_list(dw_connection=redshift, table_list=dim_tables_zerosk, command_type="zerosk")

    run_sql_commands_from_list(dw_connection=redshift, table_list=dim_tables_trunc, command_type="insert",
                               truncate_target_first=True)

    run_sql_commands_from_list(dw_connection=redshift, table_list=dim_tables_append, command_type="insert")

    run_sql_commands_from_list(dw_connection=redshift, table_list=fact_tables, command_type="insert")

    # Data Quality checks: the last phase

    for qc_test in quality_check_helper.quality_check_dict_list:
        run_data_quality_check(dw_connection=redshift, quality_check_dict=qc_test)

    redshift.close()

    print("Data load complete: total time taken: {} minutes".format(
        (datetime.datetime.now() - overall_start_time).seconds / 60.0))


def main():
    run_pipeline()


if __name__ == "__main__":
    main()
