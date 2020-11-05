import configparser
import datetime
from helpers.redshift_connection import initialize_connection
from helpers.sql_queries import SqlQueries


def run_pipeline():
    """
    The main driver function in this script. This will run the entire data pipeline for the Parking Violations data
    warehouse, leveraging lists of tables that vary based on load requirements, and uses the SqlQueries() class to
    retrieve and execute SQL to load the data. This function will also report on time taken for each table to load, as
    well as overall time for the pipeline job to finish.
    :return: None
    """
    overall_start_time = datetime.datetime.now()

    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    query_helper = SqlQueries()

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

    stage_tables_combined = stage_tables_csv + stage_tables_json

    dim_tables_zerosk = ['dim_vehicle']

    dim_tables_trunc = ['dim_registrationstate', 'dim_violation', 'dim_borough', 'dim_precinct', 'dim_issuingagency']

    dim_tables_append = ['dim_vehicle', 'dim_time', 'dim_date']

    fact_tables = ['fact_parkingviolation']

    with redshift.cursor() as crsr:
        # for table in stage_tables_combined:
        #     crsr.execute(query_helper.truncate_sql_format.format(table))
        #     redshift.commit()
        #
        # print("All Stage tables truncated and ready for COPY.")
        #
        # phase_start_time = datetime.datetime.now()
        #
        # for table in stage_tables_csv:
        #     print("COPYing " + table)
        #     crsr.execute(query_helper.copy_sql_format.format(
        #         table,
        #         's3://farchila-udacity-final/' + table.replace('stage_', ''),
        #         config['AWS']['KEY'],
        #         config['AWS']['SECRET'],
        #         'CSV\r\nIGNOREHEADER 1'))
        #     redshift.commit()
        #
        # print("CSV table COPY phase complete. time taken: {} seconds".format(
        #     (datetime.datetime.now() - phase_start_time).seconds))
        #
        # phase_start_time = datetime.datetime.now()
        #
        # for table in stage_tables_json:
        #     print("COPYing " + table)
        #     crsr.execute(query_helper.copy_sql_format.format(
        #         table,
        #         's3://farchila-udacity-final/' + table.replace('stage_', ''),
        #         config['AWS']['KEY'],
        #         config['AWS']['SECRET'],
        #         'JSON \'auto\''))
        #     redshift.commit()
        #
        # print("JSON table COPY phase complete. time taken: {} seconds".format(
        #     (datetime.datetime.now() - phase_start_time).seconds))
        #
        # for table in dim_tables_zerosk:
        #     print("Adding zeroSK row for " + table + "...")
        #     phase_start_time = datetime.datetime.now()
        #     crsr.execute(query_helper.get_sql_command(table, 'zerosk'))
        #     redshift.commit()
        #     print(table + " zeroSK inserted. Time taken: {} seconds".format(
        #         (datetime.datetime.now() - phase_start_time).seconds))
        #
        # for table in dim_tables_trunc:
        #     print("Hydrating " + table + " with TRUNCATE first...")
        #     phase_start_time = datetime.datetime.now()
        #     crsr.execute(query_helper.truncate_sql_format.format(table))
        #     crsr.execute(query_helper.get_sql_command(table, 'insert'))
        #     redshift.commit()
        #     print(table + " hydrated. Time taken: {} seconds".format(
        #         (datetime.datetime.now() - phase_start_time).seconds))
        #
        # for table in dim_tables_append:
        #     print("Hydrating " + table + " by appending...")
        #     phase_start_time = datetime.datetime.now()
        #     crsr.execute(query_helper.get_sql_command(table, 'insert'))
        #     redshift.commit()
        #     print(table + " hydrated. Time taken: {} seconds".format(
        #         (datetime.datetime.now() - phase_start_time).seconds))

        for table in fact_tables:
            print("Hydrating " + table + "...")
            phase_start_time = datetime.datetime.now()
            crsr.execute(query_helper.get_sql_command(table, 'insert'))
            redshift.commit()
            print(table + " hydrated. Time taken: {} seconds".format(
                (datetime.datetime.now() - phase_start_time).seconds))

    redshift.close()

    print("Data load complete: total time taken: {} minutes".format(
        (datetime.datetime.now() - overall_start_time).seconds / 60.0))


if __name__ == "__main__":
    run_pipeline()
