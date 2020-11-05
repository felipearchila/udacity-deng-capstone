import configparser
from helpers.redshift_connection import initialize_connection
from helpers.sql_queries import SqlQueries


def create_data_warehouse():
    """
    The main driver of this script. This will instantiate a connection to the data warehouse and run DROP and CREATE
    scripts for all tables for the data warehouse defined in the list below with the help of the SqlQueries() class.
    :return: None
    """
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    query_helper = SqlQueries()

    redshift = initialize_connection(
        config['DWH']['DWH_ENDPOINT'],
        config['DWH']['DWH_PORT'],
        config['DWH']['DWH_DB'],
        config['DWH']['DWH_DB_USER'],
        config['DWH']['DWH_DB_PASSWORD'])

    table_names = ['stage_violation', 'stage_vehicle', 'stage_registrationstate', 'stage_precinct',
                   'stage_issuingagency', 'stage_parking_violations', 'fact_parkingviolation', 'dim_issuingagency',
                   'dim_precinct', 'dim_violation', 'dim_registrationstate', 'dim_vehicle', 'dim_borough', 'dim_time',
                   'dim_date']

    print("Starting creation of the Parking Violations data warehouse...")

    with redshift.cursor() as crsr:
        for table in table_names:
            crsr.execute(query_helper.drop_sql_format.format(table))
            crsr.execute(query_helper.get_sql_command(table, 'create'))
            redshift.commit()

    redshift.close()

    print("Creation of the Parking Violations data warehouse complete!")


if __name__ == "__main__":
    create_data_warehouse()
