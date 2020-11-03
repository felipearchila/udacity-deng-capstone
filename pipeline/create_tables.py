import configparser
import psycopg2
from sql_queries import SqlQueries


def initialize_connection(endpoint, port_number, database_name, database_user, database_password):
    connection = psycopg2.connect('host={} port={} dbname={} user={} password={}'.format(
        endpoint,
        port_number,
        database_name,
        database_user,
        database_password))

    return connection


def create_data_warehouse():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    ddl_queries = SqlQueries()

    redshift = initialize_connection(
        config['DWH']['DWH_ENDPOINT'],
        config['DWH']['DWH_PORT'],
        config['DWH']['DWH_DB'],
        config['DWH']['DWH_DB_USER'],
        config['DWH']['DWH_DB_PASSWORD'])

    create_queries = [ddl_queries.create_dim_date, ddl_queries.create_dim_time, ddl_queries.create_dim_borough,
                      ddl_queries.create_dim_vehicle, ddl_queries.create_dim_registrationstate,
                      ddl_queries.create_dim_violation, ddl_queries.create_dim_precinct,
                      ddl_queries.create_dim_issuingagency, ddl_queries.create_fact_parkingviolation,
                      ddl_queries.create_stage_parking_violations, ddl_queries.create_stage_issuingagency,
                      ddl_queries.create_stage_precinct, ddl_queries.create_stage_registrationstate,
                      ddl_queries.create_stage_vehicle, ddl_queries.create_stage_violation]

    table_names = ['stage_violation', 'stage_vehicle', 'stage_registrationstate', 'stage_precinct', 'stage_issuingagency',
                   'stage_parking_violations', 'fact_parkingviolation', 'dim_issuingagency', 'dim_precinct',
                   'dim_violation', 'dim_registrationstate', 'dim_vehicle', 'dim_borough', 'dim_time', 'dim_date']

    with redshift.cursor() as crsr:
        for table in table_names:
            crsr.execute(ddl_queries.drop_sql_format.format(table))
            redshift.commit()

        for query in create_queries:
            crsr.execute(query)
            redshift.commit()

    redshift.close()


if __name__ == "__main__":
    create_data_warehouse()
