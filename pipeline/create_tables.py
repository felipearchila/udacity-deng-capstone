import configparser
import psycopg2
import sql_queries


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

    redshift = initialize_connection(
        config['DWH']['DWH_ENDPOINT'],
        config['DWH']['DWH_PORT'],
        config['DWH']['DWH_DB'],
        config['DWH']['DWH_DB_USER'],
        config['DWH']['DWH_DB_PASSWORD'])

    create_queries = [sql_queries.create_dim_date, sql_queries.create_dim_time, sql_queries.create_dim_borough,
                      sql_queries.create_dim_vehicle, sql_queries.create_dim_registrationstate,
                      sql_queries.create_dim_violation, sql_queries.create_dim_precinct,
                      sql_queries.create_dim_issuingagency, sql_queries.create_fact_parkingviolation,
                      sql_queries.create_stage_parkingviolation, sql_queries.create_stage_issuingagency,
                      sql_queries.create_stage_precinct, sql_queries.create_stage_registrationstate,
                      sql_queries.create_stage_vehicle, sql_queries.create_stage_violation]

    table_names = ['stage_violation', 'stage_vehicle', 'stage_registrationstate', 'stage_precinct', 'stage_issuingagency',
                   'stage_parkingviolation', 'fact_parkingviolation', 'dim_issuingagency', 'dim_precinct',
                   'dim_violation', 'dim_registrationstate', 'dim_vehicle', 'dim_borough', 'dim_time', 'dim_date']

    with redshift.cursor() as crsr:
        for table in table_names:
            crsr.execute(sql_queries.drop_sql_format.format(table))
            redshift.commit()

        for query in create_queries:
            crsr.execute(query)
            redshift.commit()

    redshift.close()

if __name__ == "__main__":
    create_data_warehouse()
