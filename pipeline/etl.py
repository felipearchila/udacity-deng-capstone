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


def run_pipeline():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    etl_queries = SqlQueries()

    redshift = initialize_connection(
        config['DWH']['DWH_ENDPOINT'],
        config['DWH']['DWH_PORT'],
        config['DWH']['DWH_DB'],
        config['DWH']['DWH_DB_USER'],
        config['DWH']['DWH_DB_PASSWORD'])

    stage_tables = ['stage_issuingagency', 'stage_precinct', 'stage_registrationstate',
                    'stage_vehicle', 'stage_violation', 'stage_parking_violations']

    dim_tables = ['dim_vehicle', 'dim_registrationstate', 'dim_violation', 'dim_borough', 'dim_precinct',
                  'dim_issuingagency']

    with redshift.cursor() as crsr:
        for table in stage_tables:
            crsr.execute(etl_queries.truncate_sql_format.format(table))
            redshift.commit()

        for table in stage_tables:
            if table == 'stage_parking_violations':
                crsr.execute(etl_queries.copy_sql_format.format(
                    table,
                    's3://farchila-udacity-final/' + table.replace('stage_', ''),
                    config['AWS']['KEY'],
                    config['AWS']['SECRET'],
                    'JSON \'auto\''))
            else:
                crsr.execute(etl_queries.copy_sql_format.format(
                    table,
                    's3://farchila-udacity-final/' + table.replace('stage_', ''),
                    config['AWS']['KEY'],
                    config['AWS']['SECRET'],
                    'CSV\r\nIGNOREHEADER 1'))

            redshift.commit()

        for table in dim_tables:
            crsr.execute(etl_queries.get_sql_command(table, 'insert'))
            redshift.commit()

    redshift.close()


if __name__ == "__main__":
    run_pipeline()
