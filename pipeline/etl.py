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


def run_pipeline():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    redshift = initialize_connection(
        config['DWH']['DWH_ENDPOINT'],
        config['DWH']['DWH_PORT'],
        config['DWH']['DWH_DB'],
        config['DWH']['DWH_DB_USER'],
        config['DWH']['DWH_DB_PASSWORD'])

    stage_tables = ['stage_issuingagency', 'stage_precinct', 'stage_registrationstate',
                    'stage_vehicle', 'stage_violation'] #'stage_parkingviolation'

    with redshift.cursor() as crsr:
        for table in stage_tables:
            crsr.execute(sql_queries.truncate_sql_format.format(table))
            redshift.commit()

        for table in stage_tables:
            crsr.execute(sql_queries.copy_sql_format.format(
                table,
                's3://farchila-udacity-final/' + table.replace('stage_', ''),
                config['AWS']['KEY'],
                config['AWS']['SECRET'],
                'CSV',
                '1'))
            redshift.commit()

    redshift.close()


if __name__ == "__main__":
    run_pipeline()
