import psycopg2


def initialize_connection(endpoint, port_number, database_name, database_user, database_password):
    """
    Create and return a new connection to a cloud data warehouse with the provided parameters
    :param endpoint: the FQDN or IP address of the target server
    :param port_number: the port on which the database is listening
    :param database_name: name of the database
    :param database_user: username for the connection
    :param database_password: password for the connection
    :return: A psycopg2 connection object with which we can interact with the data warehouse
    """
    connection = psycopg2.connect('host={} port={} dbname={} user={} password={}'.format(
        endpoint,
        port_number,
        database_name,
        database_user,
        database_password))

    return connection
