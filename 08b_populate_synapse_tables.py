import logging
import os
from ocean_pta_training import Environment, pyodbc_connect

logger = logging.getLogger(__name__)


def main():
    try:
        populate_tables()
    except Exception as e:
        logger.error(
            f"Populating Synapse tables terminated due to an unexpected error of type {type(e).__name__}: {e}"
        )


def populate_tables():
    """TODO"""
    with pyodbc_connect(autocommit=True) as conn:
        populate_features_table(conn)
        populate_response_table(conn)


def populate_features_table(conn):
    """TODO"""
    schema = os.environ.get(Environment.Vars.SYNAPSE_SCHEMA)
    table = os.environ.get(Environment.Vars.OCEAN_JOURNEY_FEATURES_TABLE)

    truncate_table(conn, schema, table)

    unredacted_sql = sql_query_to_populate_features_table(redact=False)
    redacted_sql = sql_query_to_populate_features_table(redact=True)
    try:
        cur = conn.cursor()
        logger.info(f"Loading table {schema}.{table} using sql query:\n{redacted_sql}")
        cur.execute(unredacted_sql)
    except Exception as e:
        logger.error(
            f"Loading table {schema}.{table} from CSV files failed due to {type(e).__name__}: {e}"
        )


def populate_response_table(conn):
    """TODO"""
    schema = os.environ.get(Environment.Vars.SYNAPSE_SCHEMA)
    table = os.environ.get(Environment.Vars.OCEAN_JOURNEY_RESPONSE_TABLE)

    truncate_table(conn, schema, table)

    unredacted_sql = sql_query_to_populate_response_table(redact=False)
    redacted_sql = sql_query_to_populate_response_table(redact=True)
    try:
        cur = conn.cursor()
        logger.info(f"Loading table {schema}.{table} using sql query:\n{redacted_sql}")
        cur.execute(unredacted_sql)
    except Exception as e:
        logger.error(
            f"Loading table {schema}.{table} from CSV files failed due to {type(e).__name__}: {e}"
        )


def sql_query_to_populate_features_table(redact: bool = False):
    """TODO"""
    schema = os.environ.get(Environment.Vars.SYNAPSE_SCHEMA)
    table = os.environ.get(Environment.Vars.OCEAN_JOURNEY_FEATURES_TABLE)
    storage_acct_key = os.environ.get(Environment.Vars.BLOB_SERVICE_ACCESS_KEY)
    storage_acct_url = os.environ.get(Environment.Vars.BLOB_SERVICE_URL)
    container = os.environ.get(Environment.Vars.BLOB_SERVICE_CONTAINER_NAME)
    data_files_subdir = os.environ.get(Environment.Vars.OCEAN_JOURNEY_FEATURES_BLOB_SUBDIR)
    url = f"{storage_acct_url}/{container}/{data_files_subdir}"
    sql_template = f"""
        COPY INTO {{schema}}.{{table}} (
            [IMO],
            [OD],
            [unique_route_id],
            [time_position],
            [elapsed_time],
            [olat],
            [olon],
            [dlat],
            [dlon],
            [ocean_distance],
            [source_to_network_dist],
            [network_to_dest_dist],
            [babelmandeb],
            [bering],
            [corinth],
            [dover],
            [gibraltar],
            [kiel],
            [magellan],
            [malacca],
            [northeast],
            [northwest],
            [panama],
            [suez]
        )
        FROM '{{url}}'
        WITH (
            CREDENTIAL=(
                IDENTITY='Storage Account Key',
                SECRET='{{storage_acct_key}}'
            ),
            FILE_TYPE='CSV',
            FIRSTROW='2',
            ROWTERMINATOR='0x0a'
        );
    """
    if redact:
        return sql_template.format(
            schema=schema,
            table=table,
            url=url,
            storage_acct_key="REDACTED"
        )
    else:
        return sql_template.format(
            schema=schema,
            table=table,
            url=url,
            storage_acct_key=storage_acct_key
        )


def sql_query_to_populate_response_table(redact: bool = False):
    """TODO"""
    schema = os.environ.get(Environment.Vars.SYNAPSE_SCHEMA)
    table = os.environ.get(Environment.Vars.OCEAN_JOURNEY_RESPONSE_TABLE)
    storage_acct_key = os.environ.get(Environment.Vars.BLOB_SERVICE_ACCESS_KEY)
    storage_acct_url = os.environ.get(Environment.Vars.BLOB_SERVICE_URL)
    container = os.environ.get(Environment.Vars.BLOB_SERVICE_CONTAINER_NAME)
    data_files_subdir = os.environ.get(Environment.Vars.OCEAN_JOURNEY_RESPONSE_BLOB_SUBDIR)
    url = f"{storage_acct_url}/{container}/{data_files_subdir}"
    sql_template = f"""
        COPY INTO {{schema}}.{{table}} (
            [IMO],
            [MMSI],
            [OD],
            [unique_route_id],
            [time_position],
            [elapsed_time],
            [remaining_lead_time]
        )
        FROM '{{url}}'
        WITH (
            CREDENTIAL=(
                IDENTITY='Storage Account Key',
                SECRET='{{storage_acct_key}}'
            ),
            FILE_TYPE='CSV',
            FIRSTROW='2',
            ROWTERMINATOR='0x0a'
        );
    """
    if redact:
        return sql_template.format(
            schema=schema,
            table=table,
            url=url,
            storage_acct_key="REDACTED"
        )
    else:
        return sql_template.format(
            schema=schema,
            table=table,
            url=url,
            storage_acct_key=storage_acct_key
        )


def truncate_table(conn, schema, table):
    """TODO"""
    cur = conn.cursor()
    sql = f"TRUNCATE TABLE {schema}.{table};"
    logger.info(f"Truncating table {schema}.{table} using sql query:\n\t{sql}")
    try:
        cur.execute(sql)
    except Exception as e:
        logger.error(f"Truncating table {schema}.{table} failed due to {type(e).__name__}: {e}")


if __name__ == "__main__":
    main()
