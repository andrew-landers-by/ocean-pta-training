import logging
import os
from ocean_pta_training import Environment, pyodbc_connect

logger = logging.getLogger(__name__)


def main():
    with pyodbc_connect(autocommit=True) as conn:
        create_ocean_journeys_features_table(conn)
        create_ocean_journeys_response_table(conn)


def create_ocean_journeys_features_table(conn):
    """Create the Synapse table for ocean journey features"""
    schema = os.environ.get(Environment.Vars.SYNAPSE_SCHEMA)
    table = os.environ.get(Environment.Vars.OCEAN_JOURNEY_FEATURES_TABLE)

    cur = conn.cursor()

    # Drop existing table (if exists)
    sql = f"""IF OBJECT_ID(N'{schema}.{table}') IS NOT NULL DROP TABLE {schema}.{table}"""
    logger.info(f"Dropping table {schema}.{table} (if exists) using SQL query:\n\t{sql}")
    try:
        cur.execute(sql)
    except Exception as e:
        logger.error(f"Drop existing table failed due to {type(e).__name__}: {e}")

    # Create new table
    sql = f"""
        CREATE TABLE {schema}.{table}
            (
                IMO integer,
                OD varchar(12),
                unique_route_id integer,
                time_position datetime,
                elapsed_time float,
                olat float,
                olon float,
                dlat float,
                dlon float,
                ocean_distance float,
                source_to_network_dist float,
                network_to_dest_dist float,
                babelmandeb bit,
                bering bit,
                corinth bit,
                dover bit,
                gibraltar bit,
                kiel bit,
                magellan bit,
                malacca bit,
                northeast bit,
                northwest bit,
                panama bit,
                suez bit
            );
    """
    logger.info(f"Creating table {schema}.{table} using SQL query:\n{sql}")
    try:
        cur.execute(sql)
    except Exception as e:
        logger.error(f"Creation of table {schema}.{table} failed due to {type(e).__name__}: {e}")


def create_ocean_journeys_response_table(conn):
    """Create the Synapse table for ocean journey response variable(s)"""
    schema = os.environ.get(Environment.Vars.SYNAPSE_SCHEMA)
    table = os.environ.get(Environment.Vars.OCEAN_JOURNEY_RESPONSE_TABLE)

    cur = conn.cursor()

    # Drop existing table (if exists)
    sql = f"""IF OBJECT_ID(N'{schema}.{table}') IS NOT NULL DROP TABLE {schema}.{table}"""
    logger.info(f"Dropping table {schema}.{table} (if exists) using SQL query:\n\t{sql}")
    try:
        cur.execute(sql)
    except Exception as e:
        logger.error(f"Drop existing table failed due to {type(e).__name__}: {e}")

    sql = f"""
        CREATE TABLE {schema}.{table}
        (
            IMO integer,
            MMSI integer,
            OD varchar(12),
            unique_route_id int,
            time_position datetime,
            elapsed_time float,
            remaining_lead_time float
        );
    """
    logger.info(f"Creating table {schema}.{table} using sql query:\n{sql}")
    try:
        cur.execute(sql)
    except Exception as e:
        logger.error(f"Creation of table {schema}.{table} failed due to {type(e).__name__}: {e}")


if __name__ == "__main__":
    main()
