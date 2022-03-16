import logging
import os
from ocean_pta_training import Environment, pyodbc_connect

logger = logging.getLogger(__name__)


def main():
    try:
        with pyodbc_connect(autocommit=True) as conn:
            drop_combined_training_data_table_if_it_exists(conn)
            create_combined_training_data_table(conn)
    except Exception as e:
        logger.error(
            f"Populating Synapse tables terminated due to an unexpected error of type {type(e).__name__}: {e}"
        )


def drop_combined_training_data_table_if_it_exists(conn):
    """See function name"""
    schema = os.environ.get(Environment.Vars.SYNAPSE_SCHEMA)
    table = os.environ.get(Environment.Vars.OCEAN_JOURNEY_DATA_TABLE)
    sql = f"""IF OBJECT_ID(N'{schema}.{table}') IS NOT NULL DROP TABLE {schema}.{table}"""
    try:
        cur = conn.cursor()
        logger.info(f"Dropping table {schema}.{table} (if exists) using SQL query:\n\t{sql}")
        cur.execute(sql)
    except Exception as e:
        logger.error(f"Drop existing table failed due to {type(e).__name__}: {e}")

def create_combined_training_data_table(conn):
    """See function name"""
    sql = create_ocean_journeys_table_sql_query()

    try:
        logger.info(f"Assembling combined training data using SQL query:\n{sql}")
        cur = conn.cursor()
        cur.execute(sql)
    except Exception as e:
        logger.error(
            f"Ocean Journeys CTAS command failed due to {type(e).__name__}: {e}"
        )


def create_ocean_journeys_table_sql_query():
    """See function name"""
    schema = os.environ.get(Environment.Vars.SYNAPSE_SCHEMA)
    ocean_journeys_table = os.environ.get(Environment.Vars.OCEAN_JOURNEY_DATA_TABLE)
    ocean_journey_features_table = os.environ.get(Environment.Vars.OCEAN_JOURNEY_FEATURES_TABLE)
    ocean_journey_response_table = os.environ.get(Environment.Vars.OCEAN_JOURNEY_RESPONSE_TABLE)
    ocean_ports_table = os.environ.get(Environment.Vars.OCEAN_PORTS_TABLE)
    ocean_vessels_table = os.environ.get(Environment.Vars.OCEAN_VESSELS_TABLE)

    return f"""
        CREATE TABLE {schema}.{ocean_journeys_table}
        WITH
        (
            DISTRIBUTION = ROUND_ROBIN
           ,CLUSTERED COLUMNSTORE INDEX
        )
        AS
        (
            SELECT
                F.IMO
               ,V.SHIPTYPE AS ship_type
               ,REPLACE(V.SHIPCLASS, CHAR(13), '') AS ship_class  -- delete carriage return
               ,V.WIDTH AS width
               ,V.LENGTH AS length
               ,YEAR(GETDATE()) - V.YEAR_BUILT AS vessel_age_years
               ,V.DRAUGHT AS draught
               ,F.OD
               ,F.origin_port
               ,F.destination_port
               ,P1.area_global AS origin_region
               ,P2.area_global AS destination_region
               ,CASE WHEN P1.area_global=P2.area_global THEN 1 ELSE 0 END AS within_region_journey
               ,F.unique_route_id
               ,F.time_position
               ,F.elapsed_time
               ,F.olat AS latitude
               ,F.olon AS longitude
               ,F.dlat AS destination_latitude
               ,F.dlon AS destination_longitude
               ,F.ocean_distance
               ,F.source_to_network_dist
               ,F.network_to_dest_dist
               ,F.ocean_distance + F.source_to_network_dist + F.source_to_network_dist AS remaining_distance
               ,F.babelmandeb
               ,F.bering
               ,F.corinth
               ,F.dover
               ,F.gibraltar
               ,F.kiel
               ,F.magellan
               ,F.malacca
               ,F.northeast
               ,F.northwest
               ,F.panama
               ,F.suez
               ,R.remaining_lead_time
        
            FROM (
                SELECT *,
                    SUBSTRING(OD, 1, 5) AS origin_port,
                    SUBSTRING(OD, 7, 5) AS destination_port
                FROM {schema}.{ocean_journey_features_table}
            ) AS F
        
            LEFT JOIN {schema}.{ocean_journey_response_table} AS R
                ON F.IMO=R.IMO AND F.OD=R.OD
                AND F.unique_route_id=R.unique_route_id
                AND F.elapsed_time=R.elapsed_time
            
            LEFT JOIN {schema}.{ocean_vessels_table} AS V
                ON F.IMO=V.IMO
            
            LEFT JOIN {schema}.{ocean_ports_table} AS P1
                ON F.origin_port=P1.locode
            
            LEFT JOIN {schema}.{ocean_ports_table} AS P2
                ON F.destination_port=P2.locode
        );
    """


if __name__ == "__main__":
    main()
