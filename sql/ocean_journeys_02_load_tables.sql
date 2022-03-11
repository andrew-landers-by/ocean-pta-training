
--Upload data from storage account to [ocean_vessel_movement].[OCEAN_JOURNEY_FEATURES]
COPY INTO [ocean_vessel_movement].[OCEAN_JOURNEY_FEATURES] (
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
FROM 'https://oceanptastorelanadev01.blob.core.windows.net/file-share/ocean_journeys/features'
WITH (
	CREDENTIAL=(
        IDENTITY='Storage Account Key',
        SECRET='81Jzccp09Jv7N6RBEWhQGTCNFbSRXcaoHYHjrRYz/aJtvUDMsCU2rQopeDi08s9W7F5tsJUWsfsP9vjypKgdFg=='
    ),
	FILE_TYPE='CSV',
	FIRSTROW='2',
	ROWTERMINATOR='0x0a'
);


--Upload data from storage account to [ocean_vessel_movement].[OCEAN_JOURNEY_RESPONSE]
COPY INTO [ocean_vessel_movement].[OCEAN_JOURNEY_RESPONSE] (
    [IMO],
    [MMSI],
    [OD],
    [unique_route_id],
    [time_position],
    [elapsed_time],
    [remaining_lead_time]
)
FROM 'https://oceanptastorelanadev01.blob.core.windows.net/file-share/ocean_journeys/response'
WITH (
	CREDENTIAL=(
        IDENTITY='Storage Account Key',
        SECRET='81Jzccp09Jv7N6RBEWhQGTCNFbSRXcaoHYHjrRYz/aJtvUDMsCU2rQopeDi08s9W7F5tsJUWsfsP9vjypKgdFg=='
    ),
	FILE_TYPE='CSV',
	FIRSTROW='2',
	ROWTERMINATOR='0x0a'
);


--Load data into [ocean_vessel_movement].[OCEAN_VESSELS]
COPY INTO [ocean_vessel_movement].[OCEAN_VESSELS]
(
    [IMO],
    [SHIPNAME],
    [SHIPTYPE],
    [WIDTH],
    [LENGTH],
    [YEAR_BUILT],
    [DRAUGHT],
    [TEU],
    [SHIPCLASS]
)
FROM 'https://oceanptastorelanadev01.blob.core.windows.net/file-share/ocean_vessels'
WITH (
	CREDENTIAL=(
        IDENTITY='Storage Account Key',
        SECRET='<<storage_account_key>>'
    ),
	FILE_TYPE='CSV',
	FIRSTROW='2',
	ROWTERMINATOR='0x0a'
);


-- Load data into [ocean_vessel_movement].[OCEAN_PORTS]
COPY INTO [ocean_vessel_movement].[OCEAN_PORTS]
(
    [locode],
    [lat],
    [lon],
    [country],
    [location],
    [port_name_alternative],
    [h3_4],
    [h3_5],
    [mapped_locode],
    [mt_port_id],
    [port_country],
    [port_name],
    [area_global],
    [area_local]
)
FROM 'https://oceanptastorelanadev01.blob.core.windows.net/file-share/ocean_ports'
WITH (
	CREDENTIAL=(
        IDENTITY='Storage Account Key',
        SECRET='<<storage_account_key>>'
    ),
	FILE_TYPE='CSV',
	FIRSTROW='2',
	ROWTERMINATOR='0x0a'
);

-- Create derived table OCEAN_JOURNEYS
IF OBJECT_ID(N'[ocean_vessel_movement].[OCEAN_JOURNEYS]') IS NOT NULL
BEGIN
DROP TABLE [ocean_vessel_movement].[OCEAN_JOURNEYS]
END
GO
CREATE TABLE [ocean_vessel_movement].[OCEAN_JOURNEYS]
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
        FROM [ocean_vessel_movement].[OCEAN_JOURNEY_FEATURES]
    ) AS F

    LEFT JOIN [ocean_vessel_movement].[OCEAN_JOURNEY_RESPONSE] AS R
        ON F.IMO=R.IMO AND F.OD=R.OD
        AND F.unique_route_id=R.unique_route_id
        AND F.elapsed_time=R.elapsed_time
    
    LEFT JOIN [ocean_vessel_movement].[OCEAN_VESSELS] AS V
        ON F.IMO=V.IMO
    
    LEFT JOIN [ocean_vessel_movement].[OCEAN_PORTS] AS P1
        ON F.origin_port=P1.locode
    
    LEFT JOIN [ocean_vessel_movement].[OCEAN_PORTS] AS P2
        ON F.destination_port=P2.locode
);
SELECT COUNT(*) FROM [ocean_vessel_movement].[OCEAN_JOURNEYS];
SELECT TOP 10000 * FROM [ocean_vessel_movement].[OCEAN_JOURNEYS] ORDER BY IMO, OD, unique_route_id, elapsed_time;