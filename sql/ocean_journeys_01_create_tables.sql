
--Create OCEAN_JOURNEYS_FEATURES
IF OBJECT_ID(N'[ocean_vessel_movement].[OCEAN_JOURNEY_FEATURES]') IS NOT NULL
BEGIN
DROP TABLE [ocean_vessel_movement].[OCEAN_JOURNEY_FEATURES]
END
GO
CREATE TABLE [ocean_vessel_movement].[OCEAN_JOURNEY_FEATURES]
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


--Create OCEAN_JOURNEY_RESPONSE
IF OBJECT_ID(N'[ocean_vessel_movement].[OCEAN_JOURNEY_RESPONSE]') IS NOT NULL
BEGIN
DROP TABLE [ocean_vessel_movement].[OCEAN_JOURNEY_RESPONSE]
END
GO
CREATE TABLE [ocean_vessel_movement].[OCEAN_JOURNEY_RESPONSE]
(
    IMO integer,
    MMSI integer,
    OD varchar(12),
    unique_route_id int,
    time_position datetime,
    elapsed_time float,
    remaining_lead_time float
);


-- Create table OCEAN_VESSELS
IF OBJECT_ID(N'[ocean_vessel_movement].[OCEAN_VESSELS]') IS NOT NULL
BEGIN
DROP TABLE [ocean_vessel_movement].[OCEAN_VESSELS]
END
GO
CREATE TABLE [ocean_vessel_movement].[OCEAN_VESSELS]
(
    [IMO] integer,
    [SHIPNAME] varchar(60),
    [SHIPTYPE] integer,
    [WIDTH] float,
    [LENGTH] float,
    [YEAR_BUILT] integer,
    [DRAUGHT] float,
    [TEU] float,
    [SHIPCLASS] varchar(20)
    
);


-- Create table COPY INTO [ocean_vessel_movement].[OCEAN_PORTS]
IF OBJECT_ID(N'[ocean_vessel_movement].[OCEAN_PORTS]') IS NOT NULL
BEGIN
DROP TABLE [ocean_vessel_movement].[OCEAN_PORTS]
END
GO
CREATE TABLE [ocean_vessel_movement].[OCEAN_PORTS]
(
    [locode] varchar(8),
    [lat] float,
    [lon] float,
    [country] varchar(12),
    [location] varchar(12),
    [port_name_alternative] varchar(60),
    [h3_4] float,
    [h3_5] float,
    [mapped_locode] varchar(20),
    [mt_port_id] integer,
    [port_country] varchar(30),
    [port_name] varchar(30),
    [area_global] varchar(30),
    [area_local] varchar(30)
);
