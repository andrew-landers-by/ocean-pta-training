
# Ocean PTA
### Feature Extraction (revised for all ODs)
#### Feb. 14, 2022

***

## Dataset: Ocean Journeys

- **Records:** 9.02 million
- **IMOs (unique):** 4,239
- **OD (Origin, Destination) pairs:** 954
- **Journeys (IMO, Origin, Destination):** 244,998
- **Environment:**
    - *Synapse server:* `lana-synapse-dev-01`
    - *Schema:* `ocean_vessel_movement`
    - *Tables:*
        - `OCEAN_JOURNEY_FEATURES` (Extract in Python > Datalake > Synapse)
        - `OCEAN_JOURNEYS_RESPONSE` (Extract in Python > Datalake > Synapse)
        - `OCEAN_PORTS` (Entity table: ports)
        - `OCEAN_VESSELS` (Entity table: vessels)
        - `OCEAN_JOURNEYS` (Machine learning dataset: derived from above)


### **Dimensions** 

- IMO (ship ID)
- Port of origin
- Port of discharge
- Observation number (one IMO, one OD, one historical journey)
- Timestamp


### **Prediction Target (Response):**

- `remaining_lead_time` (days)


### **Candidate Features** 
    
- **Vessel Characteristics**
    - Ship Class (e.g. 'NEW PANAMAX', 'SMALL FEEDER')
    - Width
    - Length
    - Year Build
    - Draught
- **Journey Information**
    - Origin Region (global)
    - Destination Region (global)
    - Within region journey (flag)
    - Elapsed time (days)
    - Current latitude (degrees)
    - Current longitude (degrees)
    - Destination latitude (degrees)
    - Destination longitude (degrees)
- **Ocean Network Shortest Path**
    - Estimated ocean distance (graph network)
    - Distance: (current location, graph network)
    - Distance: (graph network, destination)
    - Total ocean distance (estimated) = 1. + 2. + 3. above
- **'Point of Interest' flags**
    - `babelmandeb`
    - `bering` (Bering Strait)
    - `corinth` (Corinth canal, Greece)
    - `dover` (Dover Strait)
    - `gibralter` (Strait of Gibralter)
    - `kiel` (Kiel Canal, Germany)
    - `magellan` (Strait of Magellan)
    - `malacca` (Malacca, Malaysia/Indonesia/Singapore)
    - `northeast` ( )
    - `northwest` ( )
    - `panama` (Panama Canal)
    - `suez` (Suez Canal)