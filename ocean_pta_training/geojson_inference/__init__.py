from .shortest_path import (
    get_poi_flag,
    get_subnetwork,
    get_closest_node_on_network,
    get_shortest_path_length,
    get_shortest_path_length_iter
)
import logging
import os
import pandas as pd
import pickle

logger = logging.getLogger(f"{__name__}")
this_dir_path = os.path.abspath(os.path.dirname(__file__))

# POI input file
poi_file_path = os.path.join(this_dir_path, "poi_list.csv")
poi = pd.read_csv(poi_file_path)
poi_latlon = poi['latlon'].apply(eval).values.tolist()
poi_names = poi['poi'].values.tolist()

# Load all the pickled objects
obj_infile = open(os.path.join(this_dir_path, "shortest_path_objects.pkl"), "rb")
obj_names_infile = open(os.path.join(this_dir_path, "shortest_path_objects_names.pkl"), "rb")
obj_list_names = pickle.load(obj_names_infile)
exec(obj_list_names+'='+'pickle.load(obj_infile)')
obj_infile.close()
obj_names_infile.close()


def calculate_shortest_path(input_df: pd.DataFrame):
    """TODO"""
    expected_columns = ["olat", "olon", "dlat", "dlon"]
    for column in expected_columns:
        if column not in input_df.columns:
            message = f"input_df lacks the required column {column}. Cannot continue."
            logger.error(message)
            raise ValueError(message)
        elif not pd.api.types.is_numeric_dtype(input_df[column]):
            message = f"input_df required column {column} is not numeric. Cannot continue."
            logger.error(message)
            raise ValueError(message)

    input_source = input_df[['olat', 'olon']].values.tolist()
    input_dest = input_df[['dlat', 'dlon']].values.tolist()

    (
        shortest_paths,
        haversine_distance, ocean_distance,
        distance_from_source_to_network, distance_to_dest_from_network,
        points_of_interest

    ) = get_shortest_path_length_iter(
        network_obj, nodes_hexes, poi_latlon,
        input_source, input_dest
    )
    return {
        'shortest_paths': shortest_paths,
        'haversine_distance': haversine_distance,
        'ocean_distance': ocean_distance,
        'distance_from_source_to_network': distance_from_source_to_network,
        'distance_to_dest_from_network': distance_to_dest_from_network,
        'points_of_interest': points_of_interest
    }
