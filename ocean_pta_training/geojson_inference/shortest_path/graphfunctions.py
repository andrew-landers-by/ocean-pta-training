import h3.api.numpy_int as h3
import numpy as np
from igraph import Graph
from haversine import haversine, haversine_vector, Unit
import math

def get_poi_flag(poi_latlon, edge_list_mapped, nodes_master_reversed, shortest_path):
    shortest_path_mapped_edges = [edge_list_mapped[edge][0] for edge in shortest_path[0]]
    shortest_path_edges = [nodes_master_reversed[node] for node in shortest_path_mapped_edges]
    poi_flag = []
    for pairs in poi_latlon:
        poi_pairs_flag = np.max(np.where((np.min(haversine_vector(pairs, shortest_path_edges, Unit.KILOMETERS, comb=True), axis = 0)) < 5, 1 ,0)) ### poi within 5km
        poi_flag.append(poi_pairs_flag)
    return poi_flag

def get_subnetwork(nodes_master, nodes_hexes, node):
    node_hex_reso4 = h3.geo_to_h3(node[0],node[1],4)
    node_hex_reso3 = h3.geo_to_h3(node[0],node[1],3)
    node_hex_reso2 = h3.geo_to_h3(node[0],node[1],2)
    node_hex_reso1 = h3.geo_to_h3(node[0],node[1],1)
    if (node_hex_reso4 in nodes_hexes[0]):
        hex4nodes = [i for i, x in enumerate(nodes_hexes[0]) if x==node_hex_reso4]
        network = [nodes_master[i] for i in hex4nodes]
    elif (node_hex_reso3 in nodes_hexes[1]):
        hex3nodes = [i for i, x in enumerate(nodes_hexes[1]) if x==node_hex_reso3]
        network = [nodes_master[i] for i in hex3nodes]
    elif (node_hex_reso2 in nodes_hexes[2]):
        hex2nodes = [i for i, x in enumerate(nodes_hexes[2]) if x==node_hex_reso2]
        network = [nodes_master[i] for i in hex2nodes]
    elif (node_hex_reso1 in nodes_hexes[3]):
        hex1nodes = [i for i, x in enumerate(nodes_hexes[3]) if x==node_hex_reso1]
        network = [nodes_master[i] for i in hex1nodes]
    else:
        network = nodes_master
    return network

def get_closest_node_on_network(nodes_master_reversed, nodes_hexes, input):
    closest_nodes = []
    closest_nodes_dist = []
    for node in input:        
        node_network = get_subnetwork(nodes_master_reversed, nodes_hexes, node)
        dist_vector = haversine_vector(node_network, node, Unit.KILOMETERS, comb=True) #lat/long
        min_index = np.argmin(dist_vector, axis = 1)[0]
        min_dist =  np.min(dist_vector, axis = 1)[0]
        closest_nodes.append(node_network[min_index]) 
        closest_nodes_dist.append(min_dist)
    return closest_nodes,np.array(closest_nodes_dist)


def get_shortest_path_length(network_obj, nodes_hexes, source, dest):
    g, edge_list, edge_list_mapped, weights, nodes_mapping, nodes_master, nodes_master_reversed, nodes_mapping = network_obj
    d_od = haversine_vector(source, dest, Unit.KILOMETERS, comb=False)
    No,d_to_No = get_closest_node_on_network(nodes_master_reversed, nodes_hexes, source)
    Nd,d_to_Nd = get_closest_node_on_network(nodes_master_reversed, nodes_hexes, dest)
    source_indices = [nodes_mapping[str([i[1],i[0]])] for i in No] ##reversed for haversine vector
    target_indices = [nodes_mapping[str([i[1],i[0]])] for i in Nd] ##reversed for haversine vector
    d_No_to_Nd = g.shortest_paths(source= source_indices, target=target_indices, weights = weights, mode='all')
    distance = np.where((d_od <= (d_to_No + d_to_Nd)), d_od, d_No_to_Nd)
    distance_from_source_to_network = np.where((d_od <= (d_to_No + d_to_Nd)), 0, d_to_No)
    distance_to_dest_from_network  = np.where((d_od <= (d_to_No + d_to_Nd)), 0, d_to_Nd)
    return d_od, distance, distance_from_source_to_network, distance_to_dest_from_network

def get_shortest_path_length_iter(network_obj, nodes_hexes, poi_latlon, source, dest):
    g, edge_list, edge_list_mapped, weights, nodes_mapping, nodes_master, nodes_master_reversed, nodes_mapping = network_obj
    d_od = haversine_vector(source, dest, Unit.KILOMETERS, comb=False)
    No,d_to_No = get_closest_node_on_network(nodes_master_reversed, nodes_hexes, source)
    Nd,d_to_Nd = get_closest_node_on_network(nodes_master_reversed, nodes_hexes, dest)
    source_indices = [nodes_mapping[str([i[1],i[0]])] for i in No] ##reversed for haversine vector
    target_indices = [nodes_mapping[str([i[1],i[0]])] for i in Nd] ##reversed for haversine vector
    d_No_to_Nd = []
    shortest_paths=[]
    poi_flag_master = []
    for i in range(0, len(source_indices)):
        if source_indices[i] == target_indices[i]:
            shortest_path = []
        else:
            shortest_path = g.get_shortest_paths(source_indices[i], to=target_indices[i],weights=g.es["weight"],output="epath",)
        if len(shortest_path)>0:
            distance = np.sum([weights[edge] for edge in shortest_path[0]])
            d_No_to_Nd.append(distance)
            shortest_paths.append(shortest_path)
            poi_flag = get_poi_flag(poi_latlon, edge_list_mapped, nodes_master_reversed, shortest_path)
            poi_flag_master.append(poi_flag)
        else:
            try:
                d_No_to_Nd.append(np.inf)
                shortest_paths.append([])
                poi_flag = [0 for _ in range(len(poi_latlon))]
                poi_flag_master.append(poi_flag)
                # poi_flag.append([0 for _ in range(len(poi_latlon))])  # np.zeros(poi_latlon)
            except Exception as e:
                print(f"Error: {e}")
    distance = np.where((d_od <= (d_to_No + d_to_Nd)), d_od, d_No_to_Nd)
    distance_from_source_to_network = np.where((d_od <= (d_to_No + d_to_Nd)), 0, d_to_No)
    distance_to_dest_from_network  = np.where((d_od <= (d_to_No + d_to_Nd)), 0, d_to_Nd)
    return shortest_paths, d_od, distance, distance_from_source_to_network, distance_to_dest_from_network, poi_flag_master