import pandas as pd
import osmnx as ox
import networkx as nx
import time
import numpy as np


def read_organized_order(file_name):
    '''
    remove coulmn name
    :param file_name:
    :return:
    '''
    order_data = pd.read_csv(file_name)
    # count = order_data.groupby(['origin_node_index', 'start_time_in_time_interval']).size()
    return order_data.values


def wgx_to_edge_index(G: nx.MultiDiGraph, data: pd.DataFrame, wgsx, wgsy):
    xs = data[wgsx].to_numpy()
    ys = data[wgsy].to_numpy()
    closest_edges = ox.get_nearest_edges(G, xs, ys, method='balltree')
    closest_edges = list(map(tuple, closest_edges))
    #closest_edges = list(map(lambda x: G.edges[x[0], x[1], 0]['osmid'], closest_edges))
    print(closest_edges[0:20])
    return pd.Series(closest_edges)

import random
from h3 import h3


def h3_to_edge_index(cell_id_data_set, cell_id):
    if cell_id not in cell_id_data_set.keys():
        #print("error", cell_id)
        h3.k_ring(cell_id, 1)
        cell_id = random.choice(list(cell_id_data_set.keys()))
    r = random.choice(cell_id_data_set[cell_id])
    return r


def get_orders_from_raw_data(G: nx.MultiDiGraph, input_file_name):
    '''
    :param input_file_name: Input file name
    :param transform_function: transform function that changes lat, lng to index
    :param time_interval: time interval
    :return: list of orders
    '''
    data = pd.read_csv(input_file_name)
    data['created_at'] = pd.to_datetime(data['created_at'], format="%Y%m%d%H%M%S")

    data.sort_values(by = ['created_at'], inplace=True)
    data.reset_index(drop=True, inplace=True)

    start_time = data['created_at'][0]
    data['start_time_in_minute'] = data['created_at'].apply(lambda x: int((x - start_time).total_seconds() // 60))

    start_time_timer = time.time()
    data['origin_node_index'] = wgx_to_edge_index(G, data, 'origin_wgsx', 'origin_wgsy')
    end_time_timer = time.time()
    print(end_time_timer - start_time_timer)

    data['destination_node_index'] = wgx_to_edge_index(G, data, 'destination_wgsx', 'destination_wgsy')

    data['duration_in_minute'] = data['seconds_to_end'].apply(lambda x: max(x//60, 1))

    data.rename(columns={'estimated_fare': 'price'}, inplace=True)
    header = ['origin_node_index', 'destination_node_index', 'start_time_in_minute', 'duration_in_minute', 'price']

    return data[header]


def export_order(data, output_file_name):
    '''
    Exports order data
    :param data: pandas data that has order info
    :param output_file_name: output file name
    :return: none
    '''
    header = ['origin_node_index', 'destination_node_index', 'start_time_in_minute', 'duration_in_minute', 'price']
    assert all(i in list(data) for i in header)
    data.to_csv(output_file_name, columns=header, index=False)


def get_orders_from_raw_data(G: nx.MultiDiGraph, input_file_name):
    '''
    :param input_file_name: Input file name
    :param transform_function: transform function that changes lat, lng to index
    :param time_interval: time interval
    :return: list of orders
    '''
    data = pd.read_csv(input_file_name)
    data['created_at'] = pd.to_datetime(data['created_at'], format="%Y%m%d%H%M%S")

    data.sort_values(by = ['created_at'], inplace=True)
    data.reset_index(drop=True, inplace=True)

    start_time = data['created_at'][0]
    data['start_time_in_minute'] = data['created_at'].apply(lambda x: int((x - start_time).total_seconds() // 60))

    start_time_timer = time.time()
    data['origin_node_index'] = wgx_to_edge_index(G, data, 'origin_wgsx', 'origin_wgsy')
    end_time_timer = time.time()
    print(end_time_timer - start_time_timer)

    data['destination_node_index'] = wgx_to_edge_index(G, data, 'destination_wgsx', 'destination_wgsy')

    data['duration_in_minute'] = data['seconds_to_end'].apply(lambda x: max(x//60, 1))

    data.rename(columns={'estimated_fare': 'price'}, inplace=True)
    header = ['origin_node_index', 'destination_node_index', 'start_time_in_minute', 'duration_in_minute', 'price']

    return data[header]


def convert_call_data_from_h3_coord_to_road(cell_id_data_set, input_file_name):
    data = pd.read_csv(input_file_name)

    data['start_time_in_minute'] = data['time_stamp'].apply(lambda x: x)

    data['origin_node_index'] = data['origin_cell_id'].apply(lambda x: h3_to_edge_index(cell_id_data_set, x))
    data['destination_node_index'] = data['destination_cell_id'].apply(lambda x: h3_to_edge_index(cell_id_data_set, x))

    data['duration_in_minute'] = data['duration'].apply(lambda x: max(x, 1))

    header = ['origin_node_index', 'destination_node_index', 'start_time_in_minute', 'duration_in_minute', 'price']

    return data[header]
