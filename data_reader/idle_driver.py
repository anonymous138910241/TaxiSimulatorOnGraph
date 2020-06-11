from h3 import h3
import pandas as pd
import numpy as np
import random


def get_total_driver_count(input_file_name, output_file_name, is_idle=False):
    data = pd.read_csv(input_file_name)
    converted_data = data.groupby(['time_stamp']).agg('sum').reset_index()
    print(converted_data)
    if is_idle:
        converted_data['total_driver_count_mean'] = converted_data['idle_driver_count_mean'].apply(lambda x:1.217*x)
    #converted_data = pd.DataFrame(list(enumerate()),columns=['road_id', 'idle_driver_count'])
    converted_data.to_csv(output_file_name, columns=['time_stamp', 'total_driver_count_mean'], index=False)

import math

def convert_driver_distribution_from_h3_coord_to_road(cell_id_data_set, input_file_name, output_file_name,
                                                      target_column_name ='idle_driver_count'):
    data = pd.read_csv(input_file_name)
    drivers_initial_distributions = data[data['time_stamp'] == 0]
    drivers_initial_distributions_road = {}

    for index, row in drivers_initial_distributions.iterrows():
        cell_id = row['cell_id']
        idle_driver_count = int(row[target_column_name] + 0.5)
        if cell_id not in cell_id_data_set:
            print(idle_driver_count, cell_id)
            continue

        for k in range(idle_driver_count):
            r = random.choice(cell_id_data_set[cell_id])
            if r not in drivers_initial_distributions_road:
                drivers_initial_distributions_road[r] = 0
            drivers_initial_distributions_road[r] += 1

    converted_data = pd.DataFrame(list(drivers_initial_distributions_road.items()), columns=['road_id', 'idle_driver_count'])
    converted_data.to_csv(output_file_name, columns=['road_id', 'idle_driver_count'], index=False)

