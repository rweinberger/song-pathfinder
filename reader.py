import numpy as np
import h5py
import os
import sys
import tables
import math
import multiprocessing
from datetime import datetime
from pymongo import MongoClient
from mongoengine import *

PORT_NUMBER = 21017
HOST = 'localhost'
DATA_PATH = './data_small'
START_TIME = datetime.now()
MAX_HOP_KM = 500

def get_dirs(path):
    return [ name for name in os.listdir(path) if os.path.isdir(os.path.join(str(path), name))]

def get_files(path):
    return [f for f in os.listdir(path) if f.endswith('.h5')]

def get_distance_coordinate(lat_1, long_1, lat_2, long_2):
    #haversine formula
    EARTH_RADIUS = 6371
    d_lat = deg_to_rad(lat_2 - lat_1)
    d_long= deg_to_rad(long_2 - long_1)
    a = math.sin(d_lat/2) * math.sin(d_lat/2) + math.cos(deg_to_rad(lat_1)) * math.cos(deg_to_rad(lat_2)) * math.sin(d_long/2) * math.sin(d_long/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = EARTH_RADIUS * c
    return d

def deg_to_rad(deg):
    return deg * (math.pi/180)

def get_lat_long_artist_song(path):
    f = tables.open_file(path, mode='r')
    f_root = f.root.metadata.songs.cols
    data = (f_root.artist_latitude[0], f_root.artist_longitude[0], f_root.artist_name[0].decode('UTF-8'), f_root.title[0].decode('UTF-8'))
    f.close()
    return data

# adapted from https://gist.github.com/vladignatyev/06860ec2040cb497f0f3
def progress(count, total, status=''):
    bar_len = 60
    filled_len = int(round(bar_len * count / float(total)))

    percents = round(100.0 * count / float(total), 1)
    bar = '#' * filled_len + '.' * (bar_len - filled_len)
    time_diff = datetime.now() - START_TIME
    time_str = ' '+str(time_diff).replace(',', '')
    sys.stdout.write("\033[K")
    sys.stdout.write('%s [%s] %s%s   %s\r' % (time_str, bar, percents, '%', status))
    sys.stdout.flush()

def find_file_paths():
    paths = []
    root = get_dirs('%s' % DATA_PATH)

    for h1 in root:
        first_level = get_dirs('%s/%s' % (DATA_PATH, h1))
        for h2 in first_level:
            second_level = get_dirs('%s/%s/%s' % (DATA_PATH, h1, h2))
            for h3 in second_level:
                filenames = get_files('%s/%s/%s/%s' % (DATA_PATH, h1, h2, h3))
                for file in filenames:
                    path = '%s/%s/%s/%s/%s' % (DATA_PATH, h1, h2, h3, file)
                    paths.append(path)
    num_files = len(paths)
    print('%d files' % num_files)
    return paths

artists = {}
artist_neighbors = {}

def populate_db(paths):
    num_files = len(paths)
    unavailable_locations = []
    count = 0
    for p1 in paths:
        count += 1
        lat1, long1, a1, s1 = get_lat_long_artist_song(p1)
        if math.isnan(lat1) or math.isnan(long1):
            unavailable_locations.append(a1)
            continue
        artists[a1] = (a1, s1, lat1, long1)
        artist_neighbors[a1] = []
        for p2 in paths:
            if p2 != p1:
                lat2, long2, a2, s2 = get_lat_long_artist_song(p2)
                if math.isnan(lat2) or math.isnan(long2):
                    continue
                dist = get_distance_coordinate(lat1, long1, lat2, long2)
                if dist <= MAX_HOP_KM:
                    add_entry((a1, s1, lat1, long1), (a2, s2, lat2, long2))
        progress(count, num_files, a1)
    # print('locations unavailable for', unavailable_locations)
    print('\nElapsed time:',datetime.now() - START_TIME)

def add_entry(artist1, artist2):
    a1, s1, lat1, long1 = artist1
    a2, s2, lat2, long2 = artist2
    artist_neighbors[a1].append(artist2)

# class Song(Document):
#     title = StringField(required=True, max_length=200)
#     artist = StringField(required=True)
#     neighbors = ListField(ReferenceField(Song, reverse_delete_rule=mongoengine.PULL))
def worker(l, send_end):
    distances = populate_db(l)
    send_end.send(distances)

def execute_workers(cores, paths):
    pipes = []
    processes = []
    split_paths = np.array_split(np.array(paths), cores, axis=0)
    for i in range(cores):
        pipes.append(multiprocessing.Pipe(False))
    for i in range(cores):
        processes.append(multiprocessing.Process(target=worker, args=(split_paths[i], pipes[i][1])))
    for process in processes:
        process.start()
    for process in processes:
        process.join()

    result = []

    for i in range(cores):
        result.extend(pipes[i][0].recv())
    print(len(result))
    return result

if __name__ == "__main__":
    # client = MongoClient(HOST, PORT_NUMBER)
    # db = client['music_graph']
    # connect('music_graph', host=HOST, port=PORT_NUMBER)
    CORES = 4
    paths = find_file_paths()
    distances = execute_workers(CORES, paths)
    # distances = populate_db(paths)
    std = np.std(distances)
    avg = np.average(distances)
    print(std)
    print(avg)




