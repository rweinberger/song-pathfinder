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

PORT_NUMBER = 27017
HOST = 'localhost'
DATA_PATH = './data_small'
START_TIME = datetime.now()
MAX_HOP_KM = 500
CORES = 4

# class Song(Document):
#     title = StringField(required=True, max_length=200)
#     artist = StringField(required=True)
#     neighbors = ListField(ReferenceField(Song, reverse_delete_rule=mongoengine.PULL))

def get_dirs(path):
    return [ name for name in os.listdir(path) if os.path.isdir(os.path.join(str(path), name))]

def get_files(path):
    return [f for f in os.listdir(path) if f.endswith('.h5')]

def get_distance_coordinate(lat_1, long_1, lat_2, long_2): # haversine formula
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
    """ Flattens the DATA_PATH directory into a list of all contained file paths. """
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

def build_data(paths, i_start, i_end):
    """
    Builds two dictionaries from the given data paths:
        artists = {
            artist_name: { song_name: (lat, long) }, ... }
        }
        artist_neighbors = {
            artist_name: [ ( artist_name, song_name ), ...]
        }
    """
    artists = {}
    artist_neighbors = {}
    num_files = len(paths)
    count = 0

    for p1 in paths[i_start:i_end]:
        count += 1
        lat1, long1, a1, s1 = get_lat_long_artist_song(p1)
        if math.isnan(lat1) or math.isnan(long1):
            continue
        if a1 in artists:
            artists[a1].update({s1: (lat1, long1)})
        else:
            artists[a1] = {s1: (lat1, long1)}
        artist_neighbors[a1] = []
        for p2 in paths:
            if p2 != p1:
                lat2, long2, a2, s2 = get_lat_long_artist_song(p2)
                if math.isnan(lat2) or math.isnan(long2):
                    continue
                dist = get_distance_coordinate(lat1, long1, lat2, long2)
                if dist <= MAX_HOP_KM:
                    artist_neighbors[a1].append((a2, s2))
        progress(count, 80, a1)
    # print('locations unavailable for', unavailable_locations)
    # print('\nElapsed time:',datetime.now() - START_TIME)
    return [artists, artist_neighbors]

def worker(paths, i_start, i_end, send_pipe):
    data = build_data(paths, i_start, i_end)
    send_pipe.send(data)

def find_indices(i, n, c):
    increment = math.ceil(n/c)
    start = i * increment
    end = start + increment
    if end >= n:
        end = None
    return (start, end)

def execute_workers(cores, paths):
    send_pipes = []
    recv_pipes = []
    processes = []
    num_paths = len(paths)
    # split_paths = np.array_split(np.array(paths), cores, axis=0)
    for i in range(cores):
        recv_pipe, send_pipe = multiprocessing.Pipe(False)
        send_pipes.append(send_pipe)
        recv_pipes.append(recv_pipe)
    for i in range(cores):
        i_start, i_end = find_indices(i, num_paths, cores)
        processes.append(multiprocessing.Process(target=worker, args=(paths, i_start, i_end, send_pipes[i])))
    for process in processes:
        process.start()
    for process in processes:
        process.join()

    artists = {}
    artist_neighbors = {}

    for i in range(cores):
        new_artists, new_neighbors = recv_pipes[i].recv()
        artists.update(new_artists)
        artist_neighbors.update(new_neighbors)

    # print(len(result))
    return (artists, artist_neighbors)

if __name__ == "__main__":
    client = MongoClient(HOST, PORT_NUMBER)
    db = client.music_graph
    db.artists.drop()
    db.neighbors.drop()

    paths = find_file_paths()
    artists, artist_neighbors = execute_workers(CORES, paths)

    print('\ndata scraped')

    for artist in artists:
        entry = {
            "artist": artist,
            "songs": artists[artist]
        }
        db.artists.insert(entry)

    for artist in artist_neighbors:
        entry = {
            "artist": artist,
            "neighbors": artist_neighbors[artist]
        }
        db.neighbors.insert(entry)

    print('collections filled')
    # distances = build_data(paths)
    # std = np.std(distances)
    # avg = np.average(distances)
    # print(std)
    # print(avg)




