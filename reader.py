import h5py
import os
import tables
import math

data_path = './data_small'

def get_dirs(path):
    return [ name for name in os.listdir(path) if os.path.isdir(os.path.join(str(path), name))]

def get_files(path):
    return [f for f in os.listdir(path) if f.endswith('.h5')]

def get_lat_long_artist(path):
    f = tables.open_file(path, mode='r')
    f_root = f.root.metadata.songs.cols
    data = (f_root.artist_latitude[0], f_root.artist_longitude[0], f_root.artist_name[0].decode('UTF-8'))
    f.close()
    return data

all_file_paths = []
artist_neighbors = {}

root = get_dirs('%s' % data_path)

for h1 in root:
    first_level = get_dirs('%s/%s' % (data_path, h1))
    for h2 in first_level:
        second_level = get_dirs('%s/%s/%s' % (data_path, h1, h2))
        for h3 in second_level:
            filenames = get_files('%s/%s/%s/%s' % (data_path, h1, h2, h3))
            for file in filenames:
                path = '%s/%s/%s/%s/%s' % (data_path, h1, h2, h3, file)
                all_file_paths.append(path)

num_files = len(all_file_paths)
tenth_of_files = math.floor(num_files / 10)
print('scanned %d files' % num_files)

for p1 in all_file_paths:
    count = 0
    lat1, long1, a1 = get_lat_long_artist(p1)
    if math.isnan(lat1) or math.isnan(long1):
        continue
    for p2 in all_file_paths:
        count += 1
        if count % tenth_of_files == 0:
            print('#', end='', flush=True)
        lat2, long2, a2 = get_lat_long_artist(p2)
        if math.isnan(lat2) or math.isnan(long2):
            continue
    print(' completed %s' % a1)
