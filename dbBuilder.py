import pymongo

from pymongo import MongoClient
from mongoengine import *

class Song(Document):
    title = StringField(required=True, max_length=200)
    artist = StringField(required=True)
    neighbors = ListField(ReferenceField(Song, reverse_delete_rule=mongoengine.PULL))

class Builder():
    def __init__(self):
        entries = []
        processed_entries = []

    def physical_distance(entry_one, entry_two):
        #fix this formula later
        return ( (entry_one.longitude - entry_two.longitude)**2 + (entry_one.lattitude - entry_two.lattitude)**2)**0.5
    
    def is_neighbor(entry_one, entry_two, distance_fn, threshold):
        return distance_fn(entry_one, entry_two) < threshold
    
    def createSongEntry(entry, entries, distance_fn, threshold):
        found_neighbors = []
        for (e in entries):
            if ( (e != entry) and (is_neighbor(entry, e, distance_fn, threshold):
                found_neighbors.append(e)
        new_entry = Song(
                            title     = entry.title,
                            artist    = entry.artist,
                            neighbors = found_neighbors
                        )
        return new_entry

    def process_entries(Builder.physical_distance, threshold):
        p_entries = []
        for (entry in entries):
            p_entries.append(createSongEntry(entry, entries, Builder.physical_distance, threshold))

if __name__ == "__main__":
    PORT_NUMBER = 21017
    HOST = 'localhost'

    client = MongoClient(HOST, PORT_NUMBER)

    db = client['music_graph']

    connect('music_graph', host=HOST, port=PORT_NUMBER)



