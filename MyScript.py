# imports

import json
import os
import pandas
import psycopg2
import random
import requests
import spotipy
import sys
import traceback
import time
import uuid
from MusicLibrary import MusicLibrary
from spotipy.oauth2 import SpotifyOAuth

####################



# local config

config_directory = os.getcwd() + '/../local/'
exec(open(config_directory+'config.py').read())
print()

####################



# functions

def begin(method='any'):
    library = MusicLibrary()
    if method == 'db' or method == 'any':
        print('Database load: ' + str(library.load_data_from_db()))
    if method == 'json' or (method == 'any' and len(library.data['track']) == 0):
        print('JSON read: ' + str(library.read_data_from_json()))
    if method == 'backlog' or (method == 'any' and len(library.data['track']) == 0):
        print('Spotify Playlist Log: ' + str(library.load_playlist_backlog()))
    print()
    print(library)
    print()
    return library

def backlog(library, n=1):
    p = 0
    while p < n:
        success = library.process_a_backlogged_playlist()
        print('Playlist Processing: ' + str(success))
        if success is True:
            p += 1
    print()

def end(library, method='all'):
    if method == 'json' or method == 'all':
        print('JSON write: ' + str(library.write_data_to_json()))
    if method == 'db' or method == 'all':
        print('DB write: ' + str(library.save_data_to_db()))
    print()

####################



# script

lib = begin('json')

backlog(lib, 5)

# print('Generating DB Schema: ' + str(lib.generate_schema()))

# print(lib.get_random_track())

end(lib, 'all')

####################