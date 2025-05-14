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



# local config

config_directory = os.getcwd() + '/../local/'
exec(open(config_directory+'config.py').read())
print()



# script

lib = MusicLibrary()
print(lib.load_playlist_backlog(40))
print(lib.process_a_backlogged_playlist())
print(lib.data['playlist'])
print()
for object_name in lib.data:
    statement = lib.generate_create_table_statement(object_name)
    print(statement)
    print()
    print(lib.execute_database_statement(statement))
    print()
    print(lib.write_object_data_to_db(object_name))
    print()