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
print(lib.load_data_from_db())
print()

for object_name in lib.data:
    print(object_name + ': ' + str(len(lib.data[object_name])))
print()



# print(lib.read_data_from_json())
# print(lib.load_playlist_backlog(40))
# print(lib.process_a_backlogged_playlist())
# print(lib.data['playlist'])
# print(len(lib.data['playlist_backlog']))
# print()

# s = lib.generate_create_table_statement('playlist_backlog')
# print(s)
# print(lib.execute_database_statement(s))
# print(lib.write_object_data_to_db('playlist_backlog'))
# print()

# for object_name in lib.data:
#     statement = lib.generate_create_table_statement(object_name)
#     print(statement)
#     print()
#     print(lib.execute_database_statement(statement))
#     print()
#     print(lib.write_object_data_to_db(object_name))
#     print()