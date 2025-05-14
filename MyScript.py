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



# script

lib = MusicLibrary()
print(lib.get_random_track())