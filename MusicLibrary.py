import json
import os
import pandas
import psycopg2
import random
import spotipy
import sys
import traceback
import time
import uuid
from spotipy.oauth2 import SpotifyOAuth


class MusicLibrary:

    def __init__(self):
        
        self.data = {
            # objects
            'album': {},
            'artist': {},
            'genre': {},
            'label': {},
            'playlist': {},
            'track': {},
            # maps
            'album_genre': {},
            'album_label': {},
            'album_track': {},
            'artist_genre': {},
            'playlist_track': {},
            'track_artist': {},
            # utility
            'playlist_backlog': {},
        }
        self.words = []
        self.spotify_connection = spotipy.Spotify(auth_manager=SpotifyOAuth(scope="playlist-read-private"))

    def create_uuid(self, **kwargs):
        input_string = ''
        namespace = uuid.NAMESPACE_DNS
        for key, value in kwargs.items():
            input_string += str(key) + ':' + str(value) + ';'
        return str(uuid.uuid5(namespace, input_string))

    def add(self, object_name, key, value):
        success = False
        try:
            if object_name in self.data:
                self.data[object_name][key] = value
                success = True
            else:
                print('Error: named object does not exist')
        except Exception as e:
            print(e)
        return success
        
    def get(self, object_name, key=None):
        obj = None
        if object_name in self.data:
            obj = self.data[object_name]
        else:
            print('Error: named object does not exist')
        if key is None:
            return obj
        elif key in obj:
            return obj[key]
        else:
            return None

    def delete(self, object_name, key):
        success = False
        try:
            if object_name in self.data:
                del self.data[object_name][key]
                success = True
            else:
                print('Error: named object does not exist')
        except Exception as e:
            print(e)
        return success

    def check(self, object_name, key, instance=None):
        success = False
        lib_instance = self.get(object_name, key)
        if instance is not None and lib_instance is not None:
            if lib_instance == instance:
                success = True
        elif instance is None and lib_instance is not None:
            success = True
        return success

    def fetch(self, object_name, key):
        obj = None
        try:
            if object_name == 'album':
                obj = self.spotify_connection.album(key)
            elif object_name == 'artist':
                obj = self.spotify_connection.artist(key)
            elif object_name == 'playlist':
                obj = self.spotify_connection.playlist(key)
            elif object_name == 'track':
                obj = self.spotify_connection.track(key)
            time.sleep(1)
        except Exception as e:
            print(e)
            time.sleep(2)
        return obj

    def write(self, **kwargs):
        success = False
        temp_data = dict(self.data)
        try:
            for obj_name, obj_data in kwargs['metadata'].items():
                for key, value in obj_data.items():
                    if self.check(obj_name, key) is True:
                        self.delete(obj_name, key)
                    self.add(obj_name, key, value)
            success = True
        except Exception as e:
            self.data = temp_data
            print(e)
        return success

    def process_genre(self, key, **kwargs):

        # evaluate existing metadata object
        if 'metadata' in kwargs:
            metadata = kwargs['metadata']
        else:
            metadata = {}
            
        # evaluate existing object type
        if 'genre' not in metadata:
            metadata['genre'] = {}

        # add the passed value
        try:
            metadata['genre'][key] = kwargs['value']
        except Exception as e:
            metadata = None
            print(e)

        return metadata

    def process_label(self, key, **kwargs):

        # evaluate existing metadata object
        if 'metadata' in kwargs:
            metadata = kwargs['metadata']
        else:
            metadata = {}
            
        # evaluate existing object type
        if 'label' not in metadata:
            metadata['label'] = {}

        # add the passed value
        try:
            metadata['label'][key] = kwargs['value']
        except Exception as e:
            metadata = None
            print(e)

        return metadata

    def process_map(self, map_name, key, value, **kwargs):

        # evaluate existing metadata object
        if 'metadata' in kwargs:
            metadata = kwargs['metadata']
        else:
            metadata = {}
            
        # evaluate existing object type
        if map_name not in metadata:
            metadata[map_name] = {}

        # add the passed value
        metadata[map_name][key] = value

        return metadata

    def process_album(self, key, **kwargs):

        # evaluate existing metadata object
        if 'metadata' in kwargs:
            metadata = kwargs['metadata']
        else:
            metadata = {}

        # evaluate existing object type
        if 'album' not in metadata:
            metadata['album'] = {}

        # translate track data to object
        if 'value' in kwargs:
            value = kwargs['value']
        else:
            value = self.fetch('album', key)
            if value is None:
                return None
        album_object = {
            'name': value['name'],
            'release_date': value['release_date'],
            'url': value['external_urls']['spotify'],
            'total_tracks': value['total_tracks'],
            'popularity': value['popularity'],
        }
            
        # fetch album associated objects to process
        genre_ids = []
        album_genre_ids = []
        for genre in value['genres']:
            genre_id = self.create_uuid(genre=genre)
            genre_ids.append(genre_id)
            ag_id = self.create_uuid(album=key, genre=genre_id)
            album_genre_ids.append(ag_id)
        label_id = self.create_uuid(label=value['label'])
        album_label_id = self.create_uuid(album=key, label=label_id)

        # test if album data already exists in library
        add_data = False
        if self.check('album', key, album_object) is True:
            if self.check('label', label_id) is False:
                add_data = True
            if self.check('album_label', album_label_id) is False:
                add_data = True
            for genre_id in genre_ids:
                if add_data is True:
                    break
                if self.check('genre', genre_id) is False:
                    add_data = True
            for album_genre_id in album_genre_ids:
                if add_data is True:
                    break
                if self.check('album_genre', album_genre_id) is False:
                    add_data = True
        else:
            add_data = True

        # conditionally append metadata for writing
        if add_data is True:
            metadata['album'][key] = album_object
            al_value = {
                'album_id': key,
                'label_id': label_id,
            }
            metadata = self.process_map('album_label', album_label_id, al_value, metadata=metadata)
            metadata = self.process_label(label_id, metadata=metadata, value={'name': value['label']})
            for genre in value['genres']:
                if metadata is None:
                    break  
                genre_id = self.create_uuid(genre=genre)
                ag_id = self.create_uuid(album=key, genre=genre_id)
                ag_value = {
                    'album_id': key,
                    'genre_id': genre_id,
                }
                metadata = self.process_map('album_genre', ag_id, ag_value, metadata=metadata)
                metadata = self.process_genre(genre_id, metadata=metadata, value={'name': genre})

        return metadata

    def process_artist(self, key, **kwargs):

        # evaluate existing metadata object
        if 'metadata' in kwargs:
            metadata = kwargs['metadata']
        else:
            metadata = {}

        # evaluate existing object type
        if 'artist' not in metadata:
            metadata['artist'] = {}

        # translate track data to object
        if 'value' in kwargs:
            value = kwargs['value']
        else:
            value = self.fetch('artist', key)
            if value is None:
                return None
        artist_object = {
            'name': value['name'],
            'url': value['external_urls']['spotify'],
            'followers': value['followers']['total'],
            'popularity': value['popularity'],
        }
            
        # fetch artist associated objects to process
        genre_ids = []
        artist_genre_ids = []
        for genre in value['genres']:
            genre_id = self.create_uuid(genre=genre)
            genre_ids.append(genre_id)
            ag_id = self.create_uuid(artist=key, genre=genre_id)
            artist_genre_ids.append(ag_id)

        # test if album data already exists in library
        add_data = False
        if self.check('artist', key, artist_object) is True:
            for genre_id in genre_ids:
                if add_data is True:
                    break
                if self.check('genre', genre_id) is False:
                    add_data = True
            for artist_genre_id in artist_genre_ids:
                if add_data is True:
                    break
                if self.check('artist_genre', artist_genre_id) is False:
                    add_data = True
        else:
            add_data = True

        # conditionally append metadata for writing
        if add_data is True:
            metadata['artist'][key] = artist_object
            for genre in value['genres']:
                if metadata is None:
                    break  
                genre_id = self.create_uuid(genre=genre)
                ag_id = self.create_uuid(artist=key, genre=genre_id)
                ag_value = {
                    'artist_id': key,
                    'genre_id': genre_id,
                }
                metadata = self.process_map('artist_genre', ag_id, ag_value, metadata=metadata)
                metadata = self.process_genre(genre_id, metadata=metadata, value={'name': genre})

        return metadata

    def process_track(self, key, **kwargs):

        # evaluate existing metadata object
        if 'metadata' in kwargs:
            metadata = kwargs['metadata']
        else:
            metadata = {}

        # evaluate existing object type
        if 'track' not in metadata:
            metadata['track'] = {}

        # translate track data to object
        if 'value' in kwargs:
            value = kwargs['value']
        else:
            value = self.fetch('track', key)
            if value is None:
                return None
        track_object = {
            'name': value['name'],
            'duration_ms': value['duration_ms'],
            'popularity': value['popularity'],
            'url': value['external_urls']['spotify'],
            'isrc': value['external_ids']['isrc'],
        }
            
        # fetch track associated objects to process
        artist_ids = []
        track_artist_ids = []
        for artist in value['artists']:
            artist_ids.append(artist['id'])
            ta_id = self.create_uuid(track=key, artist=artist['id'])
            track_artist_ids.append(ta_id)
        album_id = value['album']['id']
        album_track_id = self.create_uuid(album=value['album']['id'], track=key)

        # test if track data already exists in library
        add_data = False
        if self.check('track', key, track_object) is True:
            if self.check('album', album_id) is False:
                add_data = True
            if self.check('album_track', album_track_id) is False:
                add_data = True
            for artist_id in artist_ids:
                if add_data is True:
                    break
                if self.check('artist', artist_id) is False:
                    add_data = True
            for track_artist_id in track_artist_ids:
                if add_data is True:
                    break
                if self.check('track_artist', track_artist_id) is False:
                    add_data = True
        else:
            add_data = True

        # conditionally append metadata for writing
        if add_data is True:
            metadata['track'][key] = track_object
            at_value = {
                'album_id': value['album']['id'],
                'track_id': key,
                'track_number': value['track_number'],
            }
            metadata = self.process_map('album_track', album_track_id, at_value, metadata=metadata)
            metadata = self.process_album(value['album']['id'], metadata=metadata)
            for artist_data in value['artists']:
                if metadata is None:
                    break  
                ta_id = self.create_uuid(track=key, artist=artist_data['id'])
                ta_value = {
                    'track_id': key,
                    'artist_id': artist_data['id'],
                    'type': artist_data['type'],
                }
                metadata = self.process_map('track_artist', ta_id, ta_value, metadata=metadata)
                metadata = self.process_artist(artist_data['id'], metadata=metadata)

        return metadata

    def process_playlist(self, key, **kwargs):

        # evaluate existing metadata object
        if 'metadata' in kwargs:
            metadata = kwargs['metadata']
        else:
            metadata = {}

        # evaluate existing object type
        if 'playlist' not in metadata:
            metadata['playlist'] = {}

        # translate playlist data to object
        if 'value' in kwargs:
            value = kwargs['value']
        else:
            value = self.fetch('playlist', key)
            if value is None:
                return None
        playlist_object = {
            'name': value['name'],
            'description': value['description'],
            'url': value['external_urls']['spotify'],
            'total_tracks': value['tracks']['total'],
        }

        # set limit
        if value['tracks']['total'] > 100:
            limit = 100
        else:
            limit = value['tracks']['total']
            
        # fetch playlist tracks to process
        try:
            n = 0
            c = limit
            track_ids = []
            playlist_track_ids = []
            while c == limit:
                pt_fetch = self.spotify_connection.playlist_tracks(key, limit=limit, offset=n*limit)
                time.sleep(1)
                i = 0
                for track_data in pt_fetch['items']:
                    track_ids.append(track_data['track']['id'])
                    pt_id = self.create_uuid(playlist=key, track=track_data['track']['id'])
                    playlist_track_ids.append(pt_id)
                    i += 1
                n += 1
                c = i 
            
        except Exception as e:
            print(e)
            time.sleep(2)
            return None

        # test if playlist data already exists in library
        add_data = False
        if self.check('playlist', key, playlist_object) is True:
            for track_id in track_ids:
                if add_data is True:
                    break
                if self.check('track', track_id) is False:
                    add_data = True
            for playlist_track_id in playlist_track_ids:
                if add_data is True:
                    break
                if self.check('playlist_track', playlist_track_id) is False:
                    add_data = True
        else:
            add_data = True

        # conditionally append metadata for writing
        if add_data is True:
            metadata['playlist'][key] = playlist_object
            for track_data in pt_fetch['items']:
                pt_id = self.create_uuid(playlist=key, track=track_data['track']['id'])
                pt_value = {
                    'playlist_id': key,
                    'track_id': track_data['track']['id'],
                    'added_at': track_data['added_at'],
                }
                metadata = self.process_map('playlist_track', pt_id, pt_value, metadata=metadata)
                metadata = self.process_track(track_data['track']['id'], metadata=metadata)
                if metadata is None:
                    break                
            
        return metadata

    def load_playlist_backlog(self, limit=20):
        success = False

        # protect against invalid limits
        if limit > 50:
            limit = 50
            print('Limit truncated to 50.  Do not attempt a higher limit!')
        elif limit < 1:
            limit = 1
            print('Limit truncated to 1.  Do not attempt a lower limit!')

        try:
            # load data into local memory
            temp_backlog = {}
            n = 0
            c = limit
            while c == limit:
                results = self.spotify_connection.current_user_playlists(limit=limit, offset=n*limit)
                time.sleep(1)
                i = 0
                for idx, item in enumerate(results['items']):
                    temp_backlog[item['id']] = item
                    i += 1
                n += 1
                c = i

            # process local data into class object
            for key, value in temp_backlog.items():
                if key in self.data['playlist_backlog']:
                    del self.data['playlist_backlog'][key]
                self.data['playlist_backlog'][key] = value
            success = True
                
        except Exception as e:
            print(e)
            time.sleep(2)

        return success

    def process_a_backlogged_playlist(self):
        success = False

        # choose the next playlist
        key = next(iter(self.data['playlist_backlog']))
        value = self.data['playlist_backlog'][key]

        # process the playlist into metadata
        metadata = self.process_playlist(key, value=value)

        # conditionally write the metadata and clean up
        if metadata is not None:
            self.write(metadata=metadata)
            del self.data['playlist_backlog'][key]
            success = True
        elif metadata is None and self.check('playlist', key) is True:
            del self.data['playlist_backlog'][key]

        return success

    def extract_words(self):
        success = False
    
        # words from existing music library
        if len(self.data['track']) >= 1:
            for key, value in self.data['track'].items():
                for word in value['name'].split():
                    if word.lower() not in self.words:
                        self.words.append(word.lower())
                        success = True
            
        # random english words from a site
        # else:
        #     response = requests.get("https://www.mit.edu/~ecprice/wordlist.10000")
        #     byte_words = response.content.splitlines()
        #     for bw in byte_words:
        #         words.append(bw.decode('utf-8'))

        return success

    def get_word_query(self):
        if len(self.words) >= 1:
            return f'%{random.choice(self.words)}%'
        else:
            return 'house'

    def get_random_track(self, query=None):
        if query is None:
            query = self.get_word_query()
        try:
            pre_search = self.spotify_connection.search(q=query, type='track', market='US')
            time.sleep(1)
            random_offset = random.randrange(pre_search['tracks']['total'])
            results = self.spotify_connection.search(q=query, type='track', offset=random_offset, market='US')
            time.sleep(1)
            item = results['tracks']['items'][0]
            return item
        except Exception as e:
            traceback.print_exception(*sys.exc_info())
            time.sleep(2)
            return None
        
    def write_data_to_json(self):
        success = False
        try:
            config_directory = os.getcwd() + '/../local/'
            filepath = os.path.join(config_directory, "spotipy_data.json")
            with open(filepath, 'w') as f:
                json.dump(self.data, f, indent=4)
            success = True
        except Exception as e:
            print(e)
        return success

    def open_db_connection(self):
        self.db_connection = psycopg2.connect(
            dbname=os.environ['LOCAL_DB_DBNAME'],
            user=os.environ['LOCAL_DB_USER'],
            password=os.environ['LOCAL_DB_PASSWORD'],
            host=os.environ['LOCAL_DB_HOST'], 
            port=os.environ['LOCAL_DB_PORT'],
        )
        self.cursor = self.db_connection.cursor()

    def close_db_connection(self):
        self.cursor.close()
        self.db_connection.close()

    def read_data_from_json(self):
        success = False
        try:
            config_directory = os.getcwd() + '/../local/'
            filepath = os.path.join(config_directory, "spotipy_data.json")
            with open(filepath, 'r') as file:
                data = json.load(file)
            success = self.write(metadata=data)
        except Exception as e:
            print(e)
        return success
    
    def get_object_dataframe(self, object_name):
        objects = self.get(object_name)
        object_array = []
        for key, item in objects.items():
            object_instance = {'id': key}
            if object_name == 'playlist_backlog':
                object_instance['data'] = item
            else:
                for field, value in item.items():
                    object_instance[field] = value
            object_array.append(object_instance)
        return pandas.DataFrame.from_records(object_array)

    def get_max_object_field_length(self, object_name, field_name):
        objects = self.get(object_name)
        max_length = 0
        if objects is not None and len(objects) >= 1:
            for key, value in objects.items():
                if field_name == 'id' and len(str(key)) > max_length:
                        max_length = len(str(key))
                elif field_name != 'id' and len(str(value[field_name])) > max_length:
                        max_length = len(str(value[field_name]))
        if max_length == 0:
            # use a default value that can fit a date
            max_length = 10
        return max_length

    def generate_create_table_statement(self, object_name):
        df = self.get_object_dataframe(object_name)
        statement = """DROP TABLE IF EXISTS """ + object_name + """;\n"""
        statement += """CREATE TABLE """ + object_name + """ (\n"""
        for column in df.columns:
            if column != df.columns[0]:
                statement += """,\n"""
            statement += """\t""" + column
            if object_name == 'playlist_backlog' and column == 'data':
                statement += """ JSON"""
            elif df[column].dtype == 'int64':
                statement += """ INTEGER"""
            else:
                statement += """ VARCHAR("""
                statement += str(self.get_max_object_field_length(object_name, column))
                statement += """)"""
        statement += """\n);"""
        return statement

    def execute_database_statement(self, statement, *args):
        query_data = {
            'status': False, 
            'result': None,
            'columns': None,
            'message': None,
        }
        self.open_db_connection()
        try:
            # revist this later for security
            # executed_statement = statement % args
            # print(f'Database statement: {executed_statement}')

            self.cursor.execute(statement, args)
            if "SELECT " in statement:
                query_data['result'] = self.cursor.fetchall()
                query_data['columns'] = [desc[0] for desc in self.cursor.description]
            else:
                query_data['message'] = str(self.cursor.rowcount) + ' row' 
                if self.cursor.rowcount != 1:
                    query_data['message'] += 's'
                query_data['message'] += ' affected'
                self.db_connection.commit()
            query_data['status'] = True
        except Exception as e:
            self.db_connection.rollback()
            print(e)
        self.close_db_connection()
        return query_data

    def test_table_definition(self, object_name):
        success = False
        errors = False
        test_statement = """
                SELECT column_name, character_maximum_length
                FROM information_schema.columns
                WHERE table_name = %s;
                """
        test_args = (object_name)
        test_query = self.execute_database_statement(test_statement, test_args)
        if test_query['status'] is True and test_query['result'] is not None:
            for col in test_query['result']:
                col_name, char_max_len = col
                if char_max_len is not None:
                    if char_max_len < self.get_max_object_field_length(object_name, col_name):
                        statement = self.generate_create_table_statement(object_name)
                        query = self.execute_database_statement(statement)
                        if query['status'] is False:
                            errors = True
                        break
            if errors is False:
                success = True
        return success

    def write_object_data_to_db(self, object_name):
        success = False
        if self.test_table_definition(object_name) is True:
            id_statement = f"""SELECT id FROM {object_name}"""
            id_query = self.execute_database_statement(id_statement)
            id_list = list(map(lambda row: row[0], id_query['result']))
            if id_query['status'] is True:
                for key, item in self.data[object_name].items():
                    if key not in id_list:
                        c = 'id'
                        v = "'" + key + "'"
                        for field, value in item.items():
                            if value is not None:
                                c += ', ' + field
                                if type(value) == str:
                                    v += ", '" + value.replace("'","") + "'"
                                else:
                                    v += ', ' + str(value)
                        if object_name == 'playlist_backlog':
                            statement = """INSERT INTO playlist_backlog (id, data) VALUES (%s, %s);"""
                            args = (key, json.dumps(item))
                            query = self.execute_database_statement(statement, *args)
                        else:
                            statement = """INSERT INTO """ + object_name + """(""" + c + """) VALUES (""" + v + """);"""
                            query = self.execute_database_statement(statement)
                        if query['status'] is False:
                            return False
                    success = True
            else:
                success = False
        return success

    def read_object_data_from_db(self, object_name):
        success = False
        statement = """SELECT * FROM """ + object_name + """;"""
        query = self.execute_database_statement(statement)
        if query['status'] is True:
            for row in query['results']:
                if object_name == 'playlist_backlog':
                    row_data = row[1]
                else:
                    row_data = {}
                    for i, col_name in enumerate(query['columns']):
                        if col_name != 'id':
                            row_data[col_name] = row[i]
                self.data[object_name][row[query['columns'].index('id')]] = row_data
            success = True
        return success

    def delete_removed_objects_from_db(self, object_name):
        success = False
        if len(self.data[object_name]) == 0:
            clear_statement = """DELETE FROM """ + object_name + """;"""
            clear_query = self.execute_database_statement(clear_statement)
            success = clear_query['status']
        else:
            id_statement = """SELECT id FROM """ + object_name + """;"""
            id_query = self.execute_database_statement(id_statement)
            if id_query['status'] is True:
                success = True
                for row in id_query['result']:
                    if row[id_query['columns'].index('id')] not in self.data[object_name]:
                        delete_statement = """DELETE FROM """ + object_name + """ WHERE id = '""" + row[0] + """';"""
                        delete_query = self.execute_database_statement(delete_statement)
                        if delete_query['status'] is False:
                            success = False
        return success

    def generate_schema(self):
        success = False
        errors = False
        for object_name in self.data:
            statement = self.generate_create_table_statement(object_name)
            if self.execute_database_statement(statement) == False:
                errors = True
        if errors == False:
            success = True
        return success

    def save_data_to_db(self):
        success = False
        errors = False

        # first write new data
        for object_name in self.data:
            if len(self.data[object_name]) >= 1:
                if self.write_object_data_to_db(object_name) == False:
                    errors = True

        # second delete removed keys
        for object_name in self.data:
            if self.delete_removed_objects_from_db(object_name) == False:
                errors = True

        if errors == False:
            success = True
        return success

    def load_data_from_db(self):
        success = False
        errors = False
        for object_name in self.data:
            if self.read_object_data_from_db(object_name) == False:
                errors = True
        if errors == False:
            success = True
        return success