import threading
import requests
import time
import logging
import os
from services.common.chord_node import ChordNode
from services.common.utils import get_sha_repr
from services.common.my_orm import JSONDatabase
from services.music_service.presentation import MusicNodePresentation
from http.server import HTTPServer

# Set up logging
logger = logging.getLogger("__main__")

class MusicNode(ChordNode):
    def __init__(self, ip: str, db: JSONDatabase, pred_db: JSONDatabase, succ_db: JSONDatabase, role: str, port: int = 8001, m: int = 160):
        super().__init__( ip, db,pred_db, succ_db,role, port, m )
        self.role = role 
        self.ip = ip
        self.port = port
        # Handler Init
        server_address = (self.ip, self.port)
        self.httpd = HTTPServer(server_address, MusicNodePresentation)
        self.httpd.node = self#TODO: Make it so this is set in ititialization
         
        
    def get_db(self):
        return self.data

    def get_song(self, song_key):
        '''
        Return a song store in the Chord Ring given a song_key
        '''
        return self.get_data(song_key, None)

    def save_song(self, key, title, album, genre, artist):
        '''
        Save a song in the Chord Ring
        '''
        
        song_data = {
            'key': key,
            'title': title,
            'album': album,
            'genre': genre,
            'artist': artist
        }
        
        try:
            # Almacena la canción en la base de datos usando la clave como identificador
            self.data.insert(song_data)

            print(f"Song '{title}' by '{artist}' successfully saved in the database.")
        except Exception as e:
            print(f"Error saving song '{title}' by '{artist}': {str(e)}")

    def get_all_songs(self):
            '''
            Return all the songs available in the Chord Ring
            '''
            all_songs = []
            all_songs.append(self.data.get_all())
            
            chord_node = self.succ
            while True:
                
                if chord_node.id == self.id:
                    return all_songs

                try:
                    songs = chord_node.get_songs()
                    all_songs.append(songs)
                    chord_node = chord_node.find_successor()

                except:
                    if not self.succ:
                        print(f'Error: Could not connect with chord node {self.chord_id}')
                        break
            
            return all_songs

    def get_songs_by_title(self, title):
        '''
        Filter the available songs by title
        '''
        all_songs = self.get_all_songs()
        songs = [song for song in all_songs if song["title"] == title]
        return songs

    def get_songs_by_author(self, artist):
        '''
        Filter the available songs by author
        '''
        all_songs = self.get_all_songs()
        songs = [song for song in all_songs if song["artist"] == artist]
        return songs

    def get_songs_by_gender(self, genre):
        '''
        Filter the available songs by gender
        '''
        all_songs = self.get_all_songs()
        songs = [song for song in all_songs if song["genre"] == genre]
        return songs
