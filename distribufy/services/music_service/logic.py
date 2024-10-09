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
    def __init__(self, ip: str, db: JSONDatabase, sec_succ_db: JSONDatabase, succ_db: JSONDatabase, role: str, port: int = 8001, m: int = 160):
        super().__init__( ip, db,sec_succ_db, succ_db,role, port, m )
        self.role = role 
        self.ip = ip
        self.port = port

    def get_db(self):
        logger.debug(f'All data from node requested')
        data = self.data.get_all()
        logger.debug(f'Node data: \n{data}')
        return data

    def get_song_by_key(self, song_key): # TODO fix this
        '''
        Return a song store in the Chord Ring given a song_key
        '''
        song = self.song_key_node(song_key)
        print(song)
        if song != None:
            return song

        next_node = self.succ
        while next_node.id != self.id:
            try:
                logger.debug(f'Getting song from node {next_node.ip}')
                song = next_node.song_key_node(data_genre)
                logger.debug(song)
                if song != None:
                    return song
                next_node = next_node.succ
            except:
                logger.error(f'Error: Error getting songs from node in ip {next_node.ip}')
        return all_songs_by_genre

    def get_all_songs(self):
        '''
        Return all the songs available in the Chord Ring
        '''
        all_songs = []
        all_songs.extend(self.data.get_all())
        
        next_node = self.succ
        while next_node.id != self.id:
            try:
                logger.debug(f'Getting songs from node {next_node.ip}')
                songs = next_node.get_db()
                logger.debug(songs)
                all_songs.extend(songs)#TODO: Verify songs with same id arent being added
                next_node = next_node.succ
            except:
                logger.error(f'Error: Error getting songs from node in ip {next_node.ip}')
        return all_songs

    def song_key_node(self, data_key:str):
        key = data_key["key"]
        return self.data.query("key", key)

    def songs_title_node(self, data_title:str):
        title = data_title["title"]
        return self.data.query("title", title)
    
    def songs_artist_node(self, data_artist:str):
        artist = data_artist["artist"]
        return self.data.query("artist", artist)
    
    def songs_genre_node(self, data_genre:str):
        genre = data_genre["genre"]
        print(genre)
        return self.data.query("genre", genre)

    def get_songs_by_title(self, data_title:str):
        '''
        Filter the available songs by title
        '''
        title = data_title["title"]
        all_songs_by_artist = []
        all_songs_by_artist.extend(self.songs_title_node(data_title))

        next_node = self.succ
        while next_node.id != self.id:
            try:
                logger.debug(f'Getting songs from node {next_node.ip}')
                songs = next_node.songs_title_node(data_title)
                logger.debug(songs)
                all_songs_by_artist.extend(songs)#TODO: Verify songs with same id arent being added
                next_node = next_node.succ
            except:
                logger.error(f'Error: Error getting songs from node in ip {next_node.ip}')
        return all_songs_by_artist

    def get_songs_by_artist(self, data_artist):
        '''
        Filter the available songs by artist
        '''
        all_songs_by_artist = []
        all_songs_by_artist.extend(self.songs_artist_node(data_artist))

        next_node = self.succ
        while next_node.id != self.id:
            try:
                logger.debug(f'Getting songs from node {next_node.ip}')
                songs = next_node.songs_artist_node(data_artist)
                logger.debug(songs)
                all_songs_by_artist.extend(songs)#TODO: Verify songs with same id arent being added
                next_node = next_node.succ
            except:
                logger.error(f'Error: Error getting songs from node in ip {next_node.ip}')
        return all_songs_by_artist

    def get_songs_by_genre(self, data_genre):
        '''
        Filter the available songs by genre
        '''
        all_songs_by_genre = []
        all_songs_by_genre.extend(self.songs_genre_node(data_genre))
        next_node = self.succ
        while next_node.id != self.id:
            try:
                logger.debug(f'Getting songs from node {next_node.ip}')
                songs = next_node.songs_genre_node(data_genre)
                logger.debug(songs)
                all_songs_by_genre.extend(songs)#TODO: Verify songs with same id arent being added
                next_node = next_node.succ
            except:
                logger.error(f'Error: Error getting songs from node in ip {next_node.ip}')
        return all_songs_by_genre
