import hashlib
import requests
import logging
from typing import List
from services.common.node_reference import ChordNodeReference

# Set up logging
logger = logging.getLogger("__main__")
logger_stab = logging.getLogger("__main__.st")
logger_ff = logging.getLogger("__main__.ff")
logger_cp = logging.getLogger("__main__.cp")
logger_rh = logging.getLogger("__main__.rh")
logger_le = logging.getLogger("__main__.le")
logger_dt = logging.getLogger("__main__.dt")

class GatewayReference(ChordNodeReference):
    # pass
    
    def notify(self, node):
        """Notify the node of a leader change."""
        self._send_request('/gw/notify', {'id': node.id, 'ip': node.ip})
    
    def save_song(self, data):
        self._send_request('/gw/save-song', data)
    
    def get_all_songs(self):
        response = self._send_request('/gw/get-songs', method='get')
        logger.debug(f'{response} id in response: {'id' in response}')
        return response
    
    def get_song_by_key(self, key):
        return self._send_request('/gw/get-song-by-key',data= key, method='post')
    
    def get_songs_by_title(self, title):
        return self._send_request('/gw/get-songs-by-title',data= title, method='post')
    
    def get_songs_by_artist(self, artist):
        return self._send_request('/gw/get-songs-by-artist',data= artist, method='post')
    
    def get_songs_by_genre(self, genre):
        return self._send_request('/gw/get-songs-by-genre',data= genre, method='post')
    
    def share_gw_knowledge(self, known_nodes: List['GatewayReference']):
        """Notify the new leader."""
        logger.debug(f'Sharing known gw nodes {known_nodes}')
        nodes = []
        for node in known_nodes:
            nodes.append({'ip': node.ip, 'port': node.port, 'id': node.id})
        self._send_request('/gw/share-gw-knowledge', {'nodes': nodes})

    def store_song_file(self, payload):#TODO
        """Initiate the file storage process by making an HTTP request."""
        try:
            response = self._send_request('/gw/store-song-file', payload)
            logger.info(f"Received UDP details for file transfer: {response}")
            return response
        except Exception as e:
            logger.error(f"Error initiating file storage: {e}")
            return None

    def send_song_file(self, song_title: str, client_ip: str, client_port: int, start_chunk: int):#TODO
        """
        Request to send a song file via UDP starting from a specific chunk.
        Args:
            song_key (str): The identifier of the song to send.
            udp_ip (str): The IP address of the UDP receiver.
            udp_port (int): The port number of the UDP receiver.
            start_chunk (int): The chunk number to start sending from.
        """
        payload = {
            'song_title': song_title,
            'client_ip': client_ip,
            'client_port': client_port,
            'start_chunk': start_chunk
        }

        try:
            response = self._send_request('/gw/get-song-file', payload)
            logger.info(f"Requested sending of {song_title} starting at chunk {start_chunk} to {client_ip}:{client_port}.")
            return response
        except Exception as e:
            logger.error(f"Error sending song file {song_title}: {e}")
            return None

    @property
    def gateway_nodes(self):
        return self._send_request('/gw/gateway-nodes', method='get')