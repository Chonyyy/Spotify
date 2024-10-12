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
        return self._send_request('/gw/get-songs', method='get')
    
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

    @property
    def gateway_nodes(self):
        return self._send_request('/gw/gateway-nodes', method='get')