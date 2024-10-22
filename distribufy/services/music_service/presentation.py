from services.common.chord_handler import ChordNodeRequestHandler
from http.server import BaseHTTPRequestHandler
import json, logging
    
# Set up logging
logger = logging.getLogger("__main__")
logger_stab = logging.getLogger("__main__.st")
logger_ff = logging.getLogger("__main__.ff")
logger_cp = logging.getLogger("__main__.cp")
logger_rh = logging.getLogger("__main__.rh")
logger_le = logging.getLogger("__main__.le")
logger_dt = logging.getLogger("__main__.dt")
    
class MusicNodePresentation(ChordNodeRequestHandler):
    
    def do_POST(self):
        super().do_POST()

        if self.path == '/save-song':
            response = self.server.node.save_song(self.post_data)
            self.send_json_response(response)

        elif self.path == '/get-song-key-node':
            response = self.server.node.song_key_node(self.post_data)
            self.send_json_response(response)
        elif self.path == '/get-songs-title-node':
            response = self.server.node.songs_title_node(self.post_data)
            self.send_json_response(response)
        elif self.path == '/get-songs-artist-node':
            response = self.server.node.songs_artist_node(self.post_data)
            self.send_json_response(response)
        elif self.path == '/get-songs-genre-node':
            response = self.server.node.songs_genre_node(self.post_data)
            self.send_json_response(response)

        elif self.path == '/get-song-by-key':
            response = self.server.node.get_song_by_key(self.post_data)
            self.send_json_response(response)
        elif self.path == '/get-songs-by-title':
            response = self.server.node.get_songs_by_title(self.post_data)
            self.send_json_response(response)
        elif self.path == '/get-songs-by-artist':
            response = self.server.node.get_songs_by_artist(self.post_data)
            self.send_json_response(response)
        elif self.path == '/get-songs-by-genre':
            response = self.server.node.get_songs_by_genre(self.post_data)
            self.send_json_response(response)
    
    def do_GET(self):
        super().do_GET()
        
        """Handle GET requests."""
        
        if self.path == '/get-db':
            response = self.server.node.get_db()
            logger.debug(f'Get DB response {response}')
            self.send_json_response(response)
        elif self.path == '/get-songs':
            response = self.server.node.get_all_songs()
            logger.debug(f'Get songs response {response}')
            self.send_json_response(response)
            

