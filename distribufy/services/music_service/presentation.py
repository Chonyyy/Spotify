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

        if self.path == 'get-song':
            response = self.server.get_song(self.post_data)  # Utilizar el post_data de la clase padre
            self.send_json_response(response)
        elif self.path == 'save-song':
            response = self.server.node.save_song(self.post_data)
            self.send_json_response(response)
        elif self.path == '/get-songs-by-title':
            response = self.server.node.get_songs_by_title(self.post_data)
            self.send_json_response(response)
        elif self.path == '/get-songs-by-author':
            response = self.server.node.get_songs_by_author(self.post_data)
            self.send_json_response(response)
        elif self.path == '/get_songs_by_gender':
            response = self.server.node.get_songs_by_gender(self.post_data)
            self.send_json_response(response)
        
        print("Sali de los if de post")

    
    def do_GET(self):
        super().do_GET()
        
        """Handle GET requests."""
        logger_rh.debug(f'Request path {self.path}')
        
        if self.path == '/get-db':
            response = self.server.node.get_db()
            print(response)
            self.send_json_response(response)
        elif self.path == '/get-songs':
            response = self.server.node.get_all_songs()
            self.send_json_response(response)
            

