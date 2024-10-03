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
        logger_rh.debug(f'Request path {self.path}')
        """Handle POST requests."""
        content_length = int(self.headers['Content-Length'])
        post_data = json.loads(self.rfile.read(content_length))
        logger_rh.debug(f'Handling the following request \n{post_data}')
        
        if self.path == 'get_song':
            response = self.server.get_song(post_data)
            self.send_json_response(response)
        elif self.path == 'save-song':
            response = self.server.save_song(post_data)
            self.send_json_response(response)
        elif self.path == '/get-songs-by-title':
            response = self.server.node.get_songs_by_title(post_data)
            self.send_json_response(response)
        elif self.path == '/get-songs-by-author':
            response = self.server.node.get_songs_by_author(post_data)
            self.send_json_response(response)
        elif self.path == '/get_songs_by_gender':
            response = self.server.node.get_songs_by_gender(post_data)
            self.send_json_response(response)
        

    
    def do_GET(self):
        """Handle GET requests."""
        logger_rh.debug(f'Request path {self.path}')
        response = None
        if self.path == '/get-db':
            response = self.server.node.get_db()
            print(response)
            self.send_json_response(response)
        elif self.path == '/get-songs':
            response = self.server.node.get_all_songs()
            self.send_json_response(response)
            

