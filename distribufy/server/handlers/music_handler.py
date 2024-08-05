import hashlib
import threading
import json
import logging
import os
from urllib.parse import urlparse, parse_qs
from server.utils.helper_funcs import get_sha_repr
from server.handlers.base_handler import BaseHandler
from server.node_reference import ChordNodeReference

logger = logging.getLogger("__main__")
logger_rh = logging.getLogger("__main__.rh")

class MusicManagerHandler(BaseHandler):
    
    def do_POST(self):
        logger_rh.debug(f'Request path {self.path}')
        """Handle POST requests."""
        content_length = int(self.headers['Content-Length'])
        post_data = json.loads(self.rfile.read(content_length))
        logger_rh.debug(f'Handling the following request \n{post_data}')
        
        response = None
        
        if self.path == '/upload-file':
            self.handle_upload_file()
        elif self.path.startswith('/songs/upload_chunks/{song_id}'):
            pass
        elif self.path == '/store-replic':
            print('store replication')
            # threading.Thread(target=self.handle_store_replic, args=[post_data], daemon=True)
            self.handle_store_replic(post_data)
            self.send_json_response({'status': 'received'})
        else:
            self.send_json_response({}, 'Invalid Endpoint', status=404)
    
    def do_GET(self):
        logger_rh.debug(f'Request path {self.path}')
        """Handle GET requests."""
        response = None
        
        if self.path.startswith('/songs/stream/{song_id}'):
            pass
        elif self.path.startswith('/songs/genre/{genre}'):
            pass
        elif self.path.startswith('/songs'):
            self.server.node.get_songs()
        elif self.path.startswith('/songs/{song_title}'):
            pass
        else:
            self.send_json_response(response, error_message='Page not found', status=404)
