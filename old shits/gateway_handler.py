import hashlib
import threading
import json
import logging
from urllib.parse import urlparse, parse_qs
from http.server import BaseHTTPRequestHandler
from server.node_reference import ChordNodeReference

# Set up logging
logger = logging.getLogger("__main__")
logger_stab = logging.getLogger("__main__.st")
logger_ff = logging.getLogger("__main__.ff")
logger_cp = logging.getLogger("__main__.cp")
logger_rh = logging.getLogger("__main__.rh")
logger_le = logging.getLogger("__main__.le")
logger_dt = logging.getLogger("__main__.dt")

def get_sha_repr(data: str) -> int:
    """Return SHA-1 hash representation of a string as an integer."""
    return int(hashlib.sha1(data.encode()).hexdigest(), 16)

#region RequestHandler
class GatewayRequestHandler(BaseHTTPRequestHandler):

    def do_POST(self):
        """Handle POST requests."""
        content_length = int(self.headers['Content-Length'])
        post_data = json.loads(self.rfile.read(content_length))
        logger_rh.debug(f'Request path {self.path}')
        logger_rh.debug(f'Handling the following request \n{post_data}')
        
        response = None

        if self.path == '/store-data':
            response = self.handle_store_data(post_data)
            self.send_json_response(response)
        else:
            self.send_json_response({}, 'Invalid Endpoint', status=404)
        
    def do_GET(self):
        """Handle GET requests."""
        logger_rh.debug(f'Request path {self.path}')
        response = None
        
        if self.path.startswith('/get_user'):
            response = self.handle_get_user(self.path)
            self.send_json_response(response, status=200)
            
        elif self.path == '/list_users':
            response = self.server.node.list_users()
            self.send_json_response(response, status=200)
        else:
            self.send_json_response(response, error_message='Page not found', status=404)
            
    def handle_song(self):
        query_params = urlparse(self.path).query
        params = parse_qs(query_params)
        
        if 'key' not in self.headers or 'file_name' not in self.headers:
            self.send_json_response(None, error_message='Missing key or file_name in headers', status=400)
            return
    
    def send_json_response(self, response, error_message=None, status=200, origin = None):
        if origin:
            print(f'Responging to {self.client_address}, from {origin}')
        self.send_response(status)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        
        if response:
            if isinstance(response, dict):
                if origin:
                    print('response as dict')
                self.wfile.write(json.dumps(response).encode())
            elif isinstance(response, ChordNodeReference):
                self.wfile.write(json.dumps({'id': response.id, 'ip': response.ip}).encode())
            else:
                self.wfile.write(json.dumps(response).encode())
        else:
            if error_message:
                self.wfile.write(json.dumps({'status': 'error', 'message': error_message}).encode())
            else:
                self.wfile.write(json.dumps({'id': None, 'ip': None}).encode())