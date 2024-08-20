import json
import logging
import threading
from services.common.node_reference import ChordNodeReference
# from server.chord_node import ChordNode
from http.server import BaseHTTPRequestHandler, HTTPServer

# Set up logging
logger = logging.getLogger("__main__")
logger_rh = logging.getLogger("__main__.rh")

class ChordServer(HTTPServer):
    def __init__(self, *args, node=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.node = node

class BaseHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        threading.Thread(target=self._do_POST, daemon=True)
        
    def do_GET(self):
        threading.Thread(target=self._do_GET, daemon=True)
        
    def _do_POST(self):
        return

    def _do_GET(self):
        return
    
    def send_json_response(self, response, error_message=None, status=200):
        logger_rh.info(f'Responding to {self.client_address}')
        self.send_response(status)
        self.send_header("Content-type", "application/json")
        self.end_headers()

        if response:
            if isinstance(response, dict):
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
