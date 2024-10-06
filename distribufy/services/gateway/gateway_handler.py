import hashlib
import json
import logging
from urllib.parse import urlparse, parse_qs
from http.server import BaseHTTPRequestHandler
from services.gateway.gateway_reference import GatewayReference
from services.common.chord_handler import ChordNodeRequestHandler

# Set up logging
logger = logging.getLogger("__main__")
logger_stab = logging.getLogger("__main__.st")
logger_ff = logging.getLogger("__main__.ff")
logger_cp = logging.getLogger("__main__.cp")
logger_rh = logging.getLogger("__main__.rh")
logger_le = logging.getLogger("__main__.le")
logger_dt = logging.getLogger("__main__.dt")

import json
import logging
from http.server import BaseHTTPRequestHandler

# Setup logging
logger = logging.getLogger("__main__")

class GatewayRequestHandler(ChordNodeRequestHandler):
    def do_POST(self):
        """Manejar solicitudes POST."""
        content_length = int(self.headers['Content-Length'])
        post_data = json.loads(self.rfile.read(content_length))
        logger.debug(f'Request received at {self.path}: {post_data}')
        
        response = None
        
        if self.path == '/notify':
            response = self.handle_notify(post_data)
        elif self.path == '/update-node-list':
            response = self.server.node.update_node_list(post_data['nodes'])
        elif self.path == '/update-leader':
            response = self.server.node.update_leader(post_data['leader_ip'])
        
        self.send_json_response(response)
        
    def do_GET(self):
        # Manejar las solicitudes GET
        if self.path == '/get-nodes':
            self.send_json_response([node.__dict__ for node in self.server.node.node_list])
        elif self.path == '/get-leader':
            self.send_json_response(self.server.node.leader.__dict__)
        else:
            self.send_json_response({"error": "Invalid endpoint"}, status=404)

        
    def send_json_response(self, response, status=200):
        """Enviar respuesta JSON."""
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        if response:
            self.wfile.write(json.dumps(response).encode())
