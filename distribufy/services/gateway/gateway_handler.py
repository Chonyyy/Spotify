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
        super().do_POST()

        if self.path == '/gw/notify':
            self.handle_gw_notify(self.post_data)
            self.send_json_response(None)
        elif self.path == '/gw/share-gw-knowledge':
            self.handle_share_gw_knowledge(self.post_data)
            self.send_json_response({"status": "success"})

    def do_GET(self):
        super().do_GET()

        if self.path == '/gw/gateway-nodes':
            all_nodes = list(self.server.node.gateway_nodes.values())
            node_dict = {}
            #FIXME: finish this
            for node in all_nodes:
                node_dict[node.id] = {'id': node.id, 'ip': node.ip}
            self.send_json_response(node_dict)

    def handle_gw_notify(self, post_data):
        node = GatewayReference(post_data['id'], post_data['ip'])
        self.server.node.notify_gw(node)

    def handle_share_gw_knowledge(self, post_data):
        self.server.node.update_known_gw_nodes(post_data)
