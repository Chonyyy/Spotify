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
        elif self.path == '/gw/save-song':
            response = self.server.node.save_song(self.post_data)
            self.send_json_response({"status":"Song store ok"})
        elif self.path == '/gw/get-song-by-key':
            response = self.server.node.get_song_by_key(self.post_data)
            self.send_json_response(response)
        elif self.path == '/gw/get-songs-by-title':
            response = self.server.node.get_songs_by_title(self.post_data)
            self.send_json_response(response)
        elif self.path == '/gw/get-songs-by-artist':
            response = self.server.node.get_songs_by_artist(self.post_data)
            self.send_json_response(response)
        elif self.path == '/gw/get-songs-by-genre':
            response = self.server.node.get_songs_by_genre(self.post_data)
            self.send_json_response(response)
        if self.path == '/gw/store-song-file':#TODO
            # Trigger the store file process
            udp_info = self.server.node.store_song_file(self.post_data)
            self.send_json_response(udp_info)  # Return the UDP IP and port
        elif self.path == '/gw/get-song-file':#TODO
            # Parse the request data
            song_key = self.post_data.get('song_key')
            udp_ip = self.post_data.get('udp_ip')
            udp_port = int(self.post_data.get('udp_port'))
            start_chunk = int(self.post_data.get('start_chunk', 0))

            if song_key and udp_ip and udp_port is not None:
                # Trigger sending the song file via UDP
                success = self.server.node.send_song_file(song_key, udp_ip, udp_port, start_chunk, self.post_data)

                if success:
                    self.send_json_response({"status": "success", "message": f"File {song_key} sent over UDP."})
                else:
                    self.send_json_response({"status": "error", "message": f"Failed to send file {song_key}."}, status=500)
            else:
                self.send_json_response({"status": "error", "message": "Missing required parameters."}, status=400)

    def do_GET(self):
        super().do_GET()

        if self.path == '/gw/gateway-nodes':
            all_nodes = list(self.server.node.gateway_nodes.values())
            node_dict = {}
            for node in all_nodes:
                node_dict[node.id] = {'id': node.id, 'ip': node.ip}
            self.send_json_response(node_dict)
        elif self.path == '/gw/get-songs':
            response = self.server.node.get_all_songs()
            return self.send_json_response(response)
        
    def handle_gw_notify(self, post_data):
        node = GatewayReference(post_data['id'], post_data['ip'])
        self.server.node.notify_gw(node)

    def handle_share_gw_knowledge(self, post_data):
        self.server.node.update_known_gw_nodes(post_data)
