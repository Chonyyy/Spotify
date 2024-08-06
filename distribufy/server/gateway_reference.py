import hashlib
import requests
import logging
from typing import List

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

class GatewayReference:
    def __init__(self, id: str, ip: str, port: int = 8001):
        self.id = get_sha_repr(ip) if not id else id
        self.ip = ip
        self.port = port
        self.replication_queue = []
        
    def _send_request(self, path: str, data: dict = None, method: str = 'POST') -> dict:
        """Send a request and handle retries."""
        max_retries = 4
        for i in range(max_retries):
            response = None
            try:
                url = f'http://{self.ip}:{self.port}{path}'

                if method.upper() == 'POST':
                    logger.info(f'Sending POST request to {url}\nPayload: {data}')
                    response_raw = requests.post(url, json=data)
                elif method.upper() == 'GET':
                    logger.info(f'Sending GET request to {url}')
                    response_raw = requests.get(url, params=data)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")

                response = response_raw.json()
                logger.debug(f'From {url} received:\n{response}')
                return response
            except requests.ConnectionError as e:
                logger.error(f'Connection Error in IP {self.ip}')
                logger.error(f'{e}')
                if i == max_retries - 1:
                    raise e
            except requests.exceptions.JSONDecodeError as e:
                logger.error(f'JSON Decode Error: {e}')
                logger.error(f'Response text: {response_raw.text}')  # Log the response body
            except Exception as e:
                logger.error(f"Error sending data to {path}: {data}\n{e}")
                raise e

    def notify(self, node: 'GatewayReference'):
        """Notify the node of a change."""
        self._send_request('/notify', {'id': node.id, 'ip': node.ip})
        
    def new_leader(self, leader: 'GatewayReference'):
        """Notify the node of a leader change."""
        self._send_request('/new_leader', {'id': leader.id, 'ip': leader.ip})
    
    def share_knowledge(self, known_nodes: List['GatewayReference']):
        """Notify the new leader."""
        nodes = []
        for node in known_nodes:
            nodes.append({'ip': node.ip, 'port': node.port, 'id': node.id})
        self._send_request('/share_knowledge', {'nodes': nodes})
    
    def ping(self):
        self._send_request('/ping', method='get')