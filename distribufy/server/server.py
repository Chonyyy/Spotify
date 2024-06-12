from server.handlers.chord_handler import ChordNode, ChordNodeReference
import logging

logger = logging.getLogger("__main__")

def start_server(ip, other_ip = None):
    print(f'Launching App on {ip}')
    node = ChordNode(ip)
    if other_ip:
        node.join(ChordNodeReference(other_ip, other_ip, node.port))

    while True:
        pass
