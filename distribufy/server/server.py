from server.handlers.chord_handler import ChordNode, ChordNodeReference
# from server.handlers.music_handler import MusicHandler
# from server.handlers.auth_handler import AuthHandler
# from server.utils.sync_utils import SyncHandler
import logging

logger = logging.getLogger("__main__")

def start_server(ip, other_ip=None):
    print(f'Launching App on {ip}')
    node = ChordNode(ip)
    if other_ip:
        node.join(ChordNodeReference(other_ip, other_ip, node.port))
        
    # # Add the new handlers to the HTTP server
    # node.httpd.RequestHandlerClass.handlers.update({
    #     '/register': AuthHandler,
    #     '/login': AuthHandler,
    #     '/add_song': MusicHandler,
    #     '/remove_song': MusicHandler,
    #     '/get_song': MusicHandler,
    #     '/sync': SyncHandler
    # })

    while True:
        pass