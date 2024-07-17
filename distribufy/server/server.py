from server.handlers.chord_handler import ChordNodeReference
from server.chord_node import ChordNode
from server.utils.my_orm import JSONDatabase
import logging

logger = logging.getLogger("__main__")

def initialize_database(role, filepath):
    columns = None
    replic_columns = None
    if role == 'music_info':
        columns = ['id','title', 'album', 'genre', 'artist']
        replic_columns = ['id','title', 'album', 'genre', 'artist', 'source']
    elif role == 'music_ftp':
        columns = ['id', 'addr']
        replic_columns = ['id', 'addr', 'columns']
    return JSONDatabase(filepath, columns), JSONDatabase(filepath + 'pred', replic_columns), JSONDatabase(filepath + 'succ', replic_columns)

def start_server(ip, other_ip=None, role = 'music_info', db_name = 'db'):
    print(f'Launching App on {ip}')
    db, pred_db, succ_db = initialize_database(role, db_name)
    node = ChordNode(ip, db = db, pred_db = pred_db, succ_db = succ_db, role='testing')
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