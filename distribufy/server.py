from services.gateway.gateway_node import Gateway
from services.music_service.logic import MusicNode
from services.common.chord_node import ChordNode
import logging

logger = logging.getLogger("__main__")

def start_server(ip, role, db, sec_succ_db, succ_db):
    if role == 'gateway':
        node = Gateway(ip)
    elif role == 'music_service':
        node = MusicNode(
            ip=ip,
            db=db,
            sec_succ_db=sec_succ_db,
            succ_db=succ_db,
            role=role
        )
    elif role == 'storage_service':
        pass
    elif role == 'chord_testing':
        node = ChordNode(
            ip=ip, 
            db=db, 
            sec_succ_db=sec_succ_db, 
            succ_db=succ_db, 
            role=role
        )
    else:
        raise Exception("Incorrect Role")

    while True:
        pass