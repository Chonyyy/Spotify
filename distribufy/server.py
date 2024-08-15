from services.gateway.gateway_node import Gateway
from services.music_service.logic import MusicNode
import logging

logger = logging.getLogger("__main__")

def start_server(ip, role, db, pred_db, succ_db):
    if role == 'gateway':
        node = Gateway(ip)
    if role == 'music_service':
        node = MusicNode(
            ip=ip,
            db=db,
            pred_db=pred_db,
            succ_db=succ_db,
            role=role
        )
    if role == 'storage_service':
        pass
    else:
        raise Exception("Incorrect Role")

    while True:
        pass