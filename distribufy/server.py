from services.gateway.gateway_node import Gateway
import logging

logger = logging.getLogger("__main__")

def start_server(ip, role, db, pred_db, succ_db):
    if role == 'gateway':
        node = Gateway(ip)
    if role == 'storage_service':
        pass
    if role == 'music_service':
        pass
    else:
        raise Exception("Incorrect Role")

    while True:
        pass