import logging
import os
from services.common.chord_node import ChordNode

# Set up logging
logger_gw = logging.getLogger("__main__.gw")

class StorageNode(ChordNode):
    def __init__(self, ip: str, db, sec_succ_db, succ_db, role: str, port: int = 8001, m: int = 160):
        super().__init__(ip, db, sec_succ_db, succ_db, role, port, m)
        self.fragment_storage = f'./databases/node_{self.ip}/song_fragments'
        os.makedirs(self.fragment_storage, exist_ok=True)

    def store_song_fragment(self, fragment_id: str, data: bytes):
        """Store a song fragment in memory (in the database)."""
        pass

    def send_song_fragment(self, fragment_id: str):
        """Retrieve and send a song fragment from memory (from the database)."""
        pass
