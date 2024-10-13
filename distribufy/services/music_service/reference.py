from services.common.node_reference import ChordNodeReference

class MusicNodeReference(ChordNodeReference):
    def store_song_data(self, payload):
        self._send_request('/save-song', payload)