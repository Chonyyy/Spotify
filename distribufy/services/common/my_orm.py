import json, logging
from prettytable import PrettyTable

logger = logging.getLogger("__main__")

class JSONDatabase:
    def __init__(self, filepath, columns, key_fields):
        self.key_fields = key_fields
        self.filepath = filepath
        self.columns = columns + ['last_update', 'deleted']
        try:
            with open(filepath + '.json', 'r') as file:
                self.data = json.load(file)
        except FileNotFoundError:
            self.data = []
            
    def __str__(self):
        table = PrettyTable()
        table.field_names = self.columns

        for record in self.data:
            row = [record.get(column, '') for column in self.columns]
            table.add_row(row)

        return f'Filepath: {self.filepath}\nColumns: {self.columns}\nData:\n{table}'

    def drop(self):
        self.data = []

    def save(self):
        with open(self.filepath, 'w') as file:
            json.dump(self.data, file, indent=4)

    def validate_record(self, record):
        for column in self.columns:
            if column not in record:
                raise KeyError(f"Missing required column: {column}")
        for key in record:
            if key not in self.columns:
                raise ValueError(f"Invalid column: {key}")

    def insert(self, record):
        self.validate_record(record)

        for song in self.data:
            if record['key'] == song['key']:
                print(f"Data already exists")
                return
            
        logger.info(record)
        self.data.append(record)
        self.save()

    def query(self, key, value, cond = None):
        if not cond:
            return [record for record in self.data if record.get(key) == value]
        return [record for record in self.data if cond(record.get(key))]

    def get_all(self):
        if not self.data:
            return []
        return self.data
        
    def update(self, key, value, update_data):
        self.validate_record(update_data)
        for record in self.data:
            if record.get(key) == value:
                record.update(update_data)
        self.save()

    def delete(self, key, value):
        self.data = [record for record in self.data if record.get(key) != value]
        self.save()
        
    def _log_database(self, logger):
        logger.info(self.data)
