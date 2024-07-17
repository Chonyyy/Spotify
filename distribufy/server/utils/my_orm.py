import json

class JSONDatabase:
    def __init__(self, filepath, columns):
        self.filepath = filepath
        self.columns = columns
        try:
            with open(filepath + '.json', 'r') as file:
                self.data = json.load(file)
        except FileNotFoundError:
            self.data = []

    def save(self):
        with open(self.filepath, 'w') as file:
            json.dump(self.data, file, indent=4)

    def validate_record(self, record):
        for column in self.columns:
            if column not in record:
                raise ValueError(f"Missing required column: {column}")
        for key in record:
            if key not in self.columns:
                raise ValueError(f"Invalid column: {key}")

    def insert(self, record):
        self.validate_record(record)
        self.data.append(record)
        self.save()

    def query(self, key, value):
        return [record for record in self.data if record.get(key) == value]

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

# # Usage
# db = JSONDatabase('db.json')

# # Insert a record
# db.insert({'name': 'John', 'age': 22})

# # Query the database
# results = db.query('name', 'John')
# print(results)

# # Update a record
# db.update('name', 'John', {'age': 23})

# # Delete a record
# db.delete('name', 'John')
