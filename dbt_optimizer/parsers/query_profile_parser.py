import json

class dbtParser(self, manifest_path):
    def __init__(self, manifest_path):
        self.manifest_path = manifest_path
        self.models = []
    
    @classmethod
    def parse_manifest(self):
        with open(self.manifest, 'r') as file:
            data = json.load(file)
        for model in data.get('nodes', {}).values():
                if model['resource_type'] == 'model':
                    self.models.append({
                        'name': model['name'],
                        'sql': model['compiled_sql'],  # Get the compiled SQL
                        'unique_id': model['unique_id'],
                    })
        return self.models

class dbtRunResults:


class dbtModel:

