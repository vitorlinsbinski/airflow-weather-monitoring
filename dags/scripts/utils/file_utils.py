import json

def read_json_from_path(file_path):
    with open(file_path, 'r') as file:
        cities = json.load(file)
    return cities