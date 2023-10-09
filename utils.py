def generate_data_types(data: list) -> dict:
    data_types = {}

    for dictionary in data:
        for key, value in dictionary.items():
            if isinstance(value, str):
                data_types[key] = f'VARCHAR'
            elif isinstance(value, int):
                if key not in data_types:
                    data_types[key] = 'INTEGER'
            elif isinstance(value, float):
                if key not in data_types:
                    data_types[key] = 'DECIMAL'

    return data_types