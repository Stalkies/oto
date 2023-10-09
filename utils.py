def generate_data_types(data: list) -> dict:
    data_types = {}

    for dictionary in data:
        for key, value in dictionary.items():
            if isinstance(value, str):
                length = len(value)
                if key in data_types:
                    if data_types[key] == 'VARCHAR':
                        data_types[key] = f'VARCHAR({max(length, data_types[key])*4})'
                else:
                    data_types[key] = f'VARCHAR({length*4})'
            elif isinstance(value, int):
                if key not in data_types:
                    data_types[key] = 'INTEGER'
            elif isinstance(value, float):
                if key not in data_types:
                    data_types[key] = 'DECIMAL'

    return data_types