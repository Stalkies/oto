from forex_python.converter import CurrencyRates

def generate_data_types(data: list) -> dict:
    data_types = {}

    for dictionary in data:
        for key, value in dictionary.items():
            if isinstance(value, str):
                data_types[key] = f'VARCHAR'
                if key == 'link':
                    data_types[key] = data_types[key] + ' NOT NULL UNIQUE'
            elif isinstance(value, int):
                if key not in data_types:
                    data_types[key] = 'INTEGER'
            elif isinstance(value, float):
                if key not in data_types:
                    data_types[key] = 'DECIMAL'

    return data_types

def get_pln_price(price: float, currency:str):
    c = CurrencyRates()
    rate = c.get_rate(currency, 'PLN')
    return round(price*rate,2)