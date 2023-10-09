import asyncio
from aiohttp import ClientSession
import sys
from bs4 import BeautifulSoup
import json
import time

from database import DataBase

'''
    TODO:
1. check currency and exchange all currencies to PLN
2. upload info to DB
3. closing session
4. add to parsing keys 'Wersja', 'Generacja'
5. fix utils.generate_data_types
'''



# URL = "https://www.otomoto.pl/osobowe"
URL = "https://www.otomoto.pl/osobowe/volkswagen/passat?search%5Bfilter_float_year%3Ato%5D=2010"

HEADERS = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
}
PARSING_KEYS = {
    'Marka pojazdu': 'brand',
    'Model pojazdu': 'model',
    'Moc': 'power',
    'Napęd':  'drive',
    'Pojemność skokowa': 'engine_capacity',
    'Przebieg': 'mileage',
    'Rodzaj paliwa': 'fuel_type',
    'Rok produkcji': 'year',
    'Skrzynia biegów': 'transmission',
    'Stan': 'state',
    
}


async def get_pages_count(session: ClientSession) -> int:

        async with session.get(URL) as response:
            if response.status == 200:
                text = await response.text()
                soup = BeautifulSoup(text, 'lxml')
                pages = soup.find_all('li', {'data-testid': 'pagination-list-item'})
                return int(pages[-1].text)
            else:
                print(response.status)
                print(response.headers)


async def get_cars_link(session: ClientSession, page_number):

        request_url = f'?page={page_number}' if not '?' in URL else f'&page={page_number}'
        response = await session.get(URL+request_url)
        if response.status == 200:
            html = await response.text()
            soup = BeautifulSoup(html, 'lxml')
            links = soup.find_all('a', href=True)
            html_links = [link['href'] for link in links if link['href'].endswith('.html')]
            print(f"-\t{page_number}")
            return html_links
        else:
            print(response.status)


async def get_car_info(session: ClientSession, link, ERROR_LINKS):
    try:
        async with session.get(link) as response:
            if response.status == 200:
                html = await response.text()
                data_dict = {} 
                soup = BeautifulSoup(html, 'lxml')
                divs = soup.find_all('div', {'data-testid': 'advert-details-item'})
                for div in divs:
                    key = div.find_all('p')[0].text
                    if key not in PARSING_KEYS.keys():
                        continue
                    try:
                        value = div.find('a').text
                    except:
                        value = div.find_all('p')[-1].text

                    if key == 'Rok produkcji':
                        data_dict[PARSING_KEYS[key]] = int(value)
                    elif key in ('Moc', 'Przebieg', 'Pojemność skokowa'):
                        value_int = ''
                        for char in value:
                            if char.isdigit():
                                value_int += char
                        value = int(value_int)
                        data_dict[PARSING_KEYS[key]] = value
                    else:
                        data_dict[PARSING_KEYS[key]] = value
                try:
                    data_dict['price'] = int(float(soup.find('h3', class_='offer-price__number')
                                                    .text.replace(' ', '').replace(',','.')))
                    data_dict['currency'] = soup.find('p', class_='offer-price__currency').text
                except AttributeError:
                    data_dict['price'] = None
                data_dict['link'] = link
                print(f'[INFO] Link {link} successfully parsed')
                return data_dict
            else:
                ERROR_LINKS.append(link)
    except:
        ERROR_LINKS.append(link)


async def main():
    ERROR_LINKS = []
    db = DataBase()
    await db.create_pool()

    session = ClientSession(headers=HEADERS)

    pages_count = await get_pages_count(session=session)
    print("Pages count:", pages_count)
    tasks = []
    results = []
    for page in range(1, pages_count+1):
        tasks.append(asyncio.create_task(get_cars_link(session=session, page_number=page)))
        if page % 20 == 0:
            results += await asyncio.gather(*tasks)
            tasks = []
            # print("\tSLEEP 180...")
            # time.sleep(180)

    links = set()
    for list_links in results:
        links.update(list_links)
    links = list(links)
    print("Links count:", len(links))

    # GETTING CAR INFO
    item = 0
    while links:
        tasks = []
        for link in links[0:20]:
            tasks.append(get_car_info(session=session, link=link, ERROR_LINKS=ERROR_LINKS))
        links = links[20:]
        cars = await asyncio.gather(*tasks)
        await db.create_table(cars)
        for car in cars:
            await db.add_car(car=car)
        print("+\t20")
        # item += 20
        # if item % 200 == 0:
        #     print("\tSLEEP 300...")
        #     time.sleep(300)

    # RETRYING ERRORS
    print('Errors:', len(ERROR_LINKS))
    tasks = []
    item = 0
    while ERROR_LINKS:
        for link in ERROR_LINKS[0:20]:
            tasks.append(get_car_info(session=session, link=link, ERROR_LINKS=ERROR_LINKS))
        ERROR_LINKS = ERROR_LINKS[20:]
        cars = await asyncio.gather(*tasks)
        for car in cars:
            await db.add_car(car=car)
        item += len(tasks)
        print(f'\tErrors left: {len(ERROR_LINKS)}')
        
        # if item % 200 == 0:
        #     print("\tSLEEP 300...")
        #     time.sleep(300)

    session.close()
if __name__ == '__main__':
    asyncio.run(main())
