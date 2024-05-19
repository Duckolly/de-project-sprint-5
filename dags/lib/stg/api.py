import requests


class Api :
    _host = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net'
    _headers = {'X-Nickname': 'Duckolly', 
                'X-Cohort': "24", 
                'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f'}
    
    def get_rests(self, limit=50, offset=0) -> dict:
        r = requests.get(url=f'{self._host}/restaurants', 
                         headers=self._headers, 
                         params={'sort_field': 'id',
                                 'sort_direction': 'asc',
                                 'limit': limit,
                                 'offset': offset})
        return r.json()

    def get_couriers(self, limit=50, offset=0) -> dict:
        r = requests.get(url=f'{self._host}/couriers', 
                         headers=self._headers, 
                         params={'sort_field': 'id',
                                 'sort_direction': 'asc',
                                 'limit': limit,
                                 'offset': offset})
        return r.json()

    def get_deliveries(self, limit=50, offset=0) -> dict:
        r = requests.get(url=f'{self._host}/deliveries', 
                         headers=self._headers, 
                         params={'sort_field': 'id',
                                 'sort_direction': 'asc',
                                 'limit': limit,
                                 'offset': offset})
        return r.json()
