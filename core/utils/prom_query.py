import requests
from core.utils import utils


def get_prometheus_data(query: str, url: str = utils.PROM_URL, token: str = utils.AUTH_TOKEN, range_query: bool = False,
                        options: str = '', timeout: int = 10) -> list:
    """
    Get data from prometheus
    Examples:
    1.
    {
        "status": "success",
        "data": {
            "resultType": "vector",
            "result": []
        }
    }
    2.
    {
        "status": "success",
        "data": {
            "resultType": "vector",
            "result": [
                {
                    "metric": {},
                    "value": [
                        1708870990,
                        "0"
                    ]
                }
            ]
        }
    }
    """
    if range_query:
        url = url + '/api/v1/query_range'
    else:
        url = url + '/api/v1/query'
    response = requests.get(f'{url}?query={query}{options}', headers={'Authorization': f'Bearer {token}'},
                            timeout=timeout)
    if response.status_code != 200:
        raise Exception('Error: ' + str(response.status_code) + ', ' + response.text)
    results = response.json()['data']['result']
    return results


def missing_data(data) -> bool:
    return data is None or len(data) == 0
