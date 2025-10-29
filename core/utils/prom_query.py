"""
   Copyright 2025 FROOOOOOO and Ma-YuXin

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

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
