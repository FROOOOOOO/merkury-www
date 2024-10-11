import logging
from core.utils import utils, prom_query


class MetricFetcher:
    """
    base class for load fetcher, bounded to one component
    """

    def __init__(self, node_name: str, node_addr: str, container: str, kubelet_port: str,
                 enable_logging: bool = False):
        if not isinstance(node_name, str):
            raise ValueError(f'node_name must be str, received {node_name} ({type(node_name)}) instead')
        self.node_name = node_name
        if not isinstance(node_addr, str):
            raise ValueError(f'node_addr must be str, received {node_addr} ({type(node_addr)}) instead')
        self.node_addr = node_addr
        utils.validate_container(container)
        self.container = container
        if not isinstance(kubelet_port, str):
            raise ValueError(f'kubelet_port must be str, received {kubelet_port} ({type(kubelet_port)}) instead')
        self.kubelet_port = kubelet_port
        self.enable_logging = enable_logging

    def __str__(self):
        return f'node {self.node_name} ({self.node_addr}), container: {self.container} (kubelet: {self.kubelet_port})'

    def _query_points(self, promql: str, data_name: str, ts_now: int, ts_last: int, max_retries: int = 3) -> list:
        if max_retries < 1:
            max_retries = 1
        option_range = f'&start={ts_last}&end={ts_now}&step=1'
        data_points = []
        for i in range(max_retries):
            try:
                data_raw = prom_query.get_prometheus_data(query=promql, range_query=True, options=option_range)
                if not prom_query.missing_data(data_raw):
                    for _, data_str in data_raw[0]['values']:
                        data_points.append(float(data_str))  # unit: core
                if len(data_points) > 0:
                    break
                elif i == max_retries - 1:
                    utils.logging_or_print(
                        f'{str(self)}: empty {data_name} points from prometheus.',
                        enable_logging=self.enable_logging, level=logging.WARNING)
                else:
                    utils.logging_or_print(
                        f'{str(self)}: empty {data_name} points from prometheus, will retry (retry times: {i + 1}).',
                        enable_logging=self.enable_logging, level=logging.WARNING)
            except Exception as e:
                if i == max_retries - 1:
                    utils.logging_or_print(
                        f'{str(self)}: an error occurred while querying {data_name} points from prometheus: {e}.',
                        enable_logging=self.enable_logging, level=logging.ERROR)
                else:
                    utils.logging_or_print(
                        f'{str(self)}: an error occurred while querying {data_name} points from prometheus: {e}, '
                        f'will retry (retry times: {i + 1}).',
                        enable_logging=self.enable_logging, level=logging.ERROR)
        return data_points

    def _query_point(self, promql: str, data_name: str, ts_now: int, data_type: str = 'int',
                     max_retries: int = 3) -> int | float | None:
        if max_retries < 1:
            max_retries = 1
        if data_type not in ['int', 'float', 'bool']:
            data_type = 'int'
        option_now = f'&time={ts_now}'
        data_point = None
        for i in range(max_retries):
            try:
                data_raw = prom_query.get_prometheus_data(query=promql, range_query=False, options=option_now)
                if not prom_query.missing_data(data_raw):
                    if data_type == 'int':  # int
                        data_point = int(data_raw[0]['value'][1])
                    elif data_type == 'float':  # float
                        data_point = float(data_raw[0]['value'][1])
                    else:  # bool
                        data_point = int(data_raw[0]['value'][1]) == 1
                    break
                elif i == max_retries - 1:
                    utils.logging_or_print(
                        f'{str(self)}: empty {data_name} data from prometheus.',
                        enable_logging=self.enable_logging, level=logging.WARNING)
                else:
                    utils.logging_or_print(
                        f'{str(self)}: empty {data_name} data from prometheus, will retry (retry times: {i + 1}).',
                        enable_logging=self.enable_logging, level=logging.WARNING)
            except Exception as e:
                if i == max_retries - 1:
                    utils.logging_or_print(
                        f'{str(self)}: an error occurred while querying {data_name} data from prometheus: {e}.',
                        enable_logging=self.enable_logging, level=logging.ERROR)
                else:
                    utils.logging_or_print(
                        f'{str(self)}: an error occurred while querying {data_name} data from prometheus: {e}, '
                        f'will retry (retry times: {i + 1}).',
                        enable_logging=self.enable_logging, level=logging.ERROR)
        return data_point

    def start(self):
        raise NotImplementedError('Implement in a child class.')

    def shutdown(self):
        raise NotImplementedError('Implement in a child class.')
