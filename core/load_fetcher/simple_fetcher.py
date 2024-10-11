import logging
import time
import numpy as np
from apscheduler.schedulers.background import BackgroundScheduler
from core.load_fetcher import base
from core.utils import prom_query, utils


class SimpleFetcher(base.MetricFetcher):
    """
    P99 fetcher (bound to one pod/instance) for periodically fetching CPU and memory metrics
    """

    def __init__(self, container: str = utils.KUBE_APISERVER,
                 node_name: str = utils.MASTERS[0].name, node_addr: str = utils.MASTERS[0].ip,
                 kubelet_port: str = utils.PORTS[utils.KUBELET],
                 query_interval: int = 60, p99_period: int = 60, enable_logging: bool = True):
        super().__init__(node_name=node_name, node_addr=node_addr, container=container, kubelet_port=kubelet_port,
                         enable_logging=enable_logging)
        self.instance_kubelet = f'{self.node_addr}:{self.kubelet_port}'
        # intervals
        utils.validate_interval(query_interval, 'query_interval')
        self._query_interval = query_interval  # unit: s
        utils.validate_interval(p99_period, 'p99_period')
        self._p99_period = p99_period  # unit: s
        # metrics
        self._p99_cpu = 0  # unit: 10^(-3)core
        self._p99_memory = 0  # unit: 10^6B
        self._latest_timestamp = 0
        self._latest_time_window = 0
        # query job scheduler
        self._scheduler = BackgroundScheduler()
        # self._update_job = self._scheduler.add_job(self.p99_update, trigger='interval', seconds=self._query_interval)

    def __str__(self) -> str:
        return (f'Simple fetcher (node {self.node_name} ({self.node_addr}), container: {self.container}, '
                f'kubelet port: {self.kubelet_port})')

    # get methods
    def get_latest_timestamp(self) -> int:
        return self._latest_timestamp

    def get_latest_time_window(self) -> int:
        return self._latest_time_window

    def get_p99_cpu(self) -> int:
        return self._p99_cpu

    def get_p99_memory(self) -> int:
        return self._p99_memory

    # query methods
    def query_cpu_points(self, timestamp_now: int, timestamp_last: int, max_retries: int = 3) -> list:
        promql_cpu = ('sum(rate(container_cpu_usage_seconds_total{container=\'%s\', instance=\'%s\'}[1m]))'
                      % (self.container, self.instance_kubelet))
        return self._query_points(promql=promql_cpu, data_name='cpu', ts_now=timestamp_now, ts_last=timestamp_last,
                                  max_retries=max_retries)

    def query_mem_points(self, timestamp_now: int, timestamp_last: int, max_retries: int = 3) -> list:
        """
        :param timestamp_now:
        :param timestamp_last:
        :param max_retries:
        :return: unit: 10^6B
        """
        promql_mem = ('sum(container_memory_working_set_bytes{container=\'%s\',instance=\'%s\'})' %
                      (self.container, self.instance_kubelet))
        mem_points = self._query_points(promql=promql_mem, data_name='memory', ts_now=timestamp_now,
                                        ts_last=timestamp_last, max_retries=max_retries)
        mem_points = [mem_point / 1e6 for mem_point in mem_points]
        return mem_points

    # update methods
    def update_query_interval(self, new_interval: int) -> None:
        if new_interval <= 0:
            utils.logging_or_print(
                f'{str(self)}: invalid query interval, positive integer required, received {new_interval} instead.',
                enable_logging=self.enable_logging, level=logging.WARNING)
        elif new_interval != self._query_interval:
            old_interval = self._query_interval
            self._query_interval = new_interval
            self._scheduler.reschedule_job(self._update_job.id, trigger='interval', seconds=new_interval)
            utils.logging_or_print(
                f'{str(self)}: query interval is updated from {old_interval} s to {new_interval} s.',
                enable_logging=self.enable_logging, level=logging.INFO)

    def update_p99_period(self, new_period: int) -> None:
        if new_period <= 0:
            utils.logging_or_print(
                f'{str(self)}: Invalid p99 period, positive integer required, receive {new_period} instead.',
                enable_logging=self.enable_logging, level=logging.WARNING)
        elif new_period != self._p99_period:
            old_period = self._p99_period
            self._p99_period = new_period
            utils.logging_or_print(
                f'{str(self)}: p99 period is updated from {old_period} s to {new_period} s.',
                enable_logging=self.enable_logging, level=logging.INFO)
            self.load_update()

    # jobs
    def load_update(self):
        now_timestamp = int(time.time())
        beginning_timestamp = now_timestamp - self._p99_period
        cpu_data = self.query_cpu_points(now_timestamp, beginning_timestamp)
        mem_data = self.query_mem_points(now_timestamp, beginning_timestamp)
        if prom_query.missing_data(cpu_data) or prom_query.missing_data(mem_data):
            utils.logging_or_print(
                f'{str(self)}: query suspended due to incomplete data.',
                enable_logging=self.enable_logging, level=logging.WARNING)
            return
        cpu = np.array(cpu_data)
        mem = np.array(mem_data)
        self._p99_cpu = int(np.percentile(cpu, 99) * 1000)
        self._p99_memory = int(np.percentile(mem, 99))
        self._latest_timestamp = now_timestamp
        self._latest_time_window = now_timestamp - beginning_timestamp
        utils.logging_or_print(
            f'{str(self)}: load update finished, p99 CPU - {self._p99_cpu} mcore, P99 memory - {self._p99_memory} MB.',
            enable_logging=self.enable_logging, level=logging.INFO)

    # start and shutdown
    def start(self):
        if not self._scheduler.running:
            self.load_update()
            self._scheduler.start()
            if self._scheduler.running:
                utils.logging_or_print(
                    f'{str(self)} starts running.',
                    enable_logging=self.enable_logging, level=logging.INFO)
        else:
            utils.logging_or_print(f'{str(self)} has already been running.',
                                   enable_logging=self.enable_logging, level=logging.WARNING)

    def shutdown(self):
        if self._scheduler.running:
            self._scheduler.shutdown()
            utils.logging_or_print(f'{str(self)} has been shutdown.',
                                   enable_logging=self.enable_logging, level=logging.INFO)
        else:
            utils.logging_or_print(f'{str(self)} has already been shutdown.',
                                   enable_logging=self.enable_logging, level=logging.WARNING)
