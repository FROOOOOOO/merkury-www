import logging
import time
import numpy as np
from apscheduler.schedulers.background import BackgroundScheduler
from core.utils import utils, queue_model, prom_query
from core.load_fetcher import base


class LoadFetcher(base.MetricFetcher):
    """
    Load fetcher (bound to one pod/instance) for:
    1. KubeRequestsSummary and baseline memory usage collection
    2. cpu utilization - concurrency mapping learning (only applicable for apiserver, disabled by default)
    """

    def __init__(self, container: str = utils.KUBE_APISERVER,
                 node_name: str = utils.MASTERS[0].name, node_addr: str = utils.MASTERS[0].ip,
                 kubelet_port: str = utils.PORTS[utils.KUBELET],
                 container_port: str = utils.PORTS[utils.KUBE_APISERVER],
                 enable_mapping: bool = False, mapping_query_interval: int = 60, mapping_update_interval: int = 60,
                 interp_method: str = 'linear', concurrency=None, cpu_utl=None, load_update_interval: int = 60,
                 enable_logging: bool = True):
        super().__init__(node_name=node_name, node_addr=node_addr, container=container, kubelet_port=kubelet_port,
                         enable_logging=enable_logging)
        # for non-component-specific metrics,
        # e.g. container_sockets{container=<container>, instance=<node_addr>:<container_port>}
        # for component-specific metrics,
        # e.g. apiserver_current_inflight_requests{instance=<node_addr>:<component_port>}
        self.container_port = container_port
        self.instance_kubelet = f'{self.node_addr}:{self.kubelet_port}'
        self.instance_container = f'{self.node_addr}:{self.container_port}'
        # intervals
        utils.validate_interval(load_update_interval, 'load_update_interval')
        self._load_update_interval = load_update_interval  # unit: s
        # cpu - concurrency mapping related variables
        self._enable_mapping = enable_mapping
        utils.validate_method(interp_method)
        self._interp_method = interp_method
        utils.validate_interval(mapping_query_interval, 'mapping_query_interval')
        self._mapping_query_interval = mapping_query_interval
        utils.validate_interval(mapping_update_interval, 'mapping_update_interval')
        self._mapping_update_interval = mapping_update_interval
        self._cpu_con_mapping = {'timestamp': 0,
                                 'mapping': None,
                                 'concurrency lower bound': 0,
                                 'concurrency upper bound': 0}
        self._points = {}  # key: concurrency, value: (timestamp, cpu utilization (unit: core))
        if not self._enable_mapping:
            if not isinstance(concurrency, list):
                raise ValueError(f'concurrency must be list, received {concurrency} ({type(concurrency)}) instead')
            if not isinstance(cpu_utl, list):
                raise ValueError(f'cpu_utl must be list, received {cpu_utl} ({type(cpu_utl)}) instead')
            if len(concurrency) != len(cpu_utl):
                raise ValueError('concurrency and cpu_utl must be of the same length')
            for i in range(len(concurrency)):
                if not isinstance(concurrency[i], int) or concurrency[i] < 0:
                    raise ValueError(f'elements of concurrency must be non-negative integers, received {concurrency[i]}'
                                     f' ({type(concurrency[i])}) instead')
                if not type(cpu_utl[i]) in [int, float] or cpu_utl[i] <= 0:
                    raise ValueError(f'elements of cpu_utl must be positive numbers, received {cpu_utl[i]} '
                                     f'({type(cpu_utl[i])}) instead')
                self._points[concurrency[i]] = (0, cpu_utl[i])
            self._cpu_con_mapping['mapping'] = queue_model.interpolate_fitting(
                concurrency, cpu_utl, kind=self._interp_method)
            self._cpu_con_mapping['concurrency lower bound'] = min(concurrency)
            self._cpu_con_mapping['concurrency upper bound'] = max(concurrency)
        # load related variables
        self._is_master = True  # for kube-scheduler and kube-controller-manager
        self._baseline_memory = 0.0  # m_0: memory utilization during idle periods, unit: 10^6B
        self._baseline_cpu = 0.0  # c_0: cpu utilization during idle periods, unit: core
        self._latest_memory = 0.0  # if m_0 unavailable, use latest memory as m_0
        self._latest_cpu = 0.0  # if c_0 unavailable, use latest cpu as c_0
        self._latest_load_timestamp = 0
        self._load_time_window = 0  # unit: s
        self._req_num = 0  # num of req processed within a time window
        # inner requests number between apiserver and scheduler/controller-manager
        self._req_inner = 0
        self._req_in_queue = 0
        self._req_in_queue_last = 0  # for controller-manager
        self._cpu_throttle = 0.0
        self._cpu_load = 0.0  # cpu load of all req within a time window, unit: 10^-3core·s
        self._mem_load_execution = 0.0  # m_e of all req within a time window, unit: 10^6B
        self._mem_load_queuing = 0.0  # m_q of all req within a time window, unit: 10^6B
        # job scheduler
        self._scheduler = BackgroundScheduler()
        # self._load_update_job = self._scheduler.add_job(
        #     self.load_update, trigger='interval', seconds=self._load_update_interval)
        if self._enable_mapping:
            self._mapping_query_job = self._scheduler.add_job(
                self._mapping_query, trigger='interval', seconds=self._mapping_query_interval)
            self._mapping_update_job = self._scheduler.add_job(
                self._mapping_update, trigger='interval', seconds=self._mapping_update_interval)

    def __str__(self) -> str:
        return (f'Load fetcher (node {self.node_name} ({self.node_addr}), container: {self.container} '
                f'(port: {self.container_port}), kubelet port: {self.kubelet_port})')

    # get methods
    def get_cpu_con_mapping(self):
        """
        get cpu - concurrency mapping
        :return: mapping function, x points, y points
        """
        x, y = [], []
        for key in self._points:
            x.append(key)
            y.append(self._points[key][1])
        x = np.array(x)
        y = np.array(y)
        return self._cpu_con_mapping, x, y

    def get_y_max(self) -> float:
        y_max = 0.0
        for key in self._points:
            if self._points[key][1] > y_max:
                y_max = self._points[key][1]
        return y_max

    def get_mem_baseline(self) -> float:
        """
        get memory baseline (float, unit: 10^6B)
        :return: baseline_memory or latest_memory
        """
        if self._baseline_memory != 0.0:
            return self._baseline_memory  # unit: 10^6B
        else:
            return self._latest_memory

    def get_cpu_baseline(self) -> float:
        if self._baseline_cpu != 0.0:
            return self._baseline_cpu
        else:
            return self._latest_cpu

    def get_req_num_inner(self) -> int:
        return self._req_inner

    def get_load(self) -> (int, int, utils.CompRequestsSummary):
        """
        get latest load
        :return: load timestamp (end), time window, component request summary
        """
        kube_load = utils.KubeLoad(cpu_load=self._cpu_load, mem_load_execution=self._mem_load_execution,
                                   mem_load_queuing=self._mem_load_queuing)
        crs = utils.CompRequestsSummary(req_num=self._req_num, req_load=kube_load)
        return self._latest_load_timestamp, self._load_time_window, crs

    def get_req_in_queue(self) -> int:
        return self._req_in_queue

    def get_req_in_queue_last(self) -> int:
        return self._req_in_queue_last

    def get_cpu_throttle(self) -> float:
        return self._cpu_throttle

    def get_master_state(self) -> bool:
        return self._is_master

    # prom query methods
    def query_concurrency_points(self, timestamp_now: int, timestamp_last: int, concurrency_metric: int = 1,
                                 max_retries: int = 3) -> list:
        if max_retries < 1:
            max_retries = 1
        # 1. use 'apiserver_current_inflight_requests' to present concurrency (apiserver only)
        # 2. use 'apiserver_flowcontrol_current_executing_requests' to present concurrency (apiserver only)
        # 3. use 'container_sockets' to present concurrency
        if self.container == utils.KUBE_APISERVER:
            if concurrency_metric == 1:
                promql_concurrency = (
                        'sum(apiserver_current_inflight_requests{instance=\'%s\'}) - '
                        'sum(min_over_time(apiserver_current_inflight_requests{instance=\'%s\'}[30m]))'
                        % (self.instance_container, self.instance_container))

            elif concurrency_metric == 2:
                promql_concurrency = (
                        'sum(apiserver_flowcontrol_current_executing_requests{instance=\'%s\'}) - '
                        'sum(min_over_time(apiserver_flowcontrol_current_executing_requests{instance=\'%s\'}[30m]))'
                        % (self.instance_container, self.instance_container))
            else:
                promql_concurrency = (
                        'sum(container_sockets{container=\'%s\', instance=\'%s\'}) - '
                        'sum(min_over_time(container_sockets{container=\'%s\', instance=\'%s\'}[30m]))'
                        % (self.container, self.instance_kubelet, self.container, self.instance_kubelet))
        else:
            promql_concurrency = (
                    'sum(container_sockets{container=\'%s\', instance=\'%s\'}) - '
                    'sum(min_over_time(container_sockets{container=\'%s\', instance=\'%s\'}[30m]))'
                    % (self.container, self.instance_kubelet, self.container, self.instance_kubelet))
        return self._query_points(promql=promql_concurrency, data_name='concurrency', ts_now=timestamp_now,
                                  ts_last=timestamp_last, max_retries=max_retries)

    def query_cpu_points(self, timestamp_now: int, timestamp_last: int, max_retries: int = 3) -> list:
        promql_cpu = ('sum(rate(container_cpu_usage_seconds_total{container=\'%s\', instance=\'%s\'}[1m]))'
                      % (self.container, self.instance_kubelet))
        return self._query_points(promql=promql_cpu, data_name='cpu', ts_now=timestamp_now,
                                  ts_last=timestamp_last, max_retries=max_retries)

    def query_cpu_limit_points(self, timestamp_now: int, timestamp_last: int, max_retries: int = 3) -> list:
        promql_cpu_limit = ('sum(container_spec_cpu_quota{container=\'%s\', instance=\'%s\'})/1e5'
                            % (self.container, self.instance_kubelet))
        return self._query_points(promql=promql_cpu_limit, data_name='cpu limit', ts_now=timestamp_now,
                                  ts_last=timestamp_last, max_retries=max_retries)

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

    def query_cpu_throttle(self, timestamp_now: int, timestamp_last: int, max_retries: int = 3) -> float | None:
        promql_throttle = ('sum(increase(container_cpu_cfs_throttled_periods_total{container=\'%s\',instance=\'%s\'}['
                           '%ds]))/sum(increase(container_cpu_cfs_periods_total{container=\'%s\',instance=\'%s\'}['
                           '%ds]))' % (self.container, self.instance_kubelet, timestamp_now - timestamp_last,
                                       self.container, self.instance_kubelet, timestamp_now - timestamp_last))
        return self._query_point(promql=promql_throttle, data_name='cpu throttle', ts_now=timestamp_now,
                                 data_type='float', max_retries=max_retries)

    def query_req_num(self, timestamp_now: int, timestamp_last: int, max_retries: int = 3) -> int | None:
        if self.container == utils.ETCD:
            promql_req_num = ('sum(increase(etcd_request_duration_seconds_count{instance=\'%s\'}[%ds]))'
                              % (self.instance_container, timestamp_now - timestamp_last))
        elif self.container == utils.KUBE_APISERVER:
            promql_req_num = ('sum(increase(apiserver_request_duration_seconds_count{verb!=\'WATCH\','
                              'instance=\'%s\'}[%ds]))' % (self.instance_container, timestamp_now - timestamp_last))
        elif self.container == utils.KUBE_SCHEDULER:
            promql_req_num = ('sum(increase(scheduler_pod_scheduling_attempts_count{instance=\'%s\'}[%ds]))'
                              % (self.instance_container, timestamp_now - timestamp_last))
        else:  # controller-manager
            promql_req_num = ('sum(increase(workqueue_work_duration_seconds_count{endpoint=\'kube-controller-manager\','
                              ' instance=\'%s\'}[%ds]))' % (self.instance_container, timestamp_now - timestamp_last))
        req_num = self._query_point(promql=promql_req_num, data_name='request number', ts_now=timestamp_now,
                                    data_type='float', max_retries=max_retries)
        if req_num is not None:
            req_num = int(req_num)
        return req_num

    def query_req_inner(self, timestamp_now: int, timestamp_last: int, max_retries: int = 3) -> int | None:
        if self.container not in [utils.KUBE_SCHEDULER,
                                  utils.KUBE_CONTROLLER_MANAGER]:
            return None
        promql_req_inner = ('sum(increase(rest_client_request_duration_seconds_count{endpoint=\'%s\',instance=\'%s\'}['
                            '%ds]))') % (self.container, self.instance_container, timestamp_now - timestamp_last)
        req_inner = self._query_point(promql=promql_req_inner, data_name='inner request number', ts_now=timestamp_now,
                                      data_type='float', max_retries=max_retries)
        if req_inner is not None:
            req_inner = int(req_inner)
        return req_inner

    def query_req_in_queue(self, timestamp: int, max_retries: int = 3) -> int | None:
        if self.container == utils.ETCD:
            return None  # TODO: how to estimate queue length of etcd
        elif self.container == utils.KUBE_APISERVER:
            promql_req_in_queue = ('sum(apiserver_current_inqueue_requests{instance=\'%s\'})'
                                   % self.instance_container)
        elif self.container == utils.KUBE_SCHEDULER:
            promql_req_in_queue = ('sum(scheduler_pending_pods{queue=\'active\', instance=\'%s\'})'
                                   % self.instance_container)
        else:  # controller-manager
            promql_req_in_queue = ('sum(workqueue_depth{endpoint=\'kube-controller-manager\',instance=\'%s\'})'
                                   % self.instance_container)
        return self._query_point(promql=promql_req_in_queue, data_name='queue length', ts_now=timestamp,
                                 data_type='int', max_retries=max_retries)

    def query_cpu_load(self, timestamp_now: int, timestamp_last: int, max_retries: int = 3) -> float | None:
        """
        query increased cpu seconds from timestamp_last to timestamp_now
        :param timestamp_now:
        :param timestamp_last:
        :param max_retries:
        :return: cpu_load (unit: 10^-3core·s) or None
        """
        promql_cpu_load = ('sum(increase(container_cpu_usage_seconds_total{container=\'%s\', instance=\'%s\'}[%ds]))'
                           % (self.container, self.instance_kubelet, timestamp_now - timestamp_last))
        cpu_load = self._query_point(promql=promql_cpu_load, data_name='cpu load', ts_now=timestamp_now,
                                     data_type='float', max_retries=max_retries)
        if cpu_load is not None:
            cpu_load *= 1000
        return cpu_load

    def query_mem_load_execution(self, timestamp_now: int, timestamp_last: int, max_retries: int = 3) -> float | None:
        """
        query increased memory allocated from timestamp_last to timestamp_now
        :param timestamp_now:
        :param timestamp_last:
        :param max_retries:
        :return: mem_load_execution (unit: 10^6B) or None
        """
        promql_mem_load_execution = ('sum(increase(go_memstats_alloc_bytes_total{instance=\'%s\'}[%ds]))'
                                     % (self.instance_container, timestamp_now - timestamp_last))
        m_e_load = self._query_point(promql=promql_mem_load_execution, data_name='execution memory load',
                                     ts_now=timestamp_now, data_type='float', max_retries=max_retries)
        if m_e_load is not None:
            m_e_load /= 1e6
        return m_e_load

    def query_mem_load_queuing(self, timestamp_now: int, timestamp_last: int, max_retries: int = 3) -> float | None:
        """
        query increased received bytes as estimation of mem_load_queuing from timestamp_last to timestamp_now
        :param timestamp_now:
        :param timestamp_last:
        :param max_retries:
        :return: mem_load_queuing (unit: 10^6B) or None
        """
        promql_mem_load_queuing = (
                'sum(increase(container_network_receive_bytes_total{pod=~\'%s.*\',instance=\'%s\'}[%ds]))'
                % (self.container, self.instance_kubelet, timestamp_now - timestamp_last))
        m_q_load = self._query_point(promql=promql_mem_load_queuing, data_name='queuing memory load',
                                     ts_now=timestamp_now, data_type='float', max_retries=max_retries)
        if m_q_load is not None:
            m_q_load /= 1e6
        return m_q_load

    def query_master_state(self, timestamp: int, max_retries: int = 3) -> bool | None:
        master_state = None
        if self.container in [utils.KUBE_CONTROLLER_MANAGER,
                              utils.KUBE_SCHEDULER]:
            promql_master = 'sum(leader_election_master_status{instance=\'%s\'})' % self.instance_container
            master_state = self._query_point(promql=promql_master, data_name='master state', ts_now=timestamp,
                                             data_type='bool', max_retries=max_retries)
        return master_state

    def query_max_concurrency(self, timestamp: int, max_retries: int = 3) -> int | None:
        max_concurrency = None
        if self.container == utils.KUBE_APISERVER:
            promql_max_concurrency = (
                    'sum(apiserver_flowcontrol_nominal_limit_seats{priority_level!=\'merkury-empty\', instance=\'%s\'})'
                    % self.instance_container)
            max_concurrency = self._query_point(promql=promql_max_concurrency, data_name='max concurrency',
                                                ts_now=timestamp, data_type='int', max_retries=max_retries)
        return max_concurrency

    # update methods
    def _update_interval(self, new_interval: int, name: str = 'mapping query interval'):
        valid_names = ['mapping query interval', 'mapping update interval', 'load update interval']
        if name not in valid_names:
            raise ValueError(f'name must be one of {valid_names}, received {name} instead')
        if new_interval <= 0:
            utils.logging_or_print(
                f'{str(self)}: invalid {name}, positive integer required, received {new_interval} instead.',
                enable_logging=self.enable_logging, level=logging.WARNING)
            return
        updated = False
        if name == valid_names[0]:
            old_interval = self._mapping_query_interval
            self._mapping_query_interval = new_interval
            if self._enable_mapping and old_interval != new_interval:
                self._scheduler.reschedule_job(self._mapping_query_job.id, trigger='interval', seconds=new_interval)
                updated = True
        elif name == valid_names[1]:
            old_interval = self._mapping_update_interval
            self._mapping_update_interval = new_interval
            if self._enable_mapping and old_interval != new_interval:
                self._scheduler.reschedule_job(self._mapping_update_job.id, trigger='interval', seconds=new_interval)
                updated = True
        else:
            old_interval = self._load_update_interval
            if old_interval != new_interval:
                self._scheduler.reschedule_job(self._load_update_job.id, trigger='interval', seconds=new_interval)
                updated = True
        if updated:
            utils.logging_or_print(f'{str(self)}: {name} is updated from {old_interval} s to {new_interval} s.',
                                   enable_logging=self.enable_logging, level=logging.INFO)

    def update_mapping_query_interval(self, new_interval: int) -> None:
        self._update_interval(new_interval, 'mapping query interval')

    def update_mapping_update_interval(self, new_interval: int) -> None:
        self._update_interval(new_interval, 'mapping update interval')

    def update_load_update_interval(self, new_interval: int) -> None:
        self._update_interval(new_interval, 'load update interval')

    def update_interp_method(self, new_method: str) -> None:
        if new_method not in utils.INTERPOLATION_METHODS:
            utils.logging_or_print(
                f'{str(self)}: invalid interpolation method,'
                f'\'linear\', \'quadratic\' or \'cubic\' required, received {new_method} instead.',
                enable_logging=self.enable_logging, level=logging.WARNING)
        if new_method != self._interp_method:
            old_method = self._interp_method
            self._interp_method = new_method
            utils.logging_or_print(
                f'{str(self)}: interpolation method is updated from {old_method} to {new_method}.',
                enable_logging=self.enable_logging, level=logging.INFO)
            self._mapping_update()

    # jobs
    def _mapping_query(self, concurrency_metric: int = 1):
        """
        Query Prometheus metrics periodically to accumulate cpu-con points
        """
        now_stamp = int(time.time())
        last_interval_stamp = now_stamp - self._mapping_query_interval
        concurrency_points = self.query_concurrency_points(
            now_stamp, last_interval_stamp, concurrency_metric=concurrency_metric)
        cpu_points = self.query_cpu_points(now_stamp, last_interval_stamp)
        cpu_limit_points = self.query_cpu_limit_points(now_stamp, last_interval_stamp)
        if prom_query.missing_data(concurrency_points) or prom_query.missing_data(cpu_points):
            utils.logging_or_print(f'{str(self)}: mapping query suspended due to incomplete data.',
                                   enable_logging=self.enable_logging, level=logging.WARNING)
            return
        # wash data points
        concurrency_washed, cpu_washed = [], []
        for i in range(len(concurrency_points)):
            if 0 < concurrency_points[i] <= cpu_points[i]:  # CPU <= concurrency
                continue
            if len(cpu_limit_points) > 0 and -0.01 <= cpu_points[i] - cpu_limit_points[i] <= 0.01:  # CPU not limited
                continue
            concurrency_washed.append(concurrency_points[i])
            cpu_washed.append(cpu_points[i])
        # if more than one concurrency are the same, use median cpu (washed) as cpu utilization
        cpu_dict = {}
        for i in range(len(concurrency_washed)):
            if concurrency_washed[i] in cpu_dict:
                cpu_dict[concurrency_washed[i]].append(cpu_washed[i])
            else:
                cpu_dict[concurrency_washed[i]] = [cpu_washed[i]]
        median_cpu = {}
        for key in cpu_dict:
            data = np.array(cpu_dict[key])
            if len(data) < 4:
                median_cpu[key] = np.median(data)
            else:
                q1 = np.percentile(data, 25)
                q3 = np.percentile(data, 75)
                iqr = q3 - q1  # type: ignore
                lower_bound = q1 - 1.5 * iqr
                upper_bound = q3 + 1.5 * iqr
                clean_data = data[(data > lower_bound) & (data < upper_bound)]
                median_cpu[key] = np.median(clean_data)
        for key in median_cpu:
            self._points[key] = (now_stamp, median_cpu[key])
        utils.logging_or_print(f'{str(self)}: mapping query finished.',
                               enable_logging=self.enable_logging, level=logging.INFO)

    def _mapping_update(self):
        """
        Update cpu utilization - concurrency mapping periodically according to points
        """
        x = np.array([key for key in self._points])
        y = np.array([self._points[key][1] for key in self._points])
        self._cpu_con_mapping['timestamp'] = int(time.time())
        self._cpu_con_mapping['mapping'] = queue_model.interpolate_fitting(x, y, kind=self._interp_method)
        self._cpu_con_mapping['concurrency lower bound'] = max(0, x.min())
        self._cpu_con_mapping['concurrency upper bound'] = x.max()
        utils.logging_or_print(f'{str(self)}: mapping update finished.',
                               enable_logging=self.enable_logging, level=logging.INFO)

    def load_update(self):
        now_stamp = int(time.time())
        last_interval_stamp = now_stamp - self._load_update_interval
        cpu_points = self.query_cpu_points(now_stamp, last_interval_stamp)
        mem_points = self.query_mem_points(now_stamp, last_interval_stamp)
        req_num = self.query_req_num(now_stamp, last_interval_stamp)
        cpu_load = self.query_cpu_load(now_stamp, last_interval_stamp)
        m_e_load = self.query_mem_load_execution(now_stamp, last_interval_stamp)
        m_q_load = self.query_mem_load_queuing(now_stamp, last_interval_stamp)
        cpu_throttle = self.query_cpu_throttle(now_stamp, last_interval_stamp, max_retries=1)
        req_in_queue = self.query_req_in_queue(now_stamp)
        # update master_state and req_inner for cm and scheduler
        if self.container in [utils.KUBE_CONTROLLER_MANAGER,
                              utils.KUBE_SCHEDULER]:
            master_state = self.query_master_state(now_stamp)
            req_inner = self.query_req_inner(now_stamp, last_interval_stamp)
            if master_state is None:
                utils.logging_or_print(
                    f'{str(self)}: master status update suspended due to incomplete data.',
                    enable_logging=self.enable_logging, level=logging.WARNING)
            else:
                self._is_master = master_state
                utils.logging_or_print(f'{str(self)}: master status updated - {self._is_master}.',
                                       enable_logging=self.enable_logging, level=logging.INFO)
            if req_inner is None:
                utils.logging_or_print(
                    f'{str(self)}: inner request number update suspended due to incomplete data.',
                    enable_logging=self.enable_logging, level=logging.WARNING)
            else:
                self._req_inner = req_inner
                utils.logging_or_print(f'{str(self)}: inner request number updated - {self._req_inner}.',
                                       enable_logging=self.enable_logging, level=logging.INFO)
        # update request load within the latest time window
        if cpu_load is None:
            utils.logging_or_print(
                f'{str(self)}: load update suspended due to incomplete data - missing cpu load data.',
                enable_logging=self.enable_logging, level=logging.WARNING)
        else:
            if m_e_load is None:  # controller-manager under high pressure is likely to lose metrics with endpoint 10257
                self._mem_load_execution = 0.0
            else:
                self._mem_load_execution = m_e_load
            if m_q_load is None:
                self._mem_load_queuing = 0.0
            else:
                self._mem_load_queuing = m_q_load
            if req_num is None:  # non-master scheduler may get [] req num metric
                self._req_num = 0
            else:
                self._req_num = req_num
            self._cpu_load = cpu_load
            self._load_time_window = now_stamp - last_interval_stamp
            self._latest_load_timestamp = now_stamp
            ts, tw, crs = self.get_load()
            utils.logging_or_print(
                f'{str(self)}: load updated, timestamp - {ts}, time window - {tw} s, component request summary '
                f'- {str(crs)}.', enable_logging=self.enable_logging, level=logging.INFO)
        # update latest memory and baseline
        if len(mem_points) == 0:
            utils.logging_or_print(
                f'{str(self)}: latest memory update suspended due to incomplete data.',
                enable_logging=self.enable_logging, level=logging.WARNING)
        else:
            self._latest_memory = mem_points[-1]
            utils.logging_or_print(f'{str(self)}: latest memory updated - {self._latest_memory} MB.',
                                   enable_logging=self.enable_logging, level=logging.INFO)
            if not self._is_master or self._req_num == 0:  # consider as idle component
                self._baseline_memory = mem_points[-1]
                utils.logging_or_print(
                    f'{str(self)}: memory baseline updated - {self._baseline_memory} MB.',
                    enable_logging=self.enable_logging, level=logging.INFO)
        # update latest cpu and baseline
        if len(cpu_points) == 0:
            utils.logging_or_print(
                f'{str(self)}: latest CPU update suspended due to incomplete data.',
                enable_logging=self.enable_logging, level=logging.WARNING)
        else:
            self._latest_cpu = cpu_points[-1]
            utils.logging_or_print(f'{str(self)}: latest CPU updated - {self._latest_cpu} core.',
                                   enable_logging=self.enable_logging, level=logging.INFO)
            if not self._is_master or self._req_num == 0:  # consider as idle component
                self._baseline_cpu = cpu_points[-1]
                utils.logging_or_print(
                    f'{str(self)}: CPU baseline updated - {self._baseline_cpu} core.',
                    enable_logging=self.enable_logging, level=logging.INFO)
        # update cpu throttle
        if cpu_throttle is None:
            utils.logging_or_print(
                f'{str(self)}: CPU throttle data update suspended due to incomplete data.',
                enable_logging=self.enable_logging, level=logging.WARNING)
        else:
            self._cpu_throttle = cpu_throttle
            utils.logging_or_print(
                f'{str(self)}: CPU throttle updated - {100 * self._cpu_throttle:.2f}%.',
                enable_logging=self.enable_logging, level=logging.INFO)
        # update queue length
        if self.container == utils.KUBE_CONTROLLER_MANAGER:
            self._req_in_queue_last = self._req_in_queue
            utils.logging_or_print(
                f'{str(self)}: last queue length updated - {self._req_in_queue_last}.',
                enable_logging=self.enable_logging, level=logging.INFO)
        if req_in_queue is None:
            utils.logging_or_print(
                f'{str(self)}: queue length data update suspended due to incomplete data.',
                enable_logging=self.enable_logging, level=logging.WARNING)
        else:
            self._req_in_queue = req_in_queue
            utils.logging_or_print(
                f'{str(self)}: queue length updated - {self._req_in_queue}.',
                enable_logging=self.enable_logging, level=logging.INFO)

    # start and shutdown
    def start(self):
        if not self._scheduler.running:
            if self._enable_mapping:
                self._mapping_query()
                self._mapping_update()
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
