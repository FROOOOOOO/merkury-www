import logging
import urllib3
import time
import psutil
import numpy as np
from collections import defaultdict
from kubernetes import client, watch


class NodeInfo:
    def __init__(self, name: str, ip: str):
        self.name = name
        self.ip = ip

    def __str__(self):
        return f'{self.name} ({self.ip})'

# constants
KUBE_APISERVER = 'kube-apiserver'
KUBE_SCHEDULER = 'kube-scheduler'
KUBE_CONTROLLER_MANAGER = 'kube-controller-manager'
ETCD = 'etcd'
CONTROL_PLANE_COMPONENTS = [KUBE_APISERVER, KUBE_SCHEDULER, KUBE_CONTROLLER_MANAGER, ETCD]
CO_LOCATED_COMPONENTS = [KUBE_APISERVER, KUBE_CONTROLLER_MANAGER, KUBE_SCHEDULER]  # replace with your config
KUBELET = 'kubelet'
# node info
MASTERS = [NodeInfo(name='test-master-0', ip='192.168.1.100'), NodeInfo(name='test-master-1', ip='192.168.1.101'),
           NodeInfo(name='test-master-2', ip='192.168.1.102')]  # replace with your master node info
PORTS = {ETCD: '2381',
         KUBE_APISERVER: '6443',
         KUBE_CONTROLLER_MANAGER: '10257',
         KUBE_SCHEDULER: '10259',
         KUBELET: '10250'}  # change it if ports are different from the default setting
NODE_ALLOCATABLE_CPU = 12.0  # unit: core, replace with your master node info
NODE_ALLOCATABLE_MEMORY = 30000  # unit: 10^6B, replace with your master node info
PROM_URL = 'http://192.168.15.78:30532'  # replace with your master node info
APISERVER_TEST_URL = 'https://192.168.1.100:6443'  # replace with your master node info
APISERVER_SUPPORT_URL = 'https://192.168.15.78:6443'  # replace with your master node info
# tokens to authenticate Prometheus query, stress test, and updater requests, generate them and fill in
# prometheus, cluster: test
AUTH_TOKEN = ''
# cluster: support
SUPPORT_TOKEN = ''
# cluster: test
TEST_TOKEN = ''
# cpu and mem requests values of control plane components, replace with your config
DEFAULT_CPU_REQ = {KUBE_APISERVER: 0.25,
                   ETCD: 0.2,
                   KUBE_CONTROLLER_MANAGER: 0.2,
                   KUBE_SCHEDULER: 0.1}  # unit: core
DEFAULT_MEM_REQ = {KUBE_APISERVER: 520,
                   ETCD: 1040,
                   KUBE_CONTROLLER_MANAGER: 520,
                   KUBE_SCHEDULER: 260}  # unit: 10^6B
# default CPU-concurrency mapping for components, obtained via offline tests using wrk
DEFAULT_CONCURRENCY = [0, 1, 2, 3, 5, 10, 20, 30, 50, 100, 200, 300]
DEFAULT_CPU_UTL = [0.09, 1.04, 1.63, 2.19, 3.15, 4.89, 6.35, 7.1, 7.62, 7.69, 7.71, 7.76]
CONCURRENCY_DICT = {KUBE_APISERVER: [0, 1, 2, 3, 5, 10, 20, 30, 50, 100, 200, 223, 349, 600],
                    KUBE_CONTROLLER_MANAGER: [0, 1, 2, 3, 5, 10, 20, 30, 50, 100, 200, 300],
                    KUBE_SCHEDULER: [0, 1, 2, 3, 5, 10, 20, 30, 50, 100, 200, 300],
                    ETCD: [0, 1, 2, 3, 5, 10, 20, 30, 50, 100, 200, 300]}
CPU_UTL_DICT = {KUBE_APISERVER: [0.12, 1.41, 2.21, 2.97, 4.27, 6.63, 8.61, 9.63, 10.33, 10.43, 10.46, 10.97, 10.24, 11.26],
                KUBE_CONTROLLER_MANAGER: [0.051, 0.59, 0.92, 1.24, 1.78, 2.76, 3.58, 4.01, 4.3, 4.34, 4.35, 4.38],
                KUBE_SCHEDULER: [0.08, 0.95, 1.48, 1.99, 2.87, 4.45, 5.78, 6.47, 6.94, 7.01, 7.02, 7.07],
                ETCD: [0.09, 1.04, 1.63, 2.19, 3.15, 4.89, 6.35, 7.1, 7.62, 7.69, 7.71, 7.76]}
# updater params
CPU_UPDATE_RANGE = 0.2
MEMORY_UPDATE_RANGE = 0.2
# recommender params
DEFAULT_CPU_GRANULARITY = 20  # 1/20 = 0.05core
DEFAULT_QUEUE_LENGTH = 50
AMPLIFIER = 1.2  # used in p99 baseline, or allocate resource for idle components
REJECT_PROB = 1e-5  # 1-p_reject>=99.999%
# params related to allocation calibration
THROTTLE_IDLE = {KUBE_APISERVER: 0.4,
                 KUBE_CONTROLLER_MANAGER: 0.3,
                 KUBE_SCHEDULER: 0.3,
                 ETCD: 0.4}
# components whose cpu throttle <= throttle_idle are considered available to lend CPU to bottleneck ones
THROTTLE_BOTTLENECK = {KUBE_APISERVER: 0.6,
                       KUBE_CONTROLLER_MANAGER: 0.7,
                       KUBE_SCHEDULER: 0.7,
                       ETCD: 0.6}
# components whose cpu throttle >= throttle_bottleneck are considered needing CPU from idle ones
LENDABLE_PERCENTAGE = 0.1
# idle components have cpu_limit[comp] * lendable_percentage CPU to lend to bottleneck ones
WEIGHTS_CALIBRATOR = 1.0
# allocation_weights for bottleneck components are added by weights_calibrator each time to acquire more resources
# in the next round of allocation
# constants for GA optimizer
GA_POP_SIZE = 1000
GA_MAX_GEN = 100
GA_SELECT_RATE = 0.2
GA_CROSS_RATE = 0.8
GA_MUTATE_RATE = 0.05
GA_ES_THRESHOLD = 0.01
GA_ES_MAX_GEN = 10
GA_TIME_SPACE_RATIO = 0.5  # ≈ optimization_time / (cpu_left * cpu_granularity)
GA_TIME_LIMIT = 10  # max allowed optimization_time
# other constants
MARGIN_ERROR = 1e-5  # if |a-b|<= MARGIN_ERROR, consider as a=b, used in mono_check


# util functions
def validate_interval(interval, name: str):
    if not isinstance(interval, int) or interval <= 0:
        raise ValueError(f'{name} must be a positive integer, received {interval} ({type(interval)}) instead')


def validate_container(container):
    if not isinstance(container, str) or container not in CONTROL_PLANE_COMPONENTS:
        raise ValueError(f'container must be one of {list(CONTROL_PLANE_COMPONENTS)}, received {container} instead')


INTERPOLATION_METHODS = ['linear', 'quadratic', 'cubic']


def validate_method(method):
    if not isinstance(method, str) or method not in INTERPOLATION_METHODS:
        raise ValueError(f'container must be one of {list(INTERPOLATION_METHODS)}, received {method} instead')


def logging_or_print(message: str, enable_logging: bool = True, level: int = logging.INFO):
    if enable_logging:
        if level not in [logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR]:
            level = logging.INFO
        if level == logging.DEBUG:
            logging.debug(message)
        elif level == logging.INFO:
            logging.info(message)
        elif level == logging.WARNING:
            logging.warning(message)
        else:  # error
            logging.error(message)
    else:
        print(message)


def monitor_pods(stop_event, namespace: str = 'kube-system', label_selector: str = 'control=test-master'):
    # monitor control plane components
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    kube_config = client.Configuration(host=APISERVER_SUPPORT_URL)
    kube_config.api_key['authorization'] = SUPPORT_TOKEN
    kube_config.api_key_prefix['authorization'] = 'Bearer'
    kube_config.verify_ssl = False
    oom_times = 0
    with client.ApiClient(kube_config) as api_client:
        api_instance = client.CoreV1Api(api_client)
        w = watch.Watch()
        while not stop_event.is_set():
            for event in w.stream(api_instance.list_namespaced_pod, namespace=namespace, label_selector=label_selector,
                                  timeout_seconds=10):
                pod_statuses = event['object'].status.container_statuses
                if pod_statuses:
                    container_status = pod_statuses[0]
                    if container_status:
                        pod_state = container_status.state
                        if pod_state.waiting and pod_state.waiting.reason == 'CrashLoopBackOff':
                            pod_name = event['object'].metadata.name
                            logging_or_print(f'monitor_pods(): deleting pod {pod_name} because of CrashLoopBackOff.',
                                             level=logging.WARNING)
                            try:
                                api_instance.delete_namespaced_pod(namespace='kube-system', name=pod_name)
                                logging_or_print(f'monitor_pods(): pod {pod_name} deleted because of CrashLoopBackOff.',
                                                 level=logging.WARNING)
                            except client.exceptions.ApiException as e:
                                logging_or_print(f'monitor_pods(): exception when deleting pod {pod_name}: {e}',
                                                 level=logging.ERROR)
                        if pod_state.terminated and pod_state.terminated.reason == 'OOMKilled':
                            oom_times += 1
                            pod_name = event['object'].metadata.name
                            logging_or_print(f'monitor_pods(): pod {pod_name} crashed due to OOM.',
                                             level=logging.WARNING)
                    else:
                        logging_or_print(f'monitor_pods(): container_statuses[0] is None for '
                                         f'pod {event["object"].metadata.name}', level=logging.WARNING)
                else:
                    logging_or_print(f'monitor_pods(): container_statuses is empty for '
                                     f'pod {event["object"].metadata.name}', level=logging.WARNING)
        logging_or_print(f'monitor_pods(): total OOM times for control plane components is {oom_times}.',
                         level=logging.INFO)


def monitor_cost(pid: int, interval: int, stop_event):
    cpu_times, mem_usages = [], []
    cumulative_cpu_time = 0
    # print(f'Monitoring main program with PID {pid}, sampling interval: {interval} s.')
    logging_or_print(f'monitor_cost(): monitoring recommender program with PID {pid}, sampling interval: {interval} s.',
                     level=logging.INFO)
    while not stop_event.is_set():
        try:
            p = psutil.Process(pid)
            cpu_time = p.cpu_times().user + p.cpu_times().system - cumulative_cpu_time
            if cumulative_cpu_time == 0:
                cpu_time_average = cpu_time / (time.time() - p.create_time())
            else:
                cpu_time_average = cpu_time / interval
            cumulative_cpu_time += cpu_time
            cpu_times.append(cpu_time_average)
            mem_usages.append(p.memory_info().rss)
            time.sleep(interval)
        except psutil.NoSuchProcess:
            # print(f"Process {pid} does not exist.")
            logging_or_print(f'monitor_cost(): process {pid} does not exist.', level=logging.WARNING)
            break
        except KeyboardInterrupt:
            break
    if cpu_times and mem_usages:
        avg_cpu = np.mean(cpu_times)
        avg_mem = np.mean(mem_usages)
        p99_cpu = np.percentile(cpu_times, 99)
        p99_mem = np.percentile(mem_usages, 99)
        # print(f'Average CPU usage: {avg_cpu:.2f}; Average memory usage: {avg_mem / 1e6:.2f} MB; '
        #       f'P99 CPU usage: {p99_cpu:.2f}; P99 memory usage: {p99_mem / 1e6:.2f} MB.')
        logging_or_print(f'monitor_cost(): pid: {pid}; average CPU usage: {avg_cpu:.2f}; average memory usage: '
                         f'{avg_mem / 1e6:.2f} MB; P99 CPU usage: {p99_cpu:.2f}; P99 memory usage: '
                         f'{p99_mem / 1e6:.2f} MB.', level=logging.INFO)
    else:
        # print(f'No data for process {pid}.')
        logging_or_print(f'monitor_cost(): no cost data for process {pid}.', level=logging.WARNING)


def int_to_binary_list(number: int, length: int) -> list:  # used in optimizer crossover
    if length < 1:
        raise ValueError('length must be a positive integer')
    if number < 0 or number > 2 ** length - 1:
        raise ValueError('number must be an integer in [1, 2 ** length - 1]')
    binary_list = []
    while number > 0:
        remainder = number % 2
        binary_list.append(remainder)
        number = number // 2
    binary_list.reverse()
    binary_list = [0] * (length - len(binary_list)) + binary_list
    return binary_list


# types and classes
def nested_dict():  # double key, e.g. dict[k1][k2]=v
    return defaultdict(dict)


def nn_dict():  # triple key, e.g. dict[k1][k2][k3]=v
    return defaultdict(nested_dict)


class KubeLoad:
    # request load to a kube component
    def __init__(self, cpu_load: float = 0.0, mem_load_execution: float = 0.0, mem_load_queuing: float = 0.0,
                 empty: bool = False):
        if empty:  # empty load
            self.cpu_load, self.mem_load_execution, self.mem_load_queuing = 0.0, 0.0, 0.0
        else:
            self.cpu_load = max(0.0, cpu_load)  # execution time per cpu core (unit: ms·core)
            self.mem_load_execution = max(0.0, mem_load_execution)  # memory load during execution (unit: 10^6B)
            self.mem_load_queuing = max(0.0, mem_load_queuing)  # memory load during queuing (unit: 10^6B)

    def __mul__(self, other: int | float):
        if type(other) in [int, float]:
            return KubeLoad(cpu_load=self.cpu_load * other, mem_load_execution=self.mem_load_execution * other,
                            mem_load_queuing=self.mem_load_queuing * other)
        else:
            raise TypeError("Unsupported operand type.")

    def __truediv__(self, other: int | float):
        if other == 0:
            raise ZeroDivisionError("Division by zero is not allowed.")
        if type(other) in [int, float]:
            return KubeLoad(cpu_load=self.cpu_load / other, mem_load_execution=self.mem_load_execution / other,
                            mem_load_queuing=self.mem_load_queuing / other)
        else:
            raise TypeError("Unsupported operand type.")

    def __add__(self, other):
        if isinstance(other, KubeLoad):
            return KubeLoad(cpu_load=self.cpu_load + other.cpu_load,
                            mem_load_execution=self.mem_load_execution + other.mem_load_execution,
                            mem_load_queuing=self.mem_load_queuing + other.mem_load_queuing)
        else:
            raise TypeError("Unsupported oprand type.")

    def __str__(self) -> str:
        return (f'cpu load: {self.cpu_load:.3f} ms·core, mem load (execution): {self.mem_load_execution:.3f} MB, '
                f'mem load (queuing): {self.mem_load_queuing:.3f} MB')

    def is_empty(self) -> bool:
        return self.cpu_load == 0.0 and self.mem_load_execution == 0.0 and self.mem_load_queuing == 0.0


class KubeRequest:
    # one kube request
    def __init__(self, url: str = 'api/v1/nodes', kind: str = 'GET',
                 apiserver_load: KubeLoad = KubeLoad(),
                 etcd_load: KubeLoad = KubeLoad(empty=True),
                 scheduler_load: KubeLoad = KubeLoad(empty=True),
                 cm_load: KubeLoad = KubeLoad(empty=True)):
        self.url = url
        self.kind = kind  # HTTP method
        self.apiserver_load = apiserver_load
        self.etcd_load = etcd_load
        self.scheduler_load = scheduler_load
        self.cm_load = cm_load

    def __str__(self) -> str:
        return self.kind + ' ' + self.url


class CompRequestsSummary:
    # summary of KubeRequests per component
    def __init__(self, req_num: int = 0, req_load: KubeLoad = KubeLoad(empty=True)):
        self.req_num = max(0, req_num)
        self.req_load = req_load

    def __str__(self):
        return f'request number: {self.req_num}, {str(self.req_load)}'


class KubeRequestsSummary:
    # summary of all KubeRequests within one time window
    def __init__(self, timestamp: int = 0, time_window: int = 60,
                 apiserver_load: CompRequestsSummary = CompRequestsSummary(),
                 etcd_load: CompRequestsSummary = CompRequestsSummary(),
                 scheduler_load: CompRequestsSummary = CompRequestsSummary(),
                 cm_load: CompRequestsSummary = CompRequestsSummary()):
        self.timestamp = timestamp  # start timestamp
        self.time_window = time_window  # unit: s
        self.summary = dict()
        for comp in CONTROL_PLANE_COMPONENTS:
            if comp == ETCD:
                self.summary[comp] = etcd_load
            if comp == KUBE_APISERVER:
                self.summary[comp] = apiserver_load
            if comp == KUBE_CONTROLLER_MANAGER:
                self.summary[comp] = cm_load
            if comp == KUBE_SCHEDULER:
                self.summary[comp] = scheduler_load

    def __str__(self) -> str:
        return (f'start timestamp: {self.timestamp}, time window: {self.time_window} s,\n'
                f'etcd: ({str(self.summary[ETCD])})\nkube-apiserver: ({str(self.summary[KUBE_APISERVER])})\n'
                f'kube-controller-manager: ({str(self.summary[KUBE_CONTROLLER_MANAGER])})\n'
                f'kube-scheduler: ({str(self.summary[KUBE_SCHEDULER])})')

    def get_end_timestamp(self) -> int:
        return self.timestamp + self.time_window

    def get_cpu_load(self, comp: str) -> float:
        if comp not in self.summary:
            raise KeyError(f'comp {comp} does not exist')
        return self.summary[comp].req_load.cpu_load

    def get_memory_load_e(self, comp: str) -> float:
        if comp not in self.summary:
            raise KeyError(f'comp {comp} does not exist')
        return self.summary[comp].req_load.mem_load_execution

    def get_memory_load_q(self, comp: str) -> float:
        if comp not in self.summary:
            raise KeyError(f'comp {comp} does not exist')
        return self.summary[comp].req_load.mem_load_queuing

    def get_req_num(self, comp: str) -> int:
        if comp not in self.summary:
            raise KeyError(f'comp {comp} does not exist')
        return self.summary[comp].req_num


class Recommendation:
    # recommendation for one control plane component
    def __init__(self, concurrency: int = 0, max_ql: int = 0,
                 cpu_alloc: int = 0, memory_alloc: int = 0):
        self.concurrency = concurrency  # max allowed concurrent executing requests
        self.max_ql = max_ql  # max queue length
        self.cpu_alloc = cpu_alloc  # cpu recommendation (10^(-3)core)
        self.memory_alloc = memory_alloc  # memory recommendation (10^6B)

    def __str__(self) -> str:
        return (f'concurrency: {self.concurrency}, max queue length: {self.max_ql}, cpu: {self.cpu_alloc} mcore, '
                f'memory: {self.memory_alloc} MB')


class RecommendationForNode:
    # recommendation for one master node, including recommendations for one or more components in apiserver, etcd,
    # scheduler and controller-manager
    def __init__(self, recommendations: dict[str, Recommendation], timestamp: int = 0):
        self.timestamp = timestamp  # unix timestamp (unit: s)
        self.recommendations = recommendations

    def __str__(self) -> str:
        str_ = f'timestamp: {self.timestamp}'
        for key, value in self.recommendations.items():
            str_ += f', {key}: ({str(value)})'
        return str_


# classes used for new allocation algorithm
class ComponentStats:
    """
    statics for control plane components (base class)
    """

    def __init__(self, cpu_load: float = 0.0, cpu_throttle: float = 0.0, req_num: int = 0, delta_t: int = 60,
                 empty: bool = False):
        self._empty = empty
        if self._empty:
            self._cpu_load = 0.0  # unit: 10^(-3)s·core
            self._cpu_throttle = 0.0  # in [0, 1]
            self._req_num = 0
        else:
            self._cpu_load = max(0.0, cpu_load)
            self._req_num = max(0, req_num)
            self._cpu_throttle = min(1.0, max(0.0, cpu_throttle))
        self._delta_t = max(1, delta_t)

    def is_empty(self) -> bool:
        return self._empty

    def get_cpu_load(self) -> float:
        return self._cpu_load

    def get_cpu_throttle(self) -> float:
        return self._cpu_throttle

    def get_req_num(self) -> int:
        return self._req_num

    def get_delta_t(self) -> int:
        return self._delta_t

    def get_delta_t_ms(self) -> int:
        return self._delta_t * 1000

    def __mul__(self, other: int | float):
        if type(other) in [int, float]:
            return ComponentStats(cpu_load=self._cpu_load * other, cpu_throttle=self._cpu_throttle,
                                  req_num=int(self._req_num * other), delta_t=self._delta_t, empty=self._empty)
        else:
            raise TypeError("Unsupported operand type.")

    def __truediv__(self, other: int | float):
        if other == 0:
            raise ZeroDivisionError("Division by zero is not allowed.")
        if type(other) in [int, float]:
            return ComponentStats(cpu_load=self._cpu_load / other, cpu_throttle=self._cpu_throttle,
                                  req_num=int(self._req_num / other), delta_t=self._delta_t, empty=self._empty)
        else:
            raise TypeError("Unsupported operand type.")


class SyncCompStats(ComponentStats):
    """
    statics describing synchronized components (apiserver and etcd)
    """

    def __init__(self, concurrency: np.ndarray = None, cpu_utl: np.ndarray = None, cpu_load: float = 0.0,
                 cpu_throttle: float = 0.0, req_num: int = 0, req_scheduler: int = 0, req_cm: int = 0,
                 delta_t: int = 60, empty: bool = False):
        super().__init__(cpu_load, cpu_throttle, req_num, delta_t, empty)
        if self._empty:
            self._concurrency = None
            self._cpu_utl = None
            self._req_scheduler = 0
            self._req_cm = 0
        else:
            if concurrency is None:
                concurrency = DEFAULT_CONCURRENCY
            self._concurrency = concurrency
            if cpu_utl is None:
                cpu_utl = DEFAULT_CPU_UTL
            self._cpu_utl = cpu_utl
            self._req_scheduler = max(0, req_scheduler)
            self._req_cm = max(0, req_cm)

    def get_concurrency(self) -> np.ndarray | None:
        return self._concurrency

    def get_cpu_utl(self) -> np.ndarray | None:
        return self._cpu_utl

    def get_req_scheduler(self) -> int:
        return self._req_scheduler

    def get_req_cm(self) -> int:
        return self._req_cm

    def __mul__(self, other: int | float):
        if type(other) in [int, float]:
            return SyncCompStats(concurrency=self._concurrency, cpu_utl=self._cpu_utl, cpu_load=self._cpu_load * other,
                                 cpu_throttle=self._cpu_throttle, req_num=int(self._req_num * other),
                                 req_scheduler=self._req_scheduler, req_cm=self._req_cm, delta_t=self._delta_t,
                                 empty=self._empty)
        else:
            raise TypeError("Unsupported operand type.")

    def __truediv__(self, other: int | float):
        if other == 0:
            raise ZeroDivisionError("Division by zero is not allowed.")
        if type(other) in [int, float]:
            return SyncCompStats(concurrency=self._concurrency, cpu_utl=self._cpu_utl, cpu_load=self._cpu_load / other,
                                 cpu_throttle=self._cpu_throttle, req_num=int(self._req_num / other),
                                 req_scheduler=self._req_scheduler, req_cm=self._req_cm, delta_t=self._delta_t,
                                 empty=self._empty)
        else:
            raise TypeError("Unsupported operand type.")


class AsyncCompStats(ComponentStats):
    """
    statics describing asynchronized components (scheduler and controller-manager)
    """

    def __init__(self, cpu_load: float = 0.0, cpu_throttle: float = 0.0, req_num: int = 0, req_in_queue: int = 0,
                 delta_t: int = 60, empty: bool = False):
        super().__init__(cpu_load, cpu_throttle, req_num, delta_t, empty)
        if self._empty:
            self._req_in_queue = 0
        else:
            self._req_in_queue = max(0, req_in_queue)

    def get_req_in_queue(self) -> int:
        return self._req_in_queue

    def __mul__(self, other: int | float):
        if type(other) in [int, float]:
            return AsyncCompStats(cpu_load=self._cpu_load * other, cpu_throttle=self._cpu_throttle,
                                  req_num=int(self._req_num * other), req_in_queue=self._req_in_queue,
                                  delta_t=self._delta_t, empty=self._empty)
        else:
            raise TypeError("Unsupported operand type.")

    def __truediv__(self, other: int | float):
        if other == 0:
            raise ZeroDivisionError("Division by zero is not allowed.")
        if type(other) in [int, float]:
            return AsyncCompStats(cpu_load=self._cpu_load / other, cpu_throttle=self._cpu_throttle,
                                  req_num=int(self._req_num / other), req_in_queue=self._req_in_queue,
                                  delta_t=self._delta_t, empty=self._empty)
        else:
            raise TypeError("Unsupported operand type.")
