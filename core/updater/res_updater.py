import logging
import time

import requests
import json
from kubernetes import client
from kubernetes.client import ApiException
from core.utils import utils
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class ResourceUpdater:
    """update control plane component pods resource (CPU, memory limit)"""

    def __init__(self, apiserver_url: str = utils.APISERVER_SUPPORT_URL, api_token: str = utils.SUPPORT_TOKEN,
                 cpu_requests: dict = utils.DEFAULT_CPU_REQ, memory_requests: dict = utils.DEFAULT_MEM_REQ,
                 collocated_components: list = utils.CONTROL_PLANE_COMPONENTS, enable_logging: bool = True,
                 cpu_total: float = utils.NODE_ALLOCATABLE_CPU, mem_total: int = utils.NODE_ALLOCATABLE_MEMORY,
                 cpu_update_range: float = utils.CPU_UPDATE_RANGE, mem_update_range: float = utils.MEMORY_UPDATE_RANGE):
        self.apiserver_url = apiserver_url
        self.api_token = api_token
        self.cpu_requests = cpu_requests
        self.memory_requests = memory_requests
        self.collocated_components = collocated_components
        self.cpu_total = int(1000 * cpu_total)  # unit: 10^(-3)core
        self.mem_total = mem_total  # unit: 10^6B
        # (current_limit - update_range, current_limit + update_range)
        self.cpu_update_range = int(cpu_update_range * self.cpu_total)
        self.mem_update_range = int(mem_update_range * self.mem_total)
        self.enable_logging = enable_logging  # set False to debug in terminals
        self.config = client.Configuration(host=apiserver_url)
        self.config.api_key['authorization'] = self.api_token
        self.config.api_key_prefix['authorization'] = 'Bearer'
        self.config.verify_ssl = False

    @classmethod
    def convert_cpu_str(cls, cpu_str: str) -> int:
        """convert cpu unit to 10^(-3)core"""
        if cpu_str[-1] == 'm':  # e.g, 100m
            cpu = int(cpu_str[:-1])
        else:
            cpu = int(float(cpu_str) * 1000)
        return cpu

    @classmethod
    def convert_mem_str(cls, mem_str: str) -> int:
        """convert memory unit to 10^6B"""
        if mem_str.endswith('m'):  # 10^(-3)B
            mem = float(mem_str[:-1]) / 1e9
        elif mem_str[-1].isdigit():  # B
            mem = float(mem_str) / 1e6
        elif mem_str.endswith('k'):  # 10^3B
            mem = float(mem_str[:-1]) / 1e3
        elif mem_str.endswith('Ki'):  # 2^10B
            mem = float(mem_str[:-2]) * 1024 / 1e6
        elif mem_str.endswith('M'):  # 10^6B
            mem = float(mem_str[:-1])
        elif mem_str.endswith('Mi'):  # 2^20B
            mem = float(mem_str[:-2]) * 1024 * 1024 / 1e6
        elif mem_str.endswith('G'):  # 10^9B
            mem = float(mem_str[:-1]) * 1e3
        elif mem_str.endswith('Gi'):  # 2^30B
            mem = float(mem_str[:-2]) * 1024 * 1024 * 1024 / 1e6
        else:  # ignore unit larger than G/Gi
            mem = 0
        return int(mem)

    def get_pod_limit(self, component: str, pod_name: str) -> (int | None, int | None):
        if component not in utils.CONTROL_PLANE_COMPONENTS:
            utils.logging_or_print('ResourceUpdater: invalid component name, update canceled.',
                                   enable_logging=self.enable_logging, level=logging.WARNING)
            return None, None
        with client.ApiClient(self.config) as api_client:
            api_instance = client.CoreV1Api(api_client)
            try:
                current_pod = api_instance.read_namespaced_pod(name=pod_name, namespace='kube-system')
            except ApiException as e:
                utils.logging_or_print(f'ResourceUpdater: failed to get pod {pod_name} data: {e}, query canceled.',
                                       enable_logging=self.enable_logging, level=logging.ERROR)
                return None, None
            cpu_limit, mem_limit = None, None
            current_limits = current_pod.spec.containers[0].resources.limits
            if current_limits is not None and 'cpu' in current_limits.keys() and 'memory' in current_limits.keys():
                current_limits_cpu_str = current_limits['cpu']
                cpu_limit = self.convert_cpu_str(current_limits_cpu_str)
            if current_limits is not None and 'cpu' in current_limits.keys() and 'memory' in current_limits.keys():
                current_limits_memory_str = current_limits['memory']
                mem_limit = self.convert_mem_str(current_limits_memory_str)
            return cpu_limit, mem_limit

    def update_resource_with_k8s_client(self, component: str, pod_name: str, cpu_value: int = None,
                                        memory_value: int = None, ignore_update_range: bool = False):
        # only pod with resource requests can PATCH limits, QoS class (burstable) cannot be changed
        if component not in utils.CONTROL_PLANE_COMPONENTS:
            utils.logging_or_print('ResourceUpdater: invalid component name, update canceled.',
                                   enable_logging=self.enable_logging, level=logging.WARNING)
            return
        with client.ApiClient(self.config) as api_client:
            api_instance = client.CoreV1Api(api_client)
            try:
                current_pod = api_instance.read_namespaced_pod(name=pod_name, namespace='kube-system')
            except ApiException as e:
                utils.logging_or_print(f'ResourceUpdater: failed to get pod {pod_name} data: {e}, update canceled.',
                                       enable_logging=self.enable_logging, level=logging.ERROR)
                return
            # check current pod's requests
            current_requests = current_pod.spec.containers[0].resources.requests
            if current_requests is None or 'cpu' not in current_requests.keys() or \
                    'memory' not in current_requests.keys():
                utils.logging_or_print(
                    f'ResourceUpdater: resource.requests of pod {pod_name} is incomplete, update canceled.',
                    enable_logging=self.enable_logging, level=logging.WARNING)
                return
            current_requests_cpu_str = current_requests['cpu']
            current_requests_memory_str = current_requests['memory']
            current_requests_cpu = self.convert_cpu_str(current_requests_cpu_str)
            current_requests_memory = self.convert_mem_str(current_requests_memory_str)
            if cpu_value is None:
                cpu_value, _ = self.get_pod_limit(component, pod_name)
            if memory_value is None:
                _, memory_value = self.get_pod_limit(component, pod_name)
            if cpu_value is None or memory_value is None:
                utils.logging_or_print('ResourceUpdater: empty cpu_value or memory_value, update canceled.',
                                       enable_logging=self.enable_logging, level=logging.WARNING)
                return
            if cpu_value < current_requests_cpu:
                utils.logging_or_print(
                    f'ResourceUpdater: CPU limits {cpu_value} < CPU requests {current_requests_cpu}, update canceled.',
                    enable_logging=self.enable_logging, level=logging.WARNING)
                return
            if memory_value < current_requests_memory:
                utils.logging_or_print(
                    f'ResourceUpdater: memory limits {memory_value} < memory requests {current_requests_memory}, '
                    f'update canceled.', enable_logging=self.enable_logging, level=logging.WARNING)
                return
            if cpu_value == current_requests_cpu and memory_value == current_requests_memory:
                cpu_value += 1  # over-alloc 1m CPU and 1MB mem to avoid changing QoS class of pod
                memory_value += 1
            current_limits = current_pod.spec.containers[0].resources.limits
            if current_limits is not None and 'cpu' in current_limits.keys() and 'memory' in current_limits.keys():
                current_limits_cpu_str = current_limits['cpu']
                current_limits_memory_str = current_limits['memory']
                current_limits_cpu = self.convert_cpu_str(current_limits_cpu_str)
                current_limits_memory = self.convert_mem_str(current_limits_memory_str)
                # if update value = current limit, cancel
                if current_limits_cpu == cpu_value and current_limits_memory == memory_value:
                    utils.logging_or_print(
                        f'ResourceUpdater: resource limit of pod {pod_name} unchanged, update canceled',
                        enable_logging=self.enable_logging, level=logging.INFO)
                    return
                if not ignore_update_range:
                    # if update value exceeds update range, change update
                    if cpu_value < current_limits_cpu - self.cpu_update_range:
                        cpu_value = current_limits_cpu - self.cpu_update_range
                    if cpu_value > current_limits_cpu + self.cpu_update_range:
                        cpu_value = current_limits_cpu + self.cpu_update_range
                    if memory_value < current_limits_memory - self.mem_update_range:
                        memory_value = current_limits_memory - self.mem_update_range
                    if memory_value > current_limits_memory + self.mem_update_range:
                        memory_value = current_limits_memory + self.mem_update_range
            current_pod.spec.containers[0].resources.limits = {'cpu': f'{cpu_value}m', 'memory': f'{memory_value}M'}
            try:
                response = api_instance.replace_namespaced_pod(name=pod_name, namespace='kube-system', body=current_pod)
                utils.logging_or_print(f'ResourceUpdater: resource limit of pod {pod_name} successfully updated '
                                       f'(CPU: {response.spec.containers[0].resources.limits["cpu"]}, '
                                       f'memory: {response.spec.containers[0].resources.limits["memory"]}).',
                                       enable_logging=self.enable_logging, level=logging.INFO)
            except ApiException as e:
                utils.logging_or_print(f'ResourceUpdater: failed to update pod {pod_name} resource: {e}.',
                                       enable_logging=self.enable_logging, level=logging.ERROR)

    def reset_resource_limit(self, component: str, pod_name: str):
        if component not in utils.CONTROL_PLANE_COMPONENTS:
            utils.logging_or_print('ResourceUpdater: invalid component name, update canceled.',
                                   enable_logging=self.enable_logging, level=logging.WARNING)
            return
        with client.ApiClient(self.config) as api_client:
            api_instance = client.CoreV1Api(api_client)
            try:
                current_pod = api_instance.read_namespaced_pod(name=pod_name, namespace='kube-system')
            except ApiException as e:
                utils.logging_or_print(f'ResourceUpdater: failed to get pod {pod_name} data: {e}, update canceled.',
                                       enable_logging=self.enable_logging, level=logging.ERROR)
                return
            current_limits = current_pod.spec.containers[0].resources.limits
            if current_limits is not None:
                self.update_resource_with_k8s_client(component, pod_name, cpu_value=self.cpu_total,
                                                     memory_value=self.mem_total, ignore_update_range=True)
                utils.logging_or_print(f'ResourceUpdater: resource limit of pod {pod_name} successfully reset.',
                                       enable_logging=self.enable_logging, level=logging.INFO)

    def reset_resource_limit_of_all_components(self, restart: bool = False, interval: int = 15):
        if interval <= 0:
            interval = 15
        for comp in self.collocated_components:
            for i in range(3):
                pod_name = f'{comp}-{i}-0'
                if restart:
                    with client.ApiClient(self.config) as api_client:
                        api_instance = client.CoreV1Api(api_client)
                        try:
                            api_instance.delete_namespaced_pod(name=pod_name, namespace='kube-system')
                        except ApiException as e:
                            utils.logging_or_print(f'ResourceUpdater: failed to delete pod {pod_name}: {e}.',
                                                   enable_logging=self.enable_logging, level=logging.ERROR)
                        utils.logging_or_print(f'ResourceUpdater: pod {pod_name} restart successfully, '
                                               f'waiting {interval}s to restart next pod.',
                                               enable_logging=self.enable_logging, level=logging.INFO)
                        time.sleep(interval)
                else:
                    self.reset_resource_limit(component=comp, pod_name=pod_name)
        utils.logging_or_print(
            f'ResourceUpdater: resource reset (restart={restart}) complete.', enable_logging=self.enable_logging,
            level=logging.INFO)

    def delete_pod(self, component: str, pod_name: str):
        if component not in utils.CONTROL_PLANE_COMPONENTS:
            utils.logging_or_print('ResourceUpdater: invalid component name, update canceled.',
                                   enable_logging=self.enable_logging, level=logging.WARNING)
            return
        with client.ApiClient(self.config) as api_client:
            api_instance = client.CoreV1Api(api_client)
            try:
                api_instance.delete_namespaced_pod(name=pod_name, namespace='kube-system')
            except ApiException as e:
                utils.logging_or_print(f'ResourceUpdater: failed to delete pod {pod_name}: {e}.',
                                       enable_logging=self.enable_logging, level=logging.ERROR)
            utils.logging_or_print(f'ResourceUpdater: pod {pod_name} delete successfully.',
                                   enable_logging=self.enable_logging, level=logging.INFO)

    # DEPRECATED
    def update_resource(self, component: str, pod_name: str, cpu_value: int,
                        memory_value: int):  # simple version, deprecated
        if component not in utils.CONTROL_PLANE_COMPONENTS:
            print('Invalid component name, update canceled')
            return
        if cpu_value < self.cpu_requests[component]:
            print('CPU limits < CPU requests, update canceled')
            return
        if memory_value < self.memory_requests[component]:
            print('Memory limits < memory requests, update canceled')
            return
        url = self.apiserver_url + f'/api/v1/namespaces/kube-system/pods/{pod_name}'
        headers = {'Authorization': f'Bearer {self.api_token}', 'Content-Type': 'application/json-patch+json'}
        payload = [
            {
                'op': 'replace',
                'path': '/spec/containers/0/resources/limits',
                'value': {
                    'cpu': f'{cpu_value}m',
                    'memory': f'{memory_value}Mi'
                }
            }
        ]
        response = requests.patch(url, headers=headers, data=json.dumps(payload), verify=False)
        if response.status_code // 100 != 2:
            raise Exception('Error: ' + str(response.status_code) + ', ' + response.text)
        else:
            print(f'Resource limit of pod {pod_name} successfully updated.')
