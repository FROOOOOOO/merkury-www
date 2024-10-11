import logging
from kubernetes import client
from kubernetes.client import ApiException
from core.utils import utils
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


class TcUpdater:
    """update traffic control params of kube-apiservers in test-cluster by PATCH prioritylevelconfiguration"""

    def __init__(self, apiserver_url: str = utils.APISERVER_TEST_URL, api_token: str = utils.TEST_TOKEN,
                 total_seats: int = 3000, plc_name: str = 'merkury-empty', enable_logging: bool = False):
        self.apiserver_url = apiserver_url
        self.api_token = api_token
        self.total_seats = total_seats  # --max-requests-inflight + --max-mutating-requests-inflight
        self.plc_name = plc_name
        self.enable_logging = enable_logging  # set False to debug in terminals
        self.config = client.Configuration(host=apiserver_url)
        self.config.api_key['authorization'] = self.api_token
        self.config.api_key_prefix['authorization'] = 'Bearer'
        self.config.verify_ssl = False
        self.default_plc_list = ['global-default', 'leader-election', 'node-high', 'system', 'workload-high',
                                 'workload-low']
        self.default_plc_ql = 50

    def update_fc_with_k8s_client(self, concurrency: int, queue_length: int):
        if concurrency <= 0:
            utils.logging_or_print(f'TcUpdater: concurrency {concurrency} <= 0, update canceled.',
                                   enable_logging=self.enable_logging, level=logging.WARNING)
            return
        if queue_length <= 0:  # cannot update queueLengthLimit to 0
            utils.logging_or_print(f'TcUpdater: queue length {queue_length} <= 0, update canceled.',
                                   enable_logging=self.enable_logging, level=logging.WARNING)
            return
        # update ncs of merkury-empty plc
        if concurrency > self.total_seats:
            utils.logging_or_print(f'TcUpdater: concurrency {concurrency} exceeds total seats {self.total_seats}, '
                                   f'try updating concurrency = total seats.',
                                   enable_logging=self.enable_logging, level=logging.WARNING)
            ncs_merkury_empty = 0
        else:
            ncs_merkury_empty = int(245 * (self.total_seats - concurrency) / concurrency)
            # 245 = sum ncs of 6 default plc
        with (client.ApiClient(self.config) as api_client):
            api_instance = client.FlowcontrolApiserverV1beta3Api(api_client)
            try:
                current_merkury_empty_plc = api_instance.read_priority_level_configuration(name=self.plc_name)
            except ApiException as e:
                utils.logging_or_print(f'TcUpdater: failed to get merkury-empty plc data: {e}, update canceled.',
                                       enable_logging=self.enable_logging, level=logging.ERROR)
                return
            # update ncs of merkury-empty plc
            current_ncs = current_merkury_empty_plc.spec.limited.nominal_concurrency_shares
            if ncs_merkury_empty == current_ncs:
                utils.logging_or_print(f'TcUpdater: desired concurrency ({concurrency}) = current concurrency, '
                                       f'update canceled.', enable_logging=self.enable_logging, level=logging.INFO)
            else:
                current_merkury_empty_plc.spec.limited.nominal_concurrency_shares = ncs_merkury_empty
                try:
                    response = api_instance.replace_priority_level_configuration(name=self.plc_name,
                                                                                 body=current_merkury_empty_plc)
                    utils.logging_or_print(f'TcUpdater: NCS of merkury-empty plc successfully updated to '
                                           f'{response.spec.limited.nominal_concurrency_shares}.',
                                           enable_logging=self.enable_logging, level=logging.INFO)
                except ApiException as e:
                    utils.logging_or_print(f'TcUpdater: failed to update NCS of merkury-empty plc, '
                                           f'update canceled: {e}.', enable_logging=self.enable_logging,
                                           level=logging.ERROR)
                    return
            # update queueLengthLimit of 6 default plc
            for default_plc in self.default_plc_list:
                try:
                    current_default_plc = api_instance.read_priority_level_configuration(name=default_plc)
                except ApiException as e:
                    utils.logging_or_print(f'TcUpdater: failed to get {default_plc} plc data: {e}, update canceled.',
                                           enable_logging=self.enable_logging, level=logging.ERROR)
                    return
                current_default_plc_queue_length_limit = current_default_plc.spec.limited.limit_response.queuing. \
                    queue_length_limit
                if current_default_plc_queue_length_limit == queue_length:
                    utils.logging_or_print(
                        f'TcUpdater: desired queue length ({queue_length}) = current queue length of {default_plc} plc,'
                        f' update canceled.', enable_logging=self.enable_logging, level=logging.INFO)
                else:
                    current_default_plc.metadata.annotations['apf.kubernetes.io/autoupdate-spec'] = 'false'
                    current_default_plc.spec.limited.limit_response.queuing.queue_length_limit = queue_length
                    try:
                        response = api_instance.replace_priority_level_configuration(name=default_plc,
                                                                                     body=current_default_plc)
                        utils.logging_or_print(
                            f'TcUpdater: queue length limit of {default_plc} plc successfully '
                            f'updated to {response.spec.limited.limit_response.queuing.queue_length_limit}.',
                            enable_logging=self.enable_logging, level=logging.INFO)
                    except ApiException as e:
                        utils.logging_or_print(f'TcUpdater: failed to update queue length limit of {default_plc} plc, '
                                               f'update canceled: {e}.', enable_logging=self.enable_logging,
                                               level=logging.ERROR)
                        return
        utils.logging_or_print(f'TcUpdater: FC update complete (f*={concurrency}, q*={queue_length}).',
                               enable_logging=self.enable_logging, level=logging.INFO)

    def reset_fc(self, wider_apiserver_fcp: bool = False):
        ncs_merkury_empty = int(245 * (self.total_seats - 600) / 600)
        # 245: sum ncs of 6 default plc; 600: default seats for apiserver
        if wider_apiserver_fcp:
            ncs_merkury_empty = 1  # if set to 0, will be automatically changed to 30
        with (client.ApiClient(self.config) as api_client):
            api_instance = client.FlowcontrolApiserverV1beta3Api(api_client)
            try:
                current_merkury_empty_plc = api_instance.read_priority_level_configuration(name=self.plc_name)
            except ApiException as e:
                utils.logging_or_print(f'TcUpdater: failed to get merkury-empty plc data: {e}, update canceled.',
                                       enable_logging=self.enable_logging, level=logging.ERROR)
                return
            # update ncs of merkury-empty plc
            current_ncs = current_merkury_empty_plc.spec.limited.nominal_concurrency_shares
            if ncs_merkury_empty == current_ncs:
                utils.logging_or_print('TcUpdater: current concurrency unchanged, update canceled.',
                                       enable_logging=self.enable_logging, level=logging.INFO)
            else:
                current_merkury_empty_plc.spec.limited.nominal_concurrency_shares = ncs_merkury_empty
                try:
                    response = api_instance.replace_priority_level_configuration(name=self.plc_name,
                                                                                 body=current_merkury_empty_plc)
                    utils.logging_or_print(f'TcUpdater: NCS of merkury-empty plc successfully reset to '
                                           f'{response.spec.limited.nominal_concurrency_shares}.',
                                           enable_logging=self.enable_logging, level=logging.INFO)
                except ApiException as e:
                    utils.logging_or_print(f'TcUpdater: failed to reset NCS of merkury-empty plc, reset canceled: {e}.',
                                           enable_logging=self.enable_logging, level=logging.ERROR)
                    return
            # update queueLengthLimit of 6 default plc
            for default_plc in self.default_plc_list:
                try:
                    current_default_plc = api_instance.read_priority_level_configuration(name=default_plc)
                except ApiException as e:
                    utils.logging_or_print(f'TcUpdater: failed to get {default_plc} plc data: {e}, reset canceled.',
                                           enable_logging=self.enable_logging, level=logging.ERROR)
                    return
                current_default_plc_queue_length_limit = current_default_plc.spec.limited.limit_response.queuing. \
                    queue_length_limit
                if current_default_plc_queue_length_limit == self.default_plc_ql:
                    utils.logging_or_print(
                        f'TcUpdater: current queue length of {default_plc} plc unchanged, reset canceled.',
                        enable_logging=self.enable_logging, level=logging.INFO)
                else:
                    current_default_plc.metadata.annotations['apf.kubernetes.io/autoupdate-spec'] = 'true'
                    try:
                        _ = api_instance.replace_priority_level_configuration(name=default_plc,
                                                                              body=current_default_plc)
                        utils.logging_or_print(
                            f'TcUpdater: queue length limit of {default_plc} plc successfully reset to '
                            f'{self.default_plc_ql}.', enable_logging=self.enable_logging, level=logging.INFO)
                    except ApiException as e:
                        utils.logging_or_print(
                            f'TcUpdater: failed to reset queue length limit of {default_plc} plc, '
                            f'update canceled: {e}.', enable_logging=self.enable_logging, level=logging.ERROR)
                        return
        utils.logging_or_print('TcUpdater: FC reset complete.', enable_logging=self.enable_logging, level=logging.INFO)
