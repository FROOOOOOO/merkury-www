import os
import argparse
from multiprocessing import Pool
from kubernetes import client, config
from kubernetes.client import ApiException

TEST_KUBECONFIG = f'{os.path.abspath(os.path.dirname(os.path.dirname(__file__)))}/config/test-kubeconfig.yaml'


def delete_node(node):
    core_v1 = client.CoreV1Api()
    try:
        core_v1.delete_node(name=node.metadata.name)
        print(f"Deleting node: {node.metadata.name}")
    except ApiException as e:
        print(f"Failed to delete node: {node.metadata.name}, error: {e}")


def delete_kwok_node(pool_size: int = 10):
    config.load_kube_config(config_file=TEST_KUBECONFIG)
    core_v1 = client.CoreV1Api()
    nodes = core_v1.list_node().items
    kwok_nodes = [node for node in nodes if 'kwok' in node.metadata.name]
    with Pool(pool_size) as pool:
        pool.map(delete_node, kwok_nodes)
    print("KWOK nodes deletion complete.")


def delete_stress_test_resource(res_namespaced):
    res, namespaced = res_namespaced
    if namespaced:
        os.system('kubectl --kubeconfig=%s -n myx-test delete %s -l env=test' %
                  (TEST_KUBECONFIG, res))
    else:
        os.system('kubectl --kubeconfig=%s delete %s -l env=test' % (TEST_KUBECONFIG, res))


def delete_stress_test_resources(pool_size: int = 10):
    res_list = [('pv', False), ('cm', True), ('ep', True), ('limits', True), ('pvc', True),
                ('podtemplate', True), ('rc', True), ('quota', True), ('secret', True), ('sa', True),
                ('svc', True), ('sts', True), ('deploy', True), ('rs', True), ('cj', True), ('ns', False)]
    with Pool(pool_size) as pool:
        pool.map(delete_stress_test_resource, res_list)
    print("Stress test resources deletion complete.")


def delete_ns(namespace):
    ns = namespace.metadata.name
    core_v1 = client.CoreV1Api()
    apps_v1 = client.AppsV1Api()
    try:
        deploys = apps_v1.list_namespaced_deployment(namespace=ns).items
        deploy_to_del = [deploy.metadata.name for deploy in deploys]
        for deploy in deploy_to_del:
            apps_v1.delete_namespaced_deployment(namespace=ns, name=deploy)
            print(f"Deleting deployment in {ns}: {deploy}")
        core_v1.delete_namespace(name=ns)
        print(f"Deleting namespace: {ns}")
    except ApiException as e:
        print(f"Failed to delete namespace: {ns}, error: {e}")


def delete_cl2_ns(pool_size: int = 10):
    config.load_kube_config(config_file=TEST_KUBECONFIG)
    core_v1 = client.CoreV1Api()
    namespaces = core_v1.list_namespace().items
    test_namespaces = [namespace for namespace in namespaces if 'test-' in namespace.metadata.name]
    with Pool(pool_size) as pool:
        pool.map(delete_ns, test_namespaces)
    print("Clusterloader2 resources deletion complete.")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Script to clean all test resources in the test cluster.')
    parser.add_argument('--pool_size', '-p', type=int, default=10, help='multiprocess pool size (default: 10)')
    args = parser.parse_args()
    num_cpu_threads = os.cpu_count()
    pool_size = args.pool_size
    if pool_size <= 0:
        pool_size = 1
    if num_cpu_threads is not None and pool_size > num_cpu_threads:
        pool_size = num_cpu_threads
    # 1. delete kwok nodes & stress test res
    delete_kwok_node(pool_size=pool_size)
    delete_stress_test_resources(pool_size=pool_size)
    # 2. delete cl2 ns
    delete_cl2_ns(pool_size=pool_size)
