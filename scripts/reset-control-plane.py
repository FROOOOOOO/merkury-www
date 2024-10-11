import argparse
import os
import sys

python_path = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
sys.path.insert(0, python_path)
from core.updater import tc_updater, res_updater


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Script to reset control plane.')
    parser.add_argument('-i', '--interval', type=int, default=15,
                        help='Interval in seconds between restarts (default: 15s).')
    parser.add_argument('--wider_fc', action='store_true',
                        help='Enable wider fc for apiservers (3000, default: 600).')
    args = parser.parse_args()
    collocated_components = ['kube-apiserver', 'kube-controller-manager', 'kube-scheduler']
    updater_fc = tc_updater.TcUpdater()
    updater_res = res_updater.ResourceUpdater(collocated_components=collocated_components, enable_logging=False)
    updater_fc.reset_fc(wider_apiserver_fcp=args.wider_fc)
    updater_res.reset_resource_limit_of_all_components()
    updater_res.reset_resource_limit_of_all_components(restart=True, interval=args.interval)
