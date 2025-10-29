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
    parser.add_argument('--wider_tc', action='store_true',
                        help='Enable wider tc for apiservers (3000, default: 600).')
    args = parser.parse_args()
    collocated_components = ['kube-apiserver', 'kube-controller-manager', 'kube-scheduler']
    updater_tc = tc_updater.TcUpdater()
    updater_res = res_updater.ResourceUpdater(collocated_components=collocated_components, enable_logging=False)
    updater_tc.reset_tc(wider_apiserver_tcp=args.wider_tc)
    updater_res.reset_resource_limit_of_all_components()
    updater_res.reset_resource_limit_of_all_components(restart=True, interval=args.interval)
