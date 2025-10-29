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

import subprocess
import argparse
import os
from multiprocessing import Pool

python_path = os.path.abspath(os.path.dirname(__file__))


def run_script_and_write(script: str, args: list, file: str):
    result = subprocess.check_output(['python', script] + args)
    with open(file, 'w') as f:
        f.write(result.decode())


def run_script(script: str, args: list):
    subprocess.run(['python', script] + args)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Simulate queue models in a multi-process manner.')
    parser.add_argument('-p', '--process', required=True, type=int, default=10, help='number of processes (default: 10)')
    args = parser.parse_args()
    pool = Pool(args.process)
    script = f'{python_path}/qm-sim.py'
    args_list = []
    loads = [1000 * n for n in range(1, 21)]
    for load in loads:
        args = ['-m', '0', '-l', str(load)]
        args_list.append(args)
    for i in range(len(args_list)):
        pool.apply_async(run_script, args=(script, args_list[i]))
    pool.close()
    pool.join()
