import sys
import subprocess
from transactionManager import TransactionManager

# pip install coverage
# coverage run -m testall 24
# coverage report -m

if len(sys.argv) == 1:
    print("Enter no of files to test")
    exit()

input = "./tests/test"

for i in range(1, int(sys.argv[1]) + 1):
    # process = subprocess.call(['python3','main.py', input + str(i)], stdout=open(f'outputs/output{i}','w'))
    # process = subprocess.call(['coverage', 'run', '-m','main', input + str(i)], stdout=open(f'outputs/output{i}','w'))
    # process2 = subprocess.call(['coverage', 'report', '-m'], stdout=open(f'outputs/output{i}','a'))

    transManager = TransactionManager(10, 20, input + str(i))
    transManager.run()