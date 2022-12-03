import sys
import subprocess

if len(sys.argv) == 1:
    print("Enter no of files to test")
    exit()

input = "./tests/test"
output = "./output/test"
for i in range(1, int(sys.argv[1]) + 1):
    process = subprocess.call(['python3','main.py', input + str(i)], stdout=open(f'outputs/output{i}','w'))