import os
import numpy as np
import argparse
import socket
import re

nodelist = ['node102']

def check_ssh(server_ip, port=22):
    try:
        test_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        test_socket.connect((server_ip, port))
    except Exception:
        return False
    else:
        test_socket.close()
    return True

def checkInput (string) :
    nodes = args.workers.strip().split(',')
    for n in nodes :
        if not check_ssh(n) :
            raise ValueError(f"Can't connect to: {n}.")
    return nodes
    
def splitInput (filename, workers) :
    path = os.getcwd()
    worker_count = len(workers)
    if worker_count == 0 :
        raise ValueError('Not enough workers.')
    sequence = np.load(os.path.join(path, filename))
    return np.split(sequence,worker_count)

# Parsing command line arguments
argparser = argparse.ArgumentParser()
argparser.add_argument("--workers", help="E.g., node102,node103,node104 ")
argparser.add_argument("--input_file", default= 'sequence.npy', help="E.g., node102,node103,node104 ")
args = argparser.parse_args()

if args.workers is None:
    raise ValueError('No workers specified.')

nodes = checkInput(args.workers)
master = nodes[0]
workers = nodes[1:]

splits = splitInput (args.input_file, workers)
print(splits)
print(f"Can connect to {master}: {check_ssh(master)}")
# scp /path/to/file username@a:/path/to/destination


pid = os.fork()

# pid greater than 0 represents
# the parent process 
if pid > 0 :
    print("I am parent process:")
    print("Process ID:", os.getpid())
    print("Child's process ID:", pid)
  
# pid equal to 0 represents
# the created child process
else :
    print("\nI am child process:")
    print("Process ID:", os.getpid())
    print("Parent's process ID:", os.getppid())