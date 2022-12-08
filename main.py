import os
import numpy as np

workers = 5
reducers = 0

path = os.getcwd()
sequence = np.load(os.path.join(path, 'sequence.npy'))
splits = np.split(sequence,workers)

# scp /path/to/file username@a:/path/to/destination
print(splits)






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