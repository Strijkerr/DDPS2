import socket
import sys
import pickle
import threading
from _thread import *
import time

def returnDict (filename) :
    infile = open(filename,'rb')
    dictionary = pickle.load(infile)
    infile.close()
    return dictionary

def checkMapTaskComplete () :
    for task in map_task_dict.keys() :
        if not (map_task_dict[task]['status'] == 'Done') :
            return False
    return True

def findFreeMapTask () :
    for task in map_task_dict.keys() :
        if (map_task_dict[task]['status'] == None) :
            worker, location = findWorker(map_task_dict[task])

            # Skip to next task if no worker available atm.
            if not worker :
                continue
            map_task_dict[task]['status'] = 'in-progress'
            map_task_dict[task]['worker'] = worker
            worker_dict[worker] = 'busy'
            return map_task_dict[task], worker, location
    return False, False, False

def findWorker (task) :
    worker_location = []

    # Only get workers where file is stored.
    for copy in shard_dict[task].keys() :
        worker_location.append([shard_dict[task][copy]['host'], shard_dict[task][copy]['location']])

    # Try to get worker
    for worker in worker_location :
        if (worker_dict[worker[0]]== None) :
            return worker
    # If no idle worker found
    return False,False

def on_new_client(conn):
    count = 0

    # Receive client identity
    try : 
        msg = conn.recv(1024).decode()
        print("Worker connected:",msg)
    except Exception as e:
        print(f"[!] Error: {e}")

    # Main while loop
    while True and count < 10:
        # try:
        #     msg = conn.recv(1024).decode()
        #     print("Worker connected:",msg)
        # except Exception as e:
        #     print("Exception reached")
        #     print(f"[!] Error: {e}")
        #     conn.remove(conn)
        # else:
        #     print("Else reached")
        #     conn.send(str(checkMapTaskComplete()).encode())
        # print("End of while loop reached")
        time.sleep(1) # Slight delay, delete later
        count+=1
    conn.close()

def server_program(client_count):
    host = socket.gethostname()
    port = 56609
    server_socket = socket.socket()
    server_socket.bind((host, port)) 
    server_socket.listen(client_count)
    threads = []
    while True and len(threads) != client_count:
        conn, address = server_socket.accept()
        t = threading.Thread(target=on_new_client, args=(conn,))
        #t.daemon = True
        t.start()
        threads.append(t)
    # At this point the threads for every client have been created.

    # Wait for threads to finish (~10s)
    for t in threads :
        t.join()

    print("Program exit")

shard_dict = returnDict(sys.argv[1])
map_task_dict = returnDict(sys.argv[2])
reduce_task_dict = returnDict(sys.argv[3])
worker_dict = returnDict(sys.argv[4])

server_program(len(worker_dict))