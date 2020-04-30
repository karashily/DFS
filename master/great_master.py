from multiprocessing import Process,Value,Lock,Manager,RLock
import zmq
import datetime
import time
import json
import os
import copy

from util import *
from process import *
from replicas import *
from alive import *
from ports import *

datakeepers_ports_ips = []

for j in range(len(datakeepers_ips)):
    for i in range(ports_per_datakeeper[j]):
        datakeepers_ports_ips.append(datakeepers_ips[j]+"551"+str(i))


def initialize_ports_table(ports_table):
    #log
    init = open("logs/init.txt", "w+")
    init.write("Master ["+str(int(time.time()))+"] "+"datakeepers ports initialized: \n")
    init.close()

    for i in range(len(datakeepers_ports_ips)):
        d = {
            'ip': datakeepers_ports_ips[i],
            'free': True,
            'alive': True,
            'last_time_alive': datetime.datetime.now() - datetime.timedelta(seconds=5)
        }

        #log
        init = open("logs/init.txt", "a")
        init.write("Master ["+str(int(time.time()))+"] "+str(d)+'\n')
        init.close()

        ports_table.append(d)

def initialize_files_table(manager, files_table, unique_files):
    file_exists = os.path.isfile('./files.json') 
    ff = None
    if(file_exists):
        if(os.stat('files.json').st_size != 0):
            with open('files.json', 'rb') as fin:
                ff = json.load(fin)
                # f = manager.list(files_table)
                # print(f[0])
            for fil in ff:
                files_table.append(fil)
    else:
        f = open("files.json", "w+")

    if not ff == None:
        for i in range(len(ff)):
            m = {
                    "file_name":ff[i]["file_name"],
                    "user_id":ff[i]["user_id"]
                }
            unique_files.append(m)
    

def main():
    with Manager() as manager:
        
        # create logs files
        init = open("logs/init.txt", "w+")
        datakeepers_death = open("logs/datakeepers_death.txt", "w+")


        num_of_replicates = int(input("minimum number of replicates: "))
        
        # creating shared variables and their locks
        files_table_lock = RLock()
        ports_table_lock = RLock()
        unique_files_lock = RLock()

        files_table = manager.list()  
        ports_table = manager.list()
        unique_files = manager.list()

        # initialize ports table
        initialize_ports_table(ports_table)

        # initialize files table
        initialize_files_table(manager, files_table, unique_files)
        print(len(files_table))
        
        # create processes
        p1 = Process(target=process, args=(files_table, files_table_lock, unique_files, unique_files_lock, ports_table, ports_table_lock, master_ports[0]))
        p2 = Process(target=process, args=(files_table, files_table_lock, unique_files, unique_files_lock, ports_table, ports_table_lock, master_ports[1]))
        p3 = Process(target=process, args=(files_table, files_table_lock, unique_files, unique_files_lock, ports_table, ports_table_lock, master_ports[2]))
        
        alive_process = Process(target=alive, args=(files_table, ports_table, files_table_lock, ports_table_lock))
        dead_process = Process(target=undertaker, args=(files_table, ports_table, files_table_lock, ports_table_lock))

        replicate_process = Process(target=replicate, args=(files_table, files_table_lock, unique_files, unique_files_lock, ports_table, ports_table_lock, num_of_replicates))

        # start processes
        p1.start()
        p2.start()
        p3.start()

        alive_process.start()
        dead_process.start()
        replicate_process.start()

        # join processes
        p1.join()
        p2.join()
        p3.join()

        alive_process.join()
        dead_process.join()
        replicate_process.join()



main()

