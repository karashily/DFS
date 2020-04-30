from util import *
from ports import *

import zmq
import datetime
import time
import json
import os
import copy


def alive(files_table, ports_table, files_table_lock, ports_table_lock):
    context = zmq.Context()
    results_receiver = context.socket(zmq.SUB)
    results_receiver.bind(master_own_ip + master_alive_port)
    results_receiver.setsockopt_string(zmq.SUBSCRIBE, "")

    while True:
        d = results_receiver.recv_string()
        
        # Update ports table
        ports_table_lock.acquire()
        for i in range(len(ports_table)):
            if (ports_table[i]['ip'][:-5] == d):
                d1 = {
                    'ip': ports_table[i]['ip'],
                    'free': ports_table[i]['free'],
                    'alive': True,
                    'last_time_alive': datetime.datetime.now()
                }
                ports_table.append(d1)
                ports_table.remove(ports_table[i])
        ports_table_lock.release()

        # Update Files table
        files_table_lock.acquire()
        for i in range(len(files_table)):
            if ((files_table[i]['data_node_number'] == d)and (files_table[i]['is_data_node_alive'] == False)):
                d2 = {
                    "user_id" : files_table[i]['user_id'],
                    "file_name" : files_table[i]['file_name'],
                    "data_node_number" : files_table[i]['data_node_number'],
                    "is_data_node_alive" : True
                }
                files_table.append(d2)
                files_table.remove(files_table[i])
                with open('files.json', 'w') as fout:
                    json.dump(copy.deepcopy(files_table), fout)
        files_table_lock.release()



def undertaker(files_table, ports_table, files_table_lock, ports_table_lock):
    while True:
        recently_dead_datakeepers = []
        
        # Update ports table
        ports_table_lock.acquire()
        for i in range(len(ports_table)):
            if (((datetime.datetime.now()-ports_table[i]['last_time_alive']).total_seconds() > 5) and ports_table[i]['alive'] == True):
                
                recently_dead_datakeepers.append(ports_table[i]['ip'])
                d = {
                    'ip': ports_table[i]['ip'],
                    'free': ports_table[i]['free'],
                    'alive': False,
                    'last_time_alive': ports_table[i]['last_time_alive']
                }
                ports_table.append(d)
                ports_table.remove(ports_table[i])


                #log
                datakeepers_death = open("logs/datakeepers_death.txt", "a")
                datakeepers_death.write("Master ["+str(int(time.time()))+"] "+"data keeper "+ports_table[i]['ip']+" has died \n")
                datakeepers_death.close()
                
        ports_table_lock.release()

        # Update files Table
        files_table_lock.acquire()
        for i in range(len(recently_dead_datakeepers)):
            for j in range(len(files_table)):
                if(files_table[j]['data_node_number'] == recently_dead_datakeepers[i][:-5]):
                    d = {
                        "user_id" : files_table[j]['user_id'],
                        "file_name" : files_table[j]['file_name'],
                        "data_node_number" : files_table[j]['data_node_number'],
                        "is_data_node_alive" : False
                    }
                    files_table.append(d)
                    files_table.remove(files_table[j])
                    with open('files.json', 'w') as fout:
                        json.dump(copy.deepcopy(files_table), fout)
        files_table_lock.release()
        time.sleep(0.001)
