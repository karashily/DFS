from ports import *

import zmq
import datetime
import time
import json
import os
import copy


def get_free_port(ports_table, ports_table_lock, node):
    ports_table_lock.acquire()
    for i in range(len(ports_table)):
        if((ports_table[i]["free"]==True) and (ports_table[i]["alive"]==True) and ((ports_table[i]['ip'][:-4] == node) or node == 'any')):
            port = copy.deepcopy(ports_table[i]['ip'])
            acquire_port(ports_table, ports_table_lock, ports_table[i]["ip"])
            ports_table_lock.release()
            return port
    
    ports_table_lock.release()
    return None


def get_file_loc(files_table, files_table_lock, filename):
    files_table_lock.acquire()
    for i in range(len(files_table)):
        if(files_table[i]['file_name'] == filename and files_table[i]['is_data_node_alive'] == True):
            files_table_lock.release()
            return files_table[i]['data_node_number']
    files_table_lock.release()

def get_datakeepers_of_file(files_table,file_name):
    datakeepers = set()
    for i in range(len(files_table)):
        if ((files_table[i]['file_name'] == file_name) and (files_table[i]['is_data_node_alive'] == True)):
            datakeepers.add(files_table[i]['data_node_number']+":")
    datakeepers = list(datakeepers)
    return datakeepers

def add_to_files_table(files_table,lock,user_id,file_name,data_node_number,is_data_node_alive):
    lock.acquire()
    d = {
        "user_id" : user_id,
        "file_name" : file_name,
        "data_node_number" : data_node_number,
        "is_data_node_alive" : is_data_node_alive
    }
    files_table.append(d)

    with open('files.json', 'w') as fout:
        json.dump(copy.deepcopy(files_table), fout)

    lock.release()


def acquire_port(ports_table, ports_table_lock, port):
    # Update ports table
    for i in range(len(ports_table)):
        if(ports_table[i]['ip'] == port):
            d = {
                'ip': ports_table[i]['ip'],
                'free': False,
                'alive': ports_table[i]['alive'],
                'last_time_alive': ports_table[i]['last_time_alive']
            }
            ports_table.remove(ports_table[i])
            ports_table.append(d)

            
            #log
            ports_log = open("logs/ports_log.txt", "a")
            ports_log.write("Master ["+str(int(time.time()))+"] "+"port# "+port+" is requested"+"\n")
            ports_log.close()

            break



def release_port(ports_table, ports_table_lock, port):
    # Update ports table
    ports_table_lock.acquire()
    for i in range(len(ports_table)):
        if(ports_table[i]['ip'] == port):
            d = {
                'ip': ports_table[i]['ip'],
                'free': True,
                'alive': ports_table[i]['alive'],
                'last_time_alive': ports_table[i]['last_time_alive']
            }
            ports_table.remove(ports_table[i])
            ports_table.append(d)
            
            #log
            ports_log = open("logs/ports_log.txt", "a")
            ports_log.write("Master ["+str(int(time.time()))+"] "+"port# "+port+" is released"+"\n")
            ports_log.close()

            break

    ports_table_lock.release()
