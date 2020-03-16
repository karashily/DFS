from util import *
from ports import *

import zmq
import datetime
import time
import json
import os
import copy


def replicate_file(files_table, files_table_lock,ports_table,ports_table_lock, file, num_of_replicates):
    datakeepers = get_datakeepers_of_file(files_table,file['file_name'])
    if(len(datakeepers) >= num_of_replicates):
        return

    num_of_replicates = num_of_replicates - len(datakeepers)

    list_dks_without_files = list(set(datakeepers_ips)-set(datakeepers))
    i = 0
    while ((num_of_replicates > 0) and (i < len(list_dks_without_files)) and (len(datakeepers)>0)):
        port_dk_to_rep_file_on = get_free_port(ports_table, ports_table_lock, list_dks_without_files[i])
        
        i += 1
        if (port_dk_to_rep_file_on == None):
            continue

        #create msg
        msg = {
                "fileName":file["file_name"],
                "ip":port_dk_to_rep_file_on
            }
        

        #log
        replicate_log = open("logs/replicate_log.txt", "a")
        replicate_log.write("Master ["+str(int(time.time()))+"] "+"file: "+msg["fileName"]+ " will be replicated to "+msg["ip"]+" from "+datakeepers[0] +"\n")
        replicate_log.close()

        
        #send msg
        port = "5200"
        context = zmq.Context()
        socket = context.socket(zmq.PAIR)
        socket.connect(datakeepers[0] + port)
        socket.send_pyobj(msg)

        # wait for success from datakeeper
        success_port = port_dk_to_rep_file_on[:-2] + str(int(port_dk_to_rep_file_on[-2]) + 1) + port_dk_to_rep_file_on[-1]
        context = zmq.Context()
        results_receiver = context.socket(zmq.PULL)
        results_receiver.connect(success_port)
        success = results_receiver.recv_pyobj()

        # add file to table
        add_to_files_table(files_table, files_table_lock, file["user_id"], file["file_name"], port_dk_to_rep_file_on[:-5], True)


        #set port_dk_to_rep_file_on free
        release_port(ports_table, ports_table_lock, port_dk_to_rep_file_on)

        num_of_replicates -= 1


def replicate(files_table, files_table_lock, unique_files, unique_files_lock, ports_table, ports_table_lock, num_of_replicates):
    while True:
        for i in range(len(unique_files)):
            unique_files_lock.acquire()
            replicate_file(files_table, files_table_lock, ports_table, ports_table_lock, unique_files[i], num_of_replicates)
            unique_files_lock.release()
        time.sleep(5)
