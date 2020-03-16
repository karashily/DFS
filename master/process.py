from util import *
from ports import *

import zmq
import datetime
import time
import json
import os
import copy

def upload(files_table, files_table_lock, unique_files, unique_files_lock, ports_table, ports_table_lock, msg, socket):
    # checking that file isn't uploaded already
    files_table_lock.acquire()
    for i in files_table:
        if(msg["fileName"] == i['file_name']):
            socket.send_string("filename_exists_already")
            files_table_lock.release()
            return
    files_table_lock.release()

    # find free port
    port = get_free_port(ports_table, ports_table_lock, 'any')    
    
    if(port is None):
        socket.send_string("no_free_ports")
        return
    

    # send port to client
    socket.send_string(port)


    #log
    upload_log = open("logs/upload_log.txt", "a")
    upload_log.write("Master ["+str(int(time.time()))+"] "+"file: "+msg["fileName"]+ " will be uploaded to "+port+"\n")
    upload_log.close()

    # wait for success from datakeeper
    success_port = port[:-2] + str(int(port[-2]) + 1) + port[-1]
    context = zmq.Context()
    results_receiver = context.socket(zmq.PULL)
    results_receiver.connect(success_port)
    success = results_receiver.recv_pyobj()

    #log
    upload_log = open("logs/upload_log.txt", "a")
    upload_log.write("Master ["+str(int(time.time()))+"] "+"got success from datakeeper: "+ port+"\n")
    upload_log.close()

    # add file to table
    add_to_files_table(files_table, files_table_lock, msg["clientID"], msg["fileName"], port[:-5], True)
    m = {
        "file_name":msg["fileName"],
        "user_id":msg["clientID"]
    }
    unique_files_lock.acquire()
    unique_files.append(m)
    unique_files_lock.release()


    # send done to client
    success_port_of_client = success['successPort']
    success_context = zmq.Context()
    success_socket = success_context.socket(zmq.PAIR)
    success_socket.connect(success_port_of_client)
    success_socket.send_pyobj(True)

    #log
    upload_log = open("logs/upload_log.txt", "a")
    upload_log.write("Master ["+str(int(time.time()))+"] "+"sent success message to client: "+ success_port_of_client+"\n")
    upload_log.close()

    release_port(ports_table, ports_table_lock, port)


def download(files_table, files_table_lock, ports_table, ports_table_lock, msg, socket):
    # find file on which datanode
    loc = get_file_loc(files_table, files_table_lock, msg['fileName'])

    if(loc is None):
        socket.send_string("file_not_found")
        return
    
    loc += ":"
    # get a free port of that machine
    port = get_free_port(ports_table, ports_table_lock, loc)
    
    if(port is None):
        socket.send_string("no_free_ports")
        return
    
    # send not busy port to client
    socket.send_string(port)

    #log
    download_log = open("logs/download_log.txt", "a")
    download_log.write("Master ["+str(int(time.time()))+"] "+"file: "+msg["fileName"]+ " will be downloaded from "+port+"\n")
    download_log.close()

    # wait for success from datakeeper
    success_port = port[:-2] + str(int(port[-2]) + 1) + port[-1]
    context = zmq.Context()
    results_receiver = context.socket(zmq.PULL)
    results_receiver.connect(success_port)
    success = results_receiver.recv_pyobj()

    #log
    download_log = open("logs/download_log.txt", "a")
    download_log.write("Master ["+str(int(time.time()))+"] "+"got success from datakeeper: "+ port+"\n")
    download_log.close()

    # send done to client
    success_port_of_client = success['successPort']
    success_context = zmq.Context()
    success_socket = success_context.socket(zmq.PAIR)
    success_socket.connect(success_port_of_client)
    success_socket.send_pyobj(True)

     #log
    download_log = open("logs/download_log.txt", "a")
    download_log.write("Master ["+str(int(time.time()))+"] "+"sent success message to client: "+ success_port_of_client+"\n")
    download_log.close()

    release_port(ports_table, ports_table_lock, port)


def process(files_table, files_table_lock, unique_files, unique_files_lock, ports_table, ports_table_lock, master_process_port):
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(master_process_port)

    while True:
        msg = socket.recv_pyobj()

        if msg['Type']==1:
            upload(files_table, files_table_lock, unique_files, unique_files_lock, ports_table, ports_table_lock, msg, socket)
        elif msg['Type']==0:
            download(files_table, files_table_lock, ports_table, ports_table_lock, msg, socket)
