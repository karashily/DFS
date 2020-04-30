# -*- coding: utf-8 -*-
"""
Created on Sat Mar  7 13:07:34 2020

@author: somar
"""
import zmq
import os
import time
from multiprocessing import Process

IP="tcp://127.0.0.1:"
MasterIP="tcp://127.0.0.1:"

class DataKeeper:
    i_Am_Alive_port="5400"
    replicationPort="5200"
    context = zmq.Context()
    def __init__(self, ID, port):
        self.ID = ID
        self.clientport=port
        self.mastersuccessport = port[:-2] + str(int(port[-2]) + 1) + port[-1]
 #################################       
    def HeartBeat(self):
        socket = self.context.socket(zmq.PUB)
        socket.connect(MasterIP+self.i_Am_Alive_port)
        while True:
            messagedata = IP[:-1]
            socket.send_string(messagedata)
            time.sleep(.5)
 #####################################           
    def UploadFile(self,message):
        name=message['fileName']
        print(name+"/n")
        file=message['File']
        f=open(name,'wb')
        f.write(file)
        f.close()
        print("datakeeper:video %s added on machine no %d successfully ^_^ /n" %(name,self.ID))
        return True
 ######################################
    def DownloadFile(self,message,socket):
        print("d5lt el download")
        fileName=message['fileName']
        f=open(fileName,"rb")
        v=f.read()
        message['File']=v
        socket.send_pyobj(message)
        print("video downloaded 😊 /n")
        f.close()
        return True
 ############################################
    def ConnectToClient(self):
        socket = self.context.socket(zmq.PAIR)
        socket.bind(IP+self.clientport)
        mastersocket = self.context.socket(zmq.PUSH)
        mastersocket.bind(IP+self.mastersuccessport) 
        while True:
            print("my client port: ",self.clientport)
            message=socket.recv_pyobj()
            print("keeper  received  from client /n")
            Type=message['Type']
            success=False
            clientSuccessPort=message['successport']
            if(Type==1):
                    success= self.UploadFile(message)
                    if(success):
                        msg={'success':True,'successPort':clientSuccessPort}
                        mastersocket.send_pyobj(msg)
                        print("success message sent to master")
                        
            else:
                success=self.DownloadFile(message,socket)
                if(success):
                    msg={'success':True,'successPort':clientSuccessPort}
                    mastersocket.send_pyobj(msg)
                    print("success message sent to master")
###############################
    def SendReplica(self):
        master_socket = self.context.socket(zmq.PAIR)
        master_socket.bind(IP+self.replicationPort)
        while True:
            message=master_socket.recv_pyobj()
            print("keeper  received replica req  from master ")
            print(message)
            ip=message['ip']
            message['successport']=""
            message['Type']=1
            replica_socket=self.context.socket(zmq.PAIR)
            replica_socket.connect(ip)
            success=False
            success=self.DownloadFile(message,replica_socket)
            if(success):
                msg={'success':True,'successPort':""}
                master_socket.send_pyobj(msg)
                replica_socket.close()
##################################

# creating processes
d1=DataKeeper(1,"5510")
d2=DataKeeper(2,"5511")
d3=DataKeeper(3,"5512")

p1 = Process(target = d1.ConnectToClient)
p2 = Process(target = d2.ConnectToClient)
p3 = Process(target = d3.ConnectToClient)

h1 = Process(target = d1.HeartBeat)
r1 = Process(target=d2.SendReplica)

# starting processes
h1.start()
r1.start()

p1.start()
p2.start()
p3.start()

# joining processes
h1.join()
r1.join()

p1.join()
p2.join()
p3.join()
