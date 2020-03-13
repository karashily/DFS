# -*- coding: utf-8 -*-
"""
Created on Sat Mar  7 16:04:17 2020

@author: somar
"""

import zmq
import random
import sys
import time
import threading 
import os
from multiprocessing import Process,Value,Lock,Manager
 
IP="tcp://127.0.0.1:"

class Client:
    context = zmq.Context() 
    masterPort="5500"      #to 5509
    def __init__(self, ID,port):
        self.ClientID = ID
        self.clientSuccessPort=IP+"530"+str(port)
#hand shaking with master,port number returned from master

    def UploadFile(self,fileName,portUpload):
        socket = self.context.socket(zmq.PAIR)
        socket.connect(portUpload)
        print("client connection to keeper done /n")
        f=open(fileName,"rb")
        v=f.read()
        uploadedVideo={'File':v,'FileName':fileName,'Type':1,'successport':self.clientSuccessPort}  #type: 1= upload   0= download
        socket.send_pyobj(uploadedVideo)
        print("client:video uploaded ^_^ /n")
        f.close()
        mastersocket=self.context.socket(zmq.PAIR)
        mastersocket.bind(self.clientSuccessPort)
        print("client waiting success message from  master /n")
        success=mastersocket.recv_pyobj()
        if(success==True):
            socket.close()
            mastersocket.close()
            print("client no. %s left successfully" %self.ClientID)
 ########################################################       
    def DownloadFile(self,fileName,dataKeeperPort):
        socket = self.context.socket(zmq.PAIR)
        socket.connect(dataKeeperPort)
        toBeDownloaded={'FileName':fileName,'Type':0,'successport':self.clientSuccessPort}
        socket.send_pyobj(toBeDownloaded)
        print("request sent... /n")
        
        downloadedVideo=socket.recv_pyobj()
        name=downloadedVideo['FileName']
        print(name+"/n")
        file=downloadedVideo['File']
        # Create target Directory if don't exist
        myfolder="client no. %s folder" %self.ClientID
        if not os.path.exists(myfolder):
            os.makedirs(myfolder)
        f = open(myfolder+"/"+fileName, "wb")
        f.write(file)
        f.close()
        print("video %s added from client no %s successfully ^_^ /n" %(name,self.ClientID))
        mastersocket=self.context.socket(zmq.PAIR)
        mastersocket.bind(self.clientSuccessPort)
        print("client waiting success message from  master /n")
        success=mastersocket.recv_pyobj()
        if(success==True):
            socket.close()
            mastersocket.close()
            print("client no. %s left successfully" %self.ClientID)
#####################################################
    def connectToMaster(self,operation,Filename):
        #connect to master
        socket = self.context.socket(zmq.REQ)
        socket.connect(IP+self.masterPort)
        message={'clientID':self.ClientID,'Type':operation,'FileName':Filename}
        socket.send_pyobj(message) #send message to master
        print("client message sent to master /n")
        dataport=socket.recv_string()#wait for port
        while(dataport == 'fatal'):
            print("Master responded with fatal error......")
            socket.send_pyobj(message) #send message to master
            print("client message sent to master /n")
            dataport=socket.recv_string()#wait for port
            
        print("master responded  to client with port {}/n".format(dataport))
        if(operation==1):#upload
            socket.close()
            self.UploadFile(Filename,dataport)
        else:#download
            socket.close()
            self.DownloadFile(Filename,dataport)
        socket.close()
            
 ######################           
#c1=Client(random.randint(0,9))

clientsNum = int(input("Number of clients: "))
clients=[]
while(clientsNum>10):
    print("maximum allowable no. of clients is 10..try again")
    clientsNum = int(input())
for i in range(clientsNum):
    id  = input("please enter client %d id: " %i )
    rightOperation = True
    while(rightOperation):
        operation = input("please enter the operation (download/upload): ")
        if(operation == 'download'):
            operation = 0
            rightOperation = False
        elif(operation == 'upload'):
            operation = 1
            rightOperation = False
        else:
            print("Wrong Operation...!")

    fileName = input("please enter the filename: ")

    c = Client(id,i)
    p = Process(target=c.connectToMaster, args=(operation, fileName))
    clients.append(p)
        
    
for i in clients:
    i.start()

for i in clients:
    i.join()
# c1.connectToMaster(1,"1.mp4")
# c1.connectToMaster(0,"1.mp4")
