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
connectionPort="tcp://127.0.0.1:"

class Client:
    context = zmq.Context() 
    masterPort="5500"      #to 5509
    def __init__(self, ID):
        self.ClientID = ID
#hand shaking with master,port number returned from master

    def UploadFile(self,fileName,portUpload,mastersocket):
        socket = self.context.socket(zmq.PAIR)
        socket.connect(connectionPort+portUpload)
        print("client connection to keeper done /n")
        f=open(fileName,"rb")
        v=f.read()
        uploadedVideo={'File':v,'FileName':fileName,'Type':1}  #type: 1= upload   0= download
        socket.send_pyobj(uploadedVideo)
        print("client:video uploaded ^_^ /n")
        f.close()
        mastersocket.send_string("done")
        print("client done message sent to master /n")
        success=mastersocket.recv_pyobj()
        if(success==True):
            socket.close()
 ########################################################       
    def DownloadFile(self,fileName,dataKeepertPort,mastersocket):
        socket = self.context.socket(zmq.PAIR)
        socket.connect(connectionPort+dataKeepertPort)
        toBeDownloaded={'FileName':fileName,'Type':0}
        socket.send_pyobj(toBeDownloaded)
        print("request sent... /n")
        
        downloadedVideo=socket.recv_pyobj()
        name=downloadedVideo['FileName']
        print(name+"/n")
        file=downloadedVideo['File']
        f = open("downloaded.mp4", "wb")
        f.write(file)
        f.close()
        print("video %s added on machine no %d successfully ^_^ /n" %(name,self.ClientID))
        mastersocket.send_string("done")
        success=mastersocket.recv_pyobj()
        if(success==True):
            socket.close()
            
#####################################################
    def connectToMaster(self,operation,Filename):
        #connect to master
        socket = self.context.socket(zmq.REQ)
        socket.connect(connectionPort+self.masterPort)
        
        message={'clientID':self.ClientID,'Type':operation,'FileName':Filename}
        socket.send_pyobj(message) #send message to master
        print("client message sent to master /n")
        dataport=socket.recv_string()#wait for port
        print("master responded  to client with port/n")
        if(operation==1):#upload
           
            self.UploadFile(Filename,dataport,socket)
        else:#download
            
            self.DownloadFile(Filename,dataport,socket)
        socket.close()
            
 ######################           
c1=Client(3)
c1.connectToMaster(1,"1.mp4")
