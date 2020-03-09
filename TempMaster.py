# -*- coding: utf-8 -*-
"""
Created on Sat Mar  7 13:59:04 2020

@author: somar
"""
import sys
import zmq

port = "5555"


# Socket to talk to server
context = zmq.Context()
socket = context.socket(zmq.SUB)
socket.connect ("tcp://127.0.0.1:%s" % port)
socket.subscribe("")
while True:
    string = socket.recv()
    string=(str)(string)
    temp1 = string.split()
    
    print ((str)(temp1[2])+' /n')

        

