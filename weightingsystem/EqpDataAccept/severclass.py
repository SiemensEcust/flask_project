#!/usr/bin/env python
# -*- coding: utf-8 -*-
from socket import socket, AF_INET, SOCK_STREAM
import re
from datetime import datetime
import time
import mysql.connector


class sever(object):

    def __init__(self):
        super(sever, self).__init__()
        self.PORT = 2000
        self.HOST = ''
        self.BUFSIZ = 2048
        self.ADDR = (self.HOST, self.PORT)
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.config = {'host': '127.0.0.1',
                       'user': 'root',
                       'password': '123456',
                       'port': 3306,
                       'database': 'scale',
                       'charset': 'utf8'}
        try:
            self.cnn = mysql.connector.connect(**self.config)
            self.cursor = self.cnn.cursor()
        except mysql.connector.Error as e:
            print('connect fails!{}'.format(e))

    def ReadVal(self):
        self.sock.bind(self.ADDR)
        self.sock.listen(5)
        while (self.cursor):
            print('wait and binding:%d' % (self.PORT))
            tcpClientSock, addr = self.sock.accept()
            # tcpClientSock.settimeout(0.0)
            print('accepted, client address is：', addr)
            while True:
                data_str = tcpClientSock.recv(self.BUFSIZ)
                # listdata = data_str.split(',')[:3]
                listdata = re.findall(r"\d+\.?\d*", data_str)
                listread = [float(x) / 100 for x in listdata]
                if not len(listread) // 4:
                    break
                for i in range(len(listread) // 4):
                    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    sqlString = ("INSERT INTO `newdata`(`Timestamp`, `WeightTag1`, `WeightTag2`, `WeightTag3`, `WeightTag4`) VALUES ('%s', %.1f, %.1f, %.1f, %.1f)" % (now, listread[4 * (i - 1)], listread[4 * (i - 1) + 1], listread[4 * (i - 1) + 2], listread[4 * (i - 1) + 3]))
                    self.cursor.execute(sqlString)
                    self.cnn.commit()
                print('success')
