#!/usr/bin/env python
# -*- coding: utf-8 -*-
from socket import socket, AF_INET, SOCK_STREAM
import re
from datetime import datetime
import time
import mysql.connector
import threading


class sever(object):

    def __init__(self):
        super(sever, self).__init__()
        self.PORT = 2000
        self.HOST = ''
        self.BUFSIZ = 40960 * 20
        self.ADDR = (self.HOST, self.PORT)
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.config = {'host': '127.0.0.1',
                       'user': 'root',
                       'password': '123456',
                       'port': 3306,
                       'database': 'weightingsystem',
                       'charset': 'utf8'}
        try:
            self.cnn = mysql.connector.connect(**self.config)
        except mysql.connector.Error as e:
            print('connect fails!{}'.format(e))

    def deal(self, tcpClientSock, addr):
        print('accepted, client address is：', addr)
        while True:
            cursor = self.cnn.cursor()
            tcpClientSock.settimeout(0.0)
            try:
                time.sleep(3)
                data_str = ""
                # while True:
                #     data = tcpClientSock.recv(self.BUFSIZ)
                #     print(data)
                #     if data:
                #         data_str += data
                #     else:
                #         print("data over")
                #         break
                data_str += tcpClientSock.recv(self.BUFSIZ)
                print("Receive Over")
            except:
                print("Receive fail")
                tcpClientSock.close()
                break
            # listdata = data_str.split(',')[:3]
            listdata = re.findall(r"\-?\d+\.?\d*", data_str)
            listread = [float(x) / 100 for x in listdata]
            sqlString = "INSERT INTO `factory01scale001newval%s`(`Timestamp`, `WeightTag1`, `WeightTag2`, `WeightTag3`, `WeightTag4`) VALUES" % (datetime.now().strftime("%Y-%m-%d %H:%M:%S")[:10])
            if not len(listread) // 4:
                tcpClientSock.close()
                break
            for i in range(len(listread) // 4):
                now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                sqlString += "('" + now + "'," + str(listread[4 * (i - 1)]) + "," + str(listread[4 * (i - 1) + 1]) + "," + str(listread[4 * (i - 1) + 2]) + "," + str(listread[4 * (i - 1) + 3]) + "),"
            try:
                cursor.execute(sqlString[:len(sqlString) - 1])
                self.cnn.commit()
            except Exception as e:
                print("insert failed!")
                tcpClientSock.close()
                break
            print(listread)
            cursor.close()

    def ReadVal(self):
        self.sock.bind(self.ADDR)
        self.sock.listen(5)
        while (self.cnn):
            print('wait and binding:%d' % (self.PORT))
            tcpClientSock, addr = self.sock.accept()
            t = threading.Thread(target=self.deal, args=(tcpClientSock, addr))
            t.setDaemon(True)
            t.start()
            print("t over")
        self.sock.close()
