#!/usr/bin/env python
# -*- coding: utf-8 -*-

from socket import socket, AF_INET, SOCK_STREAM
import re
from datetime import datetime
import time
import mysql.connector
from influxdb import InfluxDBClient


class sever(object):

    def __init__(self):
        super(sever, self).__init__()
        # self.PORT = 2000
        self.PORT = 1123
        self.HOST = ''
        self.BUFSIZ = 2048
        self.ADDR = (self.HOST, self.PORT)
        self.sock = socket(AF_INET, SOCK_STREAM)
        self.config = {'host': '127.0.0.1',
                       'user': 'root',
                       'password': '123456',
                       'port': 3306,
                       'database': 'scale180822',
                       'charset': 'utf8'}
        influx_host = 'localhost'
        influx_port = 8086
        user = 'ecust'
        pwd = '123456'
        database = 'scale'
        try:
            self.cnn = mysql.connector.connect(**self.config)
            self.cursor = self.cnn.cursor()
            self.influx_client = InfluxDBClient(influx_host, influx_port, user, pwd, database)
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

            lastWeight1 = 0
            lastWeight2 = 0
            lastWeight3 = 0
            lastWeight4 = 0

            while True:
                data_str = tcpClientSock.recv(self.BUFSIZ)
                # listdata = data_str.split(',')[:3]
                listdata = re.findall(r"\d+\.?\d*", data_str)
                listread = [float(x) / 100 for x in listdata]
                points = []
                if not len(listread) // 4:
                    break
                for i in range(len(listread) // 4):
                    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                    newWeight1 = listread[4 * (i - 1)]
                    newWeight2 = listread[4 * (i - 1) + 1]
                    newWeight3 = listread[4 * (i - 1) + 2]
                    newWeight4 = listread[4 * (i - 1) + 3]

                    if ((newWeight1==lastWeight1) and (newWeight2==lastWeight2)
                        and (newWeight3==lastWeight3) and (newWeight4==lastWeight4)):
                        continue
                    else:
                        sqlString = (
                        "INSERT INTO `scale1015`(`Timestamp`, `WeightTag1`, `WeightTag2`, `WeightTag3`, `WeightTag4`) VALUES ('%s', %.2f, %.2f, %.2f, %.2f)" % (
                        now, newWeight1, newWeight2, newWeight3, newWeight4))
                        self.cursor.execute(sqlString)
                        self.cnn.commit()
                        lastWeight1, lastWeight2, lastWeight3, lastWeight4 = newWeight1, newWeight2, newWeight3, newWeight4
                        points.append(
                            {
                                "measurement": "scale1031",
                                "tags": {
                                    "eqpid": "scale01",
                                    "factory": "ecust"
                                },
                                "fields":{
                                    "weighttag1": newWeight1,
                                    "weighttag2": newWeight2,
                                    "weighttag3": newWeight3,
                                    "weighttag4": newWeight4,
                                    "weight": newWeight1 + newWeight2 + newWeight3 + newWeight4
                                }
                            }
                        )
                self.influx_client.write_points(points)
                print('success')

newSever = sever()
newSever.ReadVal()
