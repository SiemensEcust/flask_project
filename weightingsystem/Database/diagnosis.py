#!/usr/bin/env python
# -*- coding: utf-8 -*-
import mysql.connector
import datetime
from datetime import timedelta
from scaleOperation import scaleOperation
import threading
import time
from DataBaseClass import DataBaseOperation


def connection():
    config = {'host': '127.0.0.1',
              'user': 'root',
              'password': '123456',
              'port': 3306,
              'database': 'weightingsystem',
              'charset': 'utf8'}
    try:
        cnn = mysql.connector.connect(**config)
    except mysql.connector.Error as e:
        print('connect fails!{}'.format(e))
    return cnn


def createScale(cursor, cnn):
    scaleList = []
    queryString = ("select id from factory")
    cursor.execute(queryString)
    result = cursor.fetchall()
    if not result:
        return False
    for item in result:
        queryString = ("select id from %s" % (item[0] + 'eqp'))
        cursor.execute(queryString)
        resultEqp = cursor.fetchall()
        if resultEqp:
            for eqp in resultEqp:
                eqpScale = scaleOperation(item[0], eqp[0], cnn)
                scaleList.append(eqpScale)
    cursor.close()
    return scaleList


def diagnosisEqp(scale, cnn):
    while True:
        # print('diagnosisEqp Start')
        time.sleep(1)
        scale.diagnosis(cnn)


cnn = connection()
cursor = cnn.cursor()
scaleList = createScale(cursor, cnn)
for scale in scaleList:
    t = threading.Thread(target=diagnosisEqp, args=(scale, cnn,))
    t.setDaemon(True)
    t.start()
while True:
    print('main')
    stime = datetime.datetime.now().strftime('%Y-%m-%d %X')
    db = DataBaseOperation()
    if stime[-8: -3] == '13:20':
        try:
            print(db.maintainDB())
        except Exception as e:
            print('Create Table failed, error: ', e)
    time.sleep(20)
cnn.close()
