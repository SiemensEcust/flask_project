#!/usr/bin/env python
# -*- coding: utf-8 -*-
import mysql.connector
from datetime import datetime
import time


def connection(host, password):
    config = {'host': host,
              'user': 'root',
              'password': password,
              'port': 3306,
              'database': 'scale',
              'charset': 'utf8'}
    try:
        cnn = mysql.connector.connect(**config)
        return cnn
    except mysql.connector.Error as e:
        print('connect fails!{}'.format(e))


def querySetPoint(local_cnn, local_cursor):
    dic = {}
    query_string = "select * from setpoint order by ID desc limit 1"
    local_cursor.execute(query_string)
    query_data = local_cursor.fetchall()
    if query_data:
        dic['state'] = 1
        dic['sencer'] = query_data[0][2]
        dic['noload'] = query_data[0][5]
        dic['eptload'] = query_data[0][6]
        return dic
    else:
        dic['state'] = 0
        return dic


def querySencerData(local_cnn, local_cursor, sencer):
    dic = {}
    for i in range(len(sencer)):
        dic[sencer[i]] = []
    query_string = "select * from newdata order by Timestamp desc limit 5"
    local_cursor.execute(query_string)
    data = local_cursor.fetchall()
    data.reverse()
    for item in data:
        for i in range(len(sencer)):
            dic[sencer[i]].append(item[i + 2])
    query_string = "select Timestamp from newdata order by Timestamp desc limit 1"
    local_cursor.execute(query_string)
    newstamp = local_cursor.fetchall()[0][0]
    return dic, newstamp


def queryNormal(local_cnn, local_cursor):
    query_string = "SELECT EmptyLoad_set  FROM `setpoint` order by ID DESC limit 1"
    local_cursor.execute(query_string)
    NormalList = local_cursor.fetchall()
    if NormalList:
        NormalList = NormalList[0][0].split(',')
        NormalList = [float(x) for x in NormalList]
        return NormalList
    else:
        return 0


def forcedJudge(dic, ListNormal, Scncer):
    dic_result1 = {}
    dic_result2 = {}
    listNow = []
    for key in Scncer:
        listNow.append(dic[key][4])
    state = [a - b for a, b in zip(listNow, ListNormal)]
    sortedlist = state[:]
    sortedlist.sort()
    dismax = sortedlist[3] - sortedlist[0]
    dismax2 = sortedlist[2] - sortedlist[1]
    print('forcedjudge:', dismax, dismax2)
    for i in Scncer:
        dic_result1[i + '_Forced'] = 0
    for i in Scncer:
        dic_result2[i + '_Partical'] = 0
    if dismax > 8:
        dic_result1[Scncer[state.index(sortedlist[3])] + '_Forced'] = 2
    elif dismax > 3 and dismax2 > 3:
        if abs(state.index(sortedlist[2]) - state.index(sortedlist[3])) == 1:
            dic_result2[Scncer[min(state.index(sortedlist[2]), state.index(sortedlist[3]))] + '_Partical'] = 5
        elif abs(state.index(sortedlist[2]) - state.index(sortedlist[3])) == 3:
            dic_result2[Scncer[4] + '_Partical'] = 5
    return dic_result1, dic_result2


def lostSigJudge(dic):
    dic_result = {}
    for key, value in dic.items():
        cnt = 0
        for v in value:
            if abs(v) == 0:
                cnt += 1
            else:
                break
        dic_result[key + '_lostSig'] = 1 if cnt == 5 else 0
    return dic_result


def overJugde(dic):
    dic_result = {}
    for key, value in dic.items():
        cnt = 0
        for v in value:
            if v > 15:
                cnt += 1
        if value[4] > 15 and cnt != 5:
            dic_result[key + '_over'] = 3
        elif cnt == 5:
            dic_result[key + '_over'] = 4
        else:
            dic_result[key + '_over'] = 0
    return dic_result


def insertDiagnosis(local_cnn, local_cursor, LostResult, OverResult, ForcedResult, PartialResult, sencer):
    Lostlist = []
    Overlist = []
    Forcedlist = []
    Partiallist = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for key in sencer:
        Lostlist.append(LostResult[key + '_lostSig'])
        Overlist.append(OverResult[key + '_over'])
        Forcedlist.append(ForcedResult[key + '_Forced'])
        Partiallist.append(PartialResult[key + '_Partical'])
    if (3 in Overlist) or (4 in Overlist) or (5 in Partiallist) or (2 in Forcedlist) or (1 in Lostlist):
        state = 1
    else:
        state = 0
    query_string = ("INSERT INTO `diagnosis`(`Timestamp`, `ScaleState`, `Tag1Loss`, `Tag2Loss`, `Tag3Loss`, `Tag4Loss`, `Tag1Over`, `Tag2Over`, `Tag3Over`, `Tag4Over`,`Tag1Forced`, `Tag2Forced`, `Tag3Forced`, `Tag4Forced`, `Tag1Partial`, `Tag2Partial`, `Tag3Partial`, `Tag4Partial`) VALUES ('%s', %d, %d, %d, %d,%d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d, %d)" % (now,state,Lostlist[0],Lostlist[1],Lostlist[2],Lostlist[3],Overlist[0],Overlist[1],Overlist[2],Overlist[3], Forcedlist[0],Forcedlist[1],Forcedlist[2],Forcedlist[3],Partiallist[0],Partiallist[1],Partiallist[2],Partiallist[3]))
    local_cursor.execute(query_string)
    local_cnn.commit()


def updateFault(local_cnn, local_cursor, faultCode, faultSencer, diagstate, fault_time):
    query_string = "select * from faultlist where FaultCode=" + faultCode + " and FaultState=1 and FaultSencer='" + faultSencer + "' order by id desc limit 1"
    local_cursor.execute(query_string)
    query_data = local_cursor.fetchall()
    if query_data:
        if str(diagstate) == faultCode:
            print(faultSencer, ":", diagstate)
            return 1
        elif diagstate == 0:
            recover_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            period_second = (datetime.now() - query_data[0][1]).seconds
            fault_state = 0
            query_string = ("update faultlist set RecoverTime='%s', PeriodSecond=%d, FaultState=%d where id=%d" % (recover_time, period_second, fault_state, query_data[0][0]))
            local_cursor.execute(query_string)
            local_cnn.commit()
            return 0
    else:
        if str(diagstate) == faultCode:
            query_string = ("insert into faultlist (FaultTime,FaultCode,FaultState,FaultSencer) values('%s',%d,1,'%s')" % (fault_time, diagstate, faultSencer))
            local_cursor.execute(query_string)
            local_cnn.commit()
            print(faultSencer, ":", diagstate)
            return 1
        elif diagstate == 0:
            return 0


def main():
    global timeold
    local_cnn = connection('127.0.0.1', '123456')
    local_cursor = local_cnn.cursor()
    sencer = ['WeightTag1', 'WeightTag2', 'WeightTag3', 'WeightTag4']
    while 1:
        start = datetime.now()
        try:
            data, newstamp = querySencerData(local_cnn, local_cursor, sencer)
            newstamp = newstamp.strftime("%Y-%m-%d %H:%M:%S")
            if newstamp == timeold:
                print('no new data')
                local_cursor.close()
                local_cnn.close()
                time.sleep(1)
                break
        except:
            print('query failed')
            local_cursor.close()
            local_cnn.close()
            break
        NormalData = queryNormal(local_cnn, local_cursor)
        if not NormalData:
            print('no setpoint')
            local_cursor.close()
            local_cnn.close()
            break
        ForcedResult, PartialResult = forcedJudge(data, NormalData, sencer)
        LostResult = lostSigJudge(data)
        OverResult = overJugde(data)
        insertDiagnosis(local_cnn, local_cursor, LostResult, OverResult, ForcedResult, PartialResult, sencer)
        faultCodeList = ['1', '2', '3', '4', '5']
        state_prealarm = [0, 0, 0, 0]
        state_fault = [0, 0, 0, 0]
        for i in range(len(sencer)):
            for code in faultCodeList:
                if code == '1':
                    sigState = updateFault(local_cnn, local_cursor, code, sencer[i], LostResult[sencer[i] + '_lostSig'], newstamp)
                    if sigState == 1:
                        state_fault[i] = 1
                elif code == '2':
                    loadState = updateFault(local_cnn, local_cursor, code, sencer[i], ForcedResult[sencer[i] + '_Forced'], newstamp)
                    if (loadState == 1):
                        state_prealarm[i] = 1
                elif code == '5':
                    particalState = updateFault(local_cnn, local_cursor, code, sencer[i], PartialResult[sencer[i] + '_Partical'], newstamp)
                    if (particalState == 1):
                        state_prealarm[i] = 1
                else:
                    OverState = updateFault(local_cnn, local_cursor, code, sencer[i], OverResult[sencer[i] + '_over'], newstamp)
                    if (OverState == 1) and (code == '3'):
                        state_prealarm[i] = 1
                    elif (OverState == 1) and (code == '4'):
                        state_fault[i] = 1
        stateF = 0
        stateA = 0
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        for v1, v2 in zip(state_fault, state_prealarm):
            if v1 == 1:
                stateF += 1
            if v2 == 1:
                stateA += 1
        state_fault = 1 if stateF > 0 else 0
        state_prealarm = 1 if stateA > 0 and stateF == 0 else 0
        state_normal = 1 if (state_fault == 0) and (state_prealarm == 0) else 0
        query_string = ("insert into checkedlist (Timestamp,state_fault,state_normal,state_prealarm) values ('%s',%d,%d,%d)" % (now, state_fault, state_normal, state_prealarm))
        local_cursor.execute(query_string)
        local_cnn.commit()
        timeold = newstamp
        end = datetime.now()
        print("all:", (end - start).seconds)
        time.sleep(1)


timeold = ''
while 1:
    main()
