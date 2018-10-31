#!/usr/bin/env python
# -*- coding: utf-8 -*-
import datetime
import time


class scaleOperation(object):
    """docstring for scaleOperation"""
    def __init__(self, facID, eqpID, cnn):
        super(scaleOperation, self).__init__()
        self.facID = facID
        self.eqpID = eqpID
        self.cursor = cnn.cursor()
        # self.E0 = []
        # self.Ec = []
        self.Ee = []
        self.Val = []
        self.timeNow = ""
        self.timeOld = ""

    def readSP(self):
        query_string = ("select EmptyLoad_set from `%s` order by id desc limit 1" % (self.facID + self.eqpID + "info"))
        self.cursor.execute(query_string)
        result = self.cursor.fetchall()
        if result:
            self.Ee = [float(i) for i in list(result[0])[0].split(",")]
        else:
            return False

    def queryVal(self):
        res = []
        for i in range(len(self.Ee)):
            res.append([])
        date = datetime.datetime.now().strftime("%Y-%m-%d")
        query_string = ("select * from `%s` order by id desc limit 5" % (self.facID + self.eqpID + "newval" + date))
        self.cursor.execute(query_string)
        data = self.cursor.fetchall()
        if not data:
            return False
        else:
            data.reverse()
            for item in data:
                for i in range(len(self.Ee)):
                    res[i].append(item[i + 2])
            self.Val = res
            self.timeNow = data[4][1]
            print(self.timeNow)
            return True

    def forcedJudge(self):
        resPartial = [0 for i in range(len(self.Ee))]
        resForced = [0 for i in range(len(self.Ee))]
        listNow = [self.Val[i][len(self.Val[0]) - 1] for i in range(len(self.Val))]
        print(listNow)
        state = [a - b for a, b in zip(listNow, self.Ee)]
        sortedlist = state[:]
        sortedlist.sort()
        dismax = sortedlist[3] - sortedlist[0]
        dismax2 = sortedlist[2] - sortedlist[1]
        if dismax > 8:
            resForced[state.index(sortedlist[3])] = 2
        elif dismax > 3 and dismax2 > 3:
            if abs(state.index(sortedlist[2]) - state.index(sortedlist[3])) == 1:
                resPartial[min(state.index(sortedlist[2]), state.index(sortedlist[3]))] = 5
            elif abs(state.index(sortedlist[2]) - state.index(sortedlist[3])) == 3:
                resPartial[3] = 5
        return resPartial, resForced

    def lostSigJudge(self):
        resLoss = [0 for i in range(len(self.Ee))]
        for i in range(len(resLoss)):
            if self.Val[i].count(0) == len(resLoss):
                resLoss[i] = 1
        return resLoss

    def overJudge(self):
        resOver = [0 for i in range(len(self.Ee))]
        for i in range(len(resOver)):
            state = self.Val[i][:]
            state.sort()
            if self.Val[i][4] > 15:
                resOver[i] = 3
            elif state[0] > 15:
                resOver[i] = 4
        return resOver

    def insertDiagnosis(self, resPartial, resForced, resLoss, resOver, cnn):
        date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        Partial = 0
        Forced = 0
        Over = 0
        Loss = 0
        for i in range(len(resPartial)):
            Partial += resPartial[i] * (10 ** i)
            Forced += resForced[i] * (10 ** i)
            Over += resOver[i] * (10 ** i)
            Loss += resLoss[i] * (10 ** i)
        if (2 in resForced) or (5 in resForced) or (3 in resOver):
            state = 1
        elif (1 in resLoss) or (4 in resOver):
            state = 2
        else:
            state = 0
        query_string = ("INSERT INTO `%s`(`Timestamp`, `Partial`, `Forced`, `Loss`, `Over`, `eqpState`) VALUES ('%s', %d, %d, %d, %d, %d)" % (self.facID + self.eqpID + 'faultmsg' + date[:10], date, Partial, Forced, Loss, Over, state))
        self.cursor.execute(query_string)
        cnn.commit()

    def updateFault(self, cnn, faultCode, faultSencer, diagstate, fault_time):
        query_string = "select * from " + self.facID + self.eqpID + "faultlist where FaultCode=" + faultCode + " and FaultState=1 and FaultSencer='" + faultSencer + "' order by id desc limit 1"
        self.cursor.execute(query_string)
        query_data = self.cursor.fetchall()
        if query_data:
            if str(diagstate) == faultCode:
                print(faultSencer, ":", diagstate)
                return
            elif diagstate == 0:
                recover_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                period_second = (datetime.datetime.now() - query_data[0][1]).seconds
                fault_state = 0
                query_string = ("update " + self.facID + self.eqpID + "faultlist set RecoverTime='%s', PeriodSecond=%d, FaultState=%d where id=%d" % (recover_time, period_second, fault_state, query_data[0][0]))
                self.cursor.execute(query_string)
                cnn.commit()
                return
        else:
            if str(diagstate) == faultCode:
                query_string = ("insert into " + self.facID + self.eqpID + "faultlist (FaultTime,FaultCode,FaultState,FaultSencer) values('%s',%d,1,'%s')" % (fault_time, diagstate, faultSencer))
                self.cursor.execute(query_string)
                cnn.commit()
                print(faultSencer, ":", diagstate)
                return
            elif diagstate == 0:
                return

    def diagnosis(self, cnn):
        query_string = "select SencerName from `" + self.facID + self.eqpID + "info` order by id desc limit 1"
        self.cursor.execute(query_string)
        sencername = self.cursor.fetchall()
        if not sencername:
            return "无该设备记录"
        sencername = sencername[0][0].split(',')
        while True:
            start = datetime.datetime.now()
            self.readSP()
            if not self.Ee:
                print('no SP')
                time.sleep(1)
                break
            try:
                readSuccess = self.queryVal()
                if self.timeNow == self.timeOld or not readSuccess:
                    print(self.timeNow, self.timeOld)
                    print('no new data')
                    time.sleep(5)
                    break
            except Exception as e:
                print('query failed, error: ', e)
                break
            # self.queryVal()
            resPartial, resForced = self.forcedJudge()
            resLoss = self.lostSigJudge()
            resOver = self.overJudge()
            self.insertDiagnosis(resPartial=resPartial, resForced=resForced, resLoss=resLoss, resOver=resOver, cnn=cnn)
            faultCodeList = ['1', '2', '3', '4', '5']
            faultTime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            for i in range(len(sencername)):
                for code in faultCodeList:
                    self.updateFault(cnn, code, sencername[i], resLoss[i], faultTime)
                    self.updateFault(cnn, code, sencername[i], resOver[i], faultTime)
                    self.updateFault(cnn, code, sencername[i], resPartial[i], faultTime)
                    self.updateFault(cnn, code, sencername[i], resForced[i], faultTime)
            self.timeOld = self.timeNow
            end = datetime.datetime.now()
            print("all Operation Time:", (end - start).seconds)
            time.sleep(5 - (end - start).seconds)
