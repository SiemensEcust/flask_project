#!/usr/bin/env python
# -*- coding: utf-8 -*-
import mysql.connector
import datetime
from datetime import timedelta


class DataBaseOperation(object):
    """Class for DataBase operation
       main property:
       list factorylist
       dic Eqpdic {facID: Eqplist}(list Eqplist)
       datetime date
       DBconnect DBconnector
       Operation:
       string newFactory(self, string facID, string address, string responsor)
       string newEquipment(self, String facID, string EqpID, dic SP)
       string maintainDB(self)"""

    def __init__(self):
        super(DataBaseOperation, self).__init__()
        self.factoryList = []
        self.Eqpdic = {}
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

    def newFactory(self, facID, address, responsor):
        cursor = self.cnn.cursor()
        queryString = ("select * from factory where id = '%s'" % facID)
        cursor.execute(queryString)
        result = cursor.fetchall()
        if (result):
            cursor.close()
            return "工厂已存在，数据库创建失败!"
        else:
            queryString = ("INSERT INTO `factory`(`id`, `address`, `responsor`) VALUES ('%s', '%s', '%s')" % (facID, address, responsor))
            cursor.execute(queryString)
            self.cnn.commit()
            queryString = ("CREATE TABLE `%s` (`id` varchar(20) NOT NULL,`info` varchar(100) DEFAULT NULL,`contact` varchar(20) DEFAULT NULL,PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8" % (facID + "sup"))
            cursor.execute(queryString)
            queryString = ("CREATE TABLE `%s` (`id` varchar(20) NOT NULL,`place` varchar(20) DEFAULT NULL,`supplier` varchar(20) DEFAULT NULL,PRIMARY KEY (`id`),KEY `supplier` (`supplier`),KEY `ix_Equipment_place` (`place`),CONSTRAINT `equipment_ibfk_%s` FOREIGN KEY (`supplier`) REFERENCES `%s` (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8" % (facID + "eqp", facID, facID + "sup"))
            cursor.execute(queryString)
            queryString = ("CREATE TABLE `%s` (`id` int(11) NOT NULL AUTO_INCREMENT,`EqpID` varchar(20) DEFAULT NULL,`Timestamp` datetime DEFAULT NULL,`fault` int(11) DEFAULT NULL,`alarm` int(11) DEFAULT NULL,`nromal` int(11) DEFAULT NULL,PRIMARY KEY (`id`),KEY `EqpID` (`EqpID`),KEY `ix_countState_Timestamp` (`Timestamp`),CONSTRAINT `countstate_ibfk_%s` FOREIGN KEY (`EqpID`) REFERENCES `%s` (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8" % (facID + "countstate", facID, facID + "eqp"))
            cursor.execute(queryString)
            cursor.close()
            return "工厂数据库创建成功！"

    def newEquip(self, facID, EqpID, SPdic):
        cursor = self.cnn.cursor(buffered=True)
        # queryString = ("select * from `%s` where id = '%s'" % (facID + "Eqp", EqpID))
        # result = cursor.execute(queryString)
        date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # if result:
        #     queryString = ("insert into %s (`Timestamp`, `SencerNum`, `SencerName`, `NoLoad_set`, `EmptyLoad_set`, `Temp`, `Wet`, `ExcV`, `Sensitivity`, `Resistance`) VALUES ('%s',%d,'%s','%s', '%s',%f,%f,'%s','%s','%s')" % (facID + EqpID + "Info", date, SPdic['Num'], SPdic['Name'], SPdic['Noload'], SPdic['Empty'], SPdic['Temp'], SPdic['Wet'], SPdic['ExcV'], SPdic['Sensivity'], SPdic['Resistance']))
        #     cursor.execute(queryString)
        #     queryString = ("INSERT INTO `%s`(`Timestamp`, `standard`, `zeropoint`) VALUES ('%s', '%s', '%s')" % (facID + EqpID + "Thread", date, SPdic['Standard'], SPdic['Zeropoint']))
        #     cursor.execute(queryString)
        #     self.cnn.commit()
        #     cursor.close()
        #     return "Equipment is exicted, updating the thread"
        # else:
        #     queryString = ("select * from `%s` where id = '%s'" % (facID + "Sup", SPdic['supplier']))
        #     result1 = cursor.execute(queryString)
        #     if (not result1):
        #         queryString = ("insert into `%s`(id,info,contact) values('%s', '%s', '%s')" % (facID + "Sup", SPdic['supplier'], SPdic['info'], SPdic['contact']))
        #         cursor.execute(queryString)
        #         self.cnn.commit()
        queryString = ("insert into `%s`(id,supplier,place) values('%s', '%s', '%s')" % (facID + "eqp", EqpID, SPdic['supplier'], SPdic['place']))
        cursor.execute(queryString)
        self.cnn.commit()
        queryString = ("insert into `%s`(EqpID,Timestamp,fault,alarm,nromal) values('%s', '%s', 0,0,0)" % (facID + "countstate", EqpID, date))
        cursor.execute(queryString)
        self.cnn.commit()
        queryString = ("CREATE TABLE `%s` (`id` int(11) NOT NULL AUTO_INCREMENT,`Timestamp` datetime DEFAULT NULL,`SencerNum` int(11) DEFAULT NULL,`SencerName` varchar(125) DEFAULT NULL,`NoLoad_set` varchar(100) DEFAULT NULL,`EmptyLoad_set` varchar(100) DEFAULT NULL,`Temp` float DEFAULT NULL,`Wet` float DEFAULT NULL,`ExcV` float DEFAULT NULL,`Sensitivity` float DEFAULT NULL,`Resistance` int(11) DEFAULT NULL,PRIMARY KEY (`id`),KEY `ix_Eqpinfo_Timestamp` (`Timestamp`)) ENGINE=InnoDB DEFAULT CHARSET=utf8" % (facID + EqpID +"info"))
        cursor.execute(queryString)
        queryString = ("CREATE TABLE `%s` (`id` int(11) NOT NULL AUTO_INCREMENT,`Timestamp` datetime DEFAULT NULL, `standard` float DEFAULT NULL,`zeropoint` float DEFAULT NULL,PRIMARY KEY (`id`),KEY `ix_Thread_Timestamp` (`Timestamp`)) ENGINE=InnoDB DEFAULT CHARSET=utf8" % (facID + EqpID +"thread"))
        cursor.execute(queryString)
        queryString = ("CREATE TABLE `%s` (`id` int(11) NOT NULL AUTO_INCREMENT,`Timestamp` datetime DEFAULT NULL,`record` varchar(250) DEFAULT NULL,PRIMARY KEY (`id`),KEY `ix_Operation_Timestamp` (`Timestamp`)) ENGINE=InnoDB DEFAULT CHARSET=utf8" % (facID + EqpID +"operation"))
        cursor.execute(queryString)
        queryString = ("CREATE TABLE `%s` (`id` int(11) NOT NULL AUTO_INCREMENT,`Timestamp` datetime DEFAULT NULL,`WeightTag1` float DEFAULT NULL,`WeightTag2` float DEFAULT NULL,`WeightTag3` float DEFAULT NULL,`WeightTag4` float DEFAULT NULL,`Weight` float DEFAULT NULL,PRIMARY KEY (`id`),KEY `ix_NewVal_Timestamp` (`Timestamp`)) ENGINE=InnoDB DEFAULT CHARSET=utf8" % (facID + EqpID + "newval" + date[:10]))
        cursor.execute(queryString)
        queryString = ("CREATE TABLE `%s` (`id` int(11) NOT NULL AUTO_INCREMENT,`Timestamp` datetime DEFAULT NULL,`faultCode` int(11) DEFAULT NULL,`eqpState` int(11) DEFAULT NULL,`faultTime` datetime DEFAULT NULL,`RecoverTime` datetime DEFAULT NULL,`PeriodSecond` int(11) DEFAULT NULL,PRIMARY KEY (`id`),KEY `ix_FaultMsg_Timestamp` (`Timestamp`),KEY `ix_FaultMsg_faultTime` (`faultTime`)) ENGINE=InnoDB DEFAULT CHARSET=utf8" % (facID + EqpID + "faultmsg" + date[:10]))
        cursor.execute(queryString)
        queryString = ("CREATE TABLE `%s` (`id` int(11) NOT NULL AUTO_INCREMENT,`FaultTime` datetime DEFAULT NULL,`RecoverTime` datetime DEFAULT NULL,`PeriodSecond` int(11) DEFAULT NULL,`FaultSencer` varchar(20) DEFAULT NULL,`FaultCode` int(11) DEFAULT NULL,`FaultState` tinyint(1) DEFAULT NULL,PRIMARY KEY (`id`),KEY `ix_Faultlist_FaultCode` (`FaultCode`),KEY `ix_Faultlist_FaultSencer` (`FaultSencer`),KEY `ix_Faultlist_FaultTime` (`FaultTime`)) ENGINE=InnoDB DEFAULT CHARSET=utf8" % (facID + EqpID + "faultlist"))
        cursor.execute(queryString)
        queryString = ("insert into %s (`Timestamp`, `SencerNum`, `SencerName`, `NoLoad_set`, `EmptyLoad_set`, `Temp`, `Wet`, `ExcV`, `Sensitivity`, `Resistance`) VALUES ('%s',%d,'%s','%s', '%s',%f,%f,'%s','%s','%s')" % (facID + EqpID + "info", date, SPdic['Num'], SPdic['Name'], SPdic['Noload'], SPdic['Empty'], SPdic['Temp'], SPdic['Wet'], SPdic['ExcV'], SPdic['Sensivity'], SPdic['Resistance']))
        cursor.execute(queryString)
        queryString = ("INSERT INTO `%s`(`Timestamp`, `standard`, `zeropoint`) VALUES ('%s', '%s', '%s')" % (facID + EqpID + "thread", date, SPdic['Standard'], SPdic['Zeropoint']))
        cursor.execute(queryString)
        self.cnn.commit()
        cursor.close()
        return "设备数据库创建成功!"

    def maintainDB(self):
        cursor = self.cnn.cursor()
        queryString = ("select id from factory")
        createDate = (datetime.datetime.now() + timedelta(1)).strftime('%Y-%m-%d')
        cursor.execute(queryString)
        result = cursor.fetchall()
        if result:
            for item in result:
                self.factoryList.append(item[0])
                queryString = ("select id from %s" % (item[0] + 'eqp'))
                cursor.execute(queryString)
                result1 = cursor.fetchall()
                if result1:
                    self.Eqpdic[item[0]] = result1
                else:
                    return "No Equipment in factory %s" % item
            print(self.factoryList)
            print(self.Eqpdic)
            for fac in self.factoryList:
                for eqp in self.Eqpdic[fac]:
                    queryString = ("CREATE TABLE `%s` (`ID` int(11) NOT NULL AUTO_INCREMENT,`Timestamp` datetime DEFAULT NULL,`WeightTag1` float DEFAULT NULL,`WeightTag2` float DEFAULT NULL,`WeightTag3` float DEFAULT NULL,`WeightTag4` float DEFAULT NULL,`Weight` float DEFAULT NULL,PRIMARY KEY (`ID`),KEY `ix_NewVal_Timestamp` (`Timestamp`)) ENGINE=InnoDB DEFAULT CHARSET=utf8" % (fac + eqp[0] + "newval" + createDate[:10]))
                    cursor.execute(queryString)
                    queryString = ("CREATE TABLE `%s` ( `id` int(11) NOT NULL AUTO_INCREMENT, `Timestamp` datetime DEFAULT NULL, `Partial` int(11) DEFAULT NULL, `Forced` int(11) DEFAULT NULL, `Loss` int(11) DEFAULT NULL, `Over` int(11) DEFAULT NULL, `eqpState` int(11) DEFAULT NULL, PRIMARY KEY (`id`), KEY `ix_FaultMsg_Timestamp` (`Timestamp`)) ENGINE=InnoDB DEFAULT CHARSET=utf8" % (fac + eqp[0] + "faultmsg" + createDate[:10]))
                    cursor.execute(queryString)
            return "create DB successfully"
        else:
            return "No factory please create factory first"


# database = DataBaseOperation()
# print(database.newFactory('factory01', '上海', 'ECUST'))
# SPdic = {}
# SPdic['info'] = 0
# SPdic['contact'] = 0
# SPdic['supplier'] = 0
# SPdic['Num'] = 0
# SPdic['Name'] = 0
# SPdic['Noload'] = 0
# SPdic['Empty'] = 0
# SPdic['Temp'] = 0
# SPdic['Wet'] = 0
# SPdic['ExcV'] = 0
# SPdic['Sensivity'] = 0
# SPdic['Resistance'] = 0
# SPdic['Standard'] = 0
# SPdic['Zeropoint'] = 0
# SPdic['place'] = 0
# print(database.newEquip('factory01', 'scale001', SPdic))
