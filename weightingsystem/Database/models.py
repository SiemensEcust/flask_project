from sqlalchemy import Column, Integer, ForeignKey, String, DateTime, Float, create_engine
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
import datetime

Base = declarative_base()


# 一张表
class Factory(Base):
    __tablename__ = 'Factory'
    ID = Column(String(20), primary_key=True)
    address = Column(String(40))
    responsor = Column(String(20))


# 每个工厂一张，命名：FacID + EQP
class Equipment(Base):
    __tablename__ = 'Equipment'
    ID = Column(String(20), primary_key=True)
    supplier = Column(String(20), ForeignKey('Supplier.ID'))
    State = relationship("countState", backref="Equipment")


# 每个工厂一张，命名：FacID + Sup
class Supplier(Base):
    __tablename__ = 'Supplier'
    ID = Column(String(20), primary_key=True)
    info = Column(String(100))
    contact = Column(String(20))
    Eqp = relationship("Equipment", backref="Supplier")


# 每个工厂一张，命名：FacID + countState
class countState(Base):
    __tablename__ = 'CountState'
    ID = Column(Integer, primary_key=True)
    EqpID = Column(String(20), ForeignKey('Equipment.ID'))
    Timestamp = Column(DateTime, default=datetime.datetime.now, index=True)
    fault = Column(Integer)
    alarm = Column(Integer)
    nromal = Column(Integer)


# 每个设备一张，命名：FacID + EqpID + Thread
class Thread(Base):
    __tablename__ = 'Thread'
    ID = Column(Integer, primary_key=True)
    Timestamp = Column(DateTime, default=datetime.datetime.now, index=True)
    SencerNum = Column(Integer)
    SencerName = Column(String(125))
    EqpID = Column(String(20), ForeignKey('Equipment.ID'))
    NoLoad_set = Column(String(100))
    EmptyLoad_set = Column(String(100))
    Temp = Column(Float)
    Wet = Column(Float)
    ExcV = Column(Float)
    Sensitivity = Column(Float)
    Resistance = Column(Integer)
    standard = Column(Float)
    zeropoint = Column(Float)


# 每个设备一张，命名：FacID + EqpID + FaultList
class FaultList(Base):
    __tablename__ = 'Faultlist'
    ID = Column(Integer, primary_key=True)
    FaultTime = Column(DateTime, default=datetime.datetime.now, index=True)
    RecoverTime = Column(DateTime)
    PeriodSecond = Column(Integer)
    FaultSencer = Column(String(20), index=True)
    FaultCode = Column(Integer, index=True)


# 每个设备一张，命名：FacID + EqpID + Operation
class Operation(Base):
    __tablename__ = 'Operation'
    ID = Column(Integer, primary_key=True)
    Timestamp = Column(DateTime, default=datetime.datetime.now, index=True)
    record = Column(String(250))


# 每个设备每天一张，命名：FacID + EqpID + NewVal + Date
class NewVal(Base):
    __tablename__ = 'NewVal'
    ID = Column(Integer, primary_key=True)
    Timestamp = Column(DateTime, default=datetime.datetime.now, index=True)
    WeightTag1 = Column(Float)
    WeightTag2 = Column(Float)
    WeightTag3 = Column(Float)
    WeightTag4 = Column(Float)
    Weight = Column(Float)


# 每个设备每天一张，命名：FacID + EqpID + FaultMsg + Date
class FaultMsg(Base):
    __tablename__ = 'FaultMsg'
    ID = Column(Integer, primary_key=True)
    Timestamp = Column(DateTime, default=datetime.datetime.now, index=True)
    faultCode = Column(Integer)
    eqpState = Column(Integer)
