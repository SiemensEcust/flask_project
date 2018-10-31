#!/usr/bin/env python
# -*- coding: utf-8 -*-
from . import db
import datetime
from werkzeug.security import generate_password_hash, check_password_hash
from flask_login import UserMixin
from . import login_manager
from itsdangerous import TimedJSONWebSignatureSerializer as Serializer
from flask import current_app
# import itertools
# import random


# 记载用户的回调函数
@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))


class Permission:
    FOLLOW = 0
    Operator = 1
    Engineer = 2
    ADMINSTER = 3
    Root = 4


class Role(db.Model):
    __tablename__ = 'roles'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(64), unique=True)
    users = db.relationship('User', backref='role')

    def __repr__(slef):
        return '<Role %r>' % slef.name


class User(UserMixin, db.Model):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(64), unique=True, index=True)
    role_id = db.Column(db.Integer, db.ForeignKey('roles.id'))
    password_hash = db.Column(db.String(128))
    factoryID = db.Column(db.String(128), db.ForeignKey('factory.id'))
    EqpID = db.Column(db.String(128))
    # 授权列，暂时不用
    confirmed = db.Column(db.Boolean, default=False)

    # 新增密码散列化
    @property
    def password(self):
        raise AttributeError('password is not a readable attr')

    @password.setter
    def password(self, password):
        self.password_hash = generate_password_hash(password)

    def verify_password(self, password):
        return check_password_hash(self.password_hash, password)

    def __repr__(slef):
        return '<User %r>' % slef.username


class Factory(db.Model):
    __tablename__ = 'factory'
    id = db.Column(db.String(20), primary_key=True)
    address = db.Column(db.String(40))
    responsor = db.Column(db.String(20))


# 每个工厂一张，命名：FacID + EQP
class Equipment(db.Model):
    __tablename__ = 'equipment'
    id = db.Column(db.String(20), primary_key=True)
    place = db.Column(db.String(20), index=True)
    supplier = db.Column(db.String(20), db.ForeignKey('supplier.id'))
    State = db.relationship("countState", backref="equipment")


class FEquipment(Equipment):

    def __init__(self, name):
        super(FEquipment, self).__init__()
        self.__tablename__ = name


# 每个工厂一张，命名：FacID + Sup
class Supplier(db.Model):
    __tablename__ = 'supplier'
    id = db.Column(db.String(20), primary_key=True)
    info = db.Column(db.String(100))
    contact = db.Column(db.String(20))
    Eqp = db.relationship("Equipment", backref="Eqpsupplier")


# 每个工厂一张，命名：FacID + countState
class countState(db.Model):
    __tablename__ = 'countstate'
    id = db.Column(db.Integer, primary_key=True)
    EqpID = db.Column(db.String(20), db.ForeignKey('equipment.id'))
    Timestamp = db.Column(db.DateTime, default=datetime.datetime.now, index=True)
    fault = db.Column(db.Integer)
    alarm = db.Column(db.Integer)
    nromal = db.Column(db.Integer)


# 每个设备一张，命名：FacID + EqpID + Info
class Eqpinfo(db.Model):
    __tablename__ = 'eqpinfo'
    id = db.Column(db.Integer, primary_key=True)
    Timestamp = db.Column(db.DateTime, default=datetime.datetime.now, index=True)
    SencerNum = db.Column(db.Integer)
    SencerName = db.Column(db.String(125))
    NoLoad_set = db.Column(db.String(100))
    EmptyLoad_set = db.Column(db.String(100))
    Temp = db.Column(db.Float)
    Wet = db.Column(db.Float)
    ExcV = db.Column(db.Float)
    Sensitivity = db.Column(db.Float)
    Resistance = db.Column(db.Integer)


# 每个设备一张，命名：FacID + EqpID + Thread
class Thread(db.Model):
    __tablename__ = 'thread'
    id = db.Column(db.Integer, primary_key=True)
    Timestamp = db.Column(db.DateTime, default=datetime.datetime.now, index=True)
    standard = db.Column(db.Float)
    zeropoint = db.Column(db.Float)


# 每个设备一张，命名：FacID + EqpID + FaultList.
class FaultList(db.Model):
    __tablename__ = 'faultlist'
    id = db.Column(db.Integer, primary_key=True)
    FaultTime = db.Column(db.DateTime, default=datetime.datetime.now, index=True)
    RecoverTime = db.Column(db.DateTime)
    PeriodSecond = db.Column(db.Integer)
    FaultSencer = db.Column(db.String(20), index=True)
    FaultCode = db.Column(db.Integer, index=True)
    FaultState = db.Column(db.Boolean)


# 每个设备一张，命名：FacID + EqpID + Operation
class Operation(db.Model):
    __tablename__ = 'operation'
    id = db.Column(db.Integer, primary_key=True)
    Timestamp = db.Column(db.DateTime, default=datetime.datetime.now, index=True)
    record = db.Column(db.String(250))


# 每个设备每天一张，命名：FacID + EqpID + NewVal + Date
class NewVal(db.Model):
    __tablename__ = 'newval'
    id = db.Column(db.Integer, primary_key=True)
    Timestamp = db.Column(db.DateTime, default=datetime.datetime.now, index=True)
    WeightTag1 = db.Column(db.Float)
    WeightTag2 = db.Column(db.Float)
    WeightTag3 = db.Column(db.Float)
    WeightTag4 = db.Column(db.Float)
    Weight = db.Column(db.Float)


# # 每个设备每天一张，命名：FacID + EqpID + FaultMsg + Date
# class FaultMsg(db.Model):
#     __tablename__ = 'FaultMsg'
#     id = db.Column(db.Integer, primary_key=True)
#     Timestamp = db.Column(db.DateTime, default=datetime.datetime.now, index=True)
#     faultCode = db.Column(db.Integer)
#     eqpState = db.Column(db.Integer)


# 每个设备每天一张，命名：FacID + EqpID + FaultMsg + Date
class FaultMsg(db.Model):
    __tablename__ = 'faultmsg'
    id = db.Column(db.Integer, primary_key=True)
    Timestamp = db.Column(db.DateTime, default=datetime.datetime.now, index=True)
    Partial = db.Column(db.Integer)
    Forced = db.Column(db.Integer)
    Loss = db.Column(db.Integer)
    Over = db.Column(db.Integer)
    eqpState = db.Column(db.Integer)
