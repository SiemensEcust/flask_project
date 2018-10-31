#!/usr/bin/env python
# -*- coding: utf-8 -*-
from datetime import datetime
from flask_login import login_required, current_user
from flask import render_template, session, redirect, url_for, flash, abort, request, jsonify
from . import main
from .. import db
from ..models import Equipment, FaultMsg, NewVal, FaultList


def code2state(state):
    if state == 1:
        return '预警状态'
    elif state == 2:
        return '设备故障'
    else:
        return '正常运行'


def codeToMes(code):
    if code == 1:
        return '信号丢失故障'
    elif code == 3:
        return '瞬时过流故障'
    elif code == 2:
        return '瞬时受力报警'
    elif code == 4:
        return '超出量程故障'
    elif code == 5:
        return '偏载报警'


def judgeFault(x):
    res = []
    x += 10000
    while x > 0:
        res.insert(0, x % 10)
        x = x // 10
    return res


@main.route('/', methods=['POST', 'GET'])
def index():
    date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')[:10]
    if current_user.is_authenticated:
        if current_user.role_id >= 3:
            Equipment.__table__.name = current_user.factoryID + 'eqp'
            eqpList = db.session.query(Equipment).all()
            for eqp in eqpList:
                FaultMsg.__table__.name = current_user.factoryID + eqp.id + 'faultmsg' + date
                state = db.session.query(FaultMsg).order_by(FaultMsg.id.desc()).first()
                setattr(eqp, 'state', code2state(state.eqpState))
            return render_template('main/MainPage.html', eqpList=eqpList)
        elif current_user.role_id < 3:
            eqpList = []
            eqpName = current_user.EqpID
            if eqpName:
                eqpName = eqpName.split(",")
                Equipment.__table__.name = current_user.factoryID + 'eqp'
                for item in eqpName:
                    eqpList.append(db.session.query(Equipment).filter(Equipment.id == item).first())
                for eqp in eqpList:
                    FaultMsg.__table__.name = current_user.factoryID + eqp.id + 'faultmsg' + date
                    state = db.session.query(FaultMsg).order_by(FaultMsg.id.desc()).first()
                    setattr(eqp, 'state', code2state(state.eqpState))
            else:
                flash("请联系管理员为您添加设备！")
            return render_template('main/MainPage.html', eqpList=eqpList)
        else:
            pass
    return render_template('/main/MainPage.html')


@main.route('/Running/<string:EqpName>', methods=['POST', 'GET'])
@login_required
def Running(EqpName):
    return render_template('/main/RunningUI.html')


@main.route('/QueryNow', methods=['POST', 'GET'])
@login_required
def QueryNow():
    dicRes = {}
    date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')[:10]
    eqp = request.form.get("eqp", "")
    NewVal.__table__.name = current_user.factoryID + eqp + 'newval' + date
    result = db.session.query(NewVal).order_by(NewVal.id.desc()).first()
    dicRes['Tag1'] = result.WeightTag1
    dicRes['Tag2'] = result.WeightTag2
    dicRes['Tag3'] = result.WeightTag3
    dicRes['Tag4'] = result.WeightTag4
    dicRes['weight'] = result.WeightTag1 + result.WeightTag2 + result.WeightTag3 + result.WeightTag4
    FaultMsg.__table__.name = current_user.factoryID + eqp + 'faultmsg' + date
    result2 = db.session.query(FaultMsg).order_by(FaultMsg.id.desc()).first()
    Partial = judgeFault(result2.Partial)[1: 5]
    Forced = judgeFault(result2.Forced)[1: 5]
    Loss = judgeFault(result2.Loss)[1: 5]
    Over = judgeFault(result2.Over)[1: 5]
    print(Partial, Forced, Loss, Over)
    for i in range(len(Partial)):
        if Forced[i] == 2 or Over[i] == 3:
            dicRes['state' + str(i + 1)] = 1
        elif Loss[i] == 1 or Over[i] == 4:
            dicRes['state' + str(i + 1)] = 2
        else:
            dicRes['state' + str(i + 1)] = 0
        if Partial[i] == 5:
            dicRes['partial' + str(i + 1)] = 1
        else:
            dicRes['partial' + str(i + 1)] = 0
    return jsonify(dicRes)


@main.route('/QueryFault', methods=['POST', 'GET'])
@login_required
def QueryFault():
    dic = {}
    dic['id'] = []
    dic['fault_time'] = []
    dic['recover_time'] = []
    dic['period_second'] = []
    dic['fault_reason'] = []
    dic['fault_state'] = []
    dic['fault_level'] = []
    eqp = request.form.get("eqp", "")
    facID = current_user.factoryID
    FaultList.__table__.name = facID + eqp + 'faultlist'
    result = db.session.query(FaultList).order_by(FaultList.id.desc()).limit(30)
    for item in result:
        dic['id'].append(item.id)
        dic['fault_time'].append(item.FaultTime.strftime('%Y-%m-%d %H:%M:%S'))
        if item.RecoverTime:
            dic['recover_time'].append(item.RecoverTime.strftime('%Y-%m-%d %H:%M:%S'))
            delt = (item.RecoverTime - item.FaultTime).seconds
            days = delt // (24 * 3600)
            hours = (delt - (24 * 3600) * days) // 3600
            minutes = (delt - (24 * 3600) * days - 3600 * hours) // 60
            dic['period_second'].append(str(days) + "天" + str(hours) + "小时" + str(minutes) + "分钟")
        else:
            dic['recover_time'].append('——')
            dic['period_second'].append('——')
        if item.FaultSencer:
            dic['fault_reason'].append(item.FaultSencer + codeToMes(item.FaultCode))
        # else:
        #     dic['fault_reason'].append()
        dic['fault_state'].append('已修复' if item.FaultState == 0 else '未修复')
        dic['fault_level'].append(2 if item.FaultCode in [1, 3, 4] else 1)
    return jsonify(dic)
