from flask import render_template, redirect, request, url_for, flash, jsonify
from flask_login import login_user, current_user
from flask_login import logout_user, login_required
from . import UE
from ..models import User, Factory, Equipment, FaultMsg, NewVal, FaultList, Supplier, Eqpinfo
from .. import db
from ..decorators import admin_required
import logging
import json
from datetime import datetime
from datetime import timedelta
from .. import client
import random
# from pushjack import APNSSandboxClient
# from ..emailDev import send_email
logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
handler = logging.FileHandler("/var/www/weightingsystem/logs/flask.log")
handler.setLevel(level=logging.INFO)
logger.addHandler(handler)


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


@UE.route('/loginUE', methods=['GET', 'POST'])
def loginUE():
    res = {}
    if request.method == 'POST':
        a = request.get_data()
        a = str(a, encoding="utf-8")
        a = a.replace('\\n', '')
        a = a.replace('\\t', '')
        a = a.replace('\\r', '')
        dict1 = json.loads(a)
        username = dict1['username']
        password = dict1['password']
        keep = dict1['keep']
        keep = True if keep == 'true' else False
        user = User.query.filter_by(username=username).first()
        # logger.info('param', username, password, keep, user)
        # logger.info(username)
        if user is not None and user.verify_password(password):
            login_user(user, keep)
            res['state'] = 'success'
        else:
            res['state'] = 'fail'
        return jsonify(res)


@UE.route('/loginjudge', methods=['GET', 'POST'])
def loginjudge():
    res = {}
    res['state'] = "nologin"
    if current_user.is_authenticated:
        res['state'] = "haslogin"
    print(res['state'], current_user.is_authenticated)
    return jsonify(res)


@UE.route('/EqpList', methods=['GET', 'POST'])
def EqpList():
    res = {}
    res['id'] = []
    res['place'] = []
    res['state'] = []
    res['sup'] = []
    res['flag'] = 1
    # date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')[:10]
    date = datetime(2018, 6, 27, 9, 54, 10, 68665).strftime('%Y-%m-%d %H:%M:%S')[:10]
    if current_user.role_id >= 3:
        Equipment.__table__.name = current_user.factoryID + 'eqp'
        eqpList = db.session.query(Equipment).all()
        for eqp in eqpList:
            FaultMsg.__table__.name = current_user.factoryID + eqp.id + 'faultmsg' + date
            state = db.session.query(FaultMsg).order_by(FaultMsg.id.desc()).first()
            NewVal.__table__.name = current_user.factoryID + eqp.id + 'newval' + date
            result = db.session.query(NewVal).order_by(NewVal.id.desc()).first()
            res['id'].append(str(eqp.id))
            res['place'].append(str(eqp.place))
            res['state'].append(code2state(state.eqpState))
            res['sup'].append(str(round(result.WeightTag1 + result.WeightTag2 + result.WeightTag3 + result.WeightTag4, 2)))
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
                NewVal.__table__.name = current_user.factoryID + eqp.id + 'newval' + date
                result = db.session.query(NewVal).order_by(NewVal.id.desc()).first()
                res['id'].append(str(eqp.id))
                res['place'].append(str(eqp.place))
                res['state'].append(code2state(state.eqpState))
                res['sup'].append(str(round(result.WeightTag1 + result.WeightTag2 + result.WeightTag3 + result.WeightTag4, 2)))
        else:
            res['flag'] = 0
    listState = ['正常运行', '预警状态', '设备故障']
    eqplistE = ['scale00' + str(i) for i in range(2, 9)]
    eqplistE2 = ['scale0' + str(i) for i in range(10, 12)]
    placeE = ['称重工段' for i in range(2, 12)]
    stateE = [listState[random.randint(0, 2)] for i in range(2, 12)]
    supE = [round(random.random() * 5, 2) for i in range(2, 12)]
    res['id'].extend(eqplistE)
    res['id'].extend(eqplistE2)
    res['place'].extend(placeE)
    res['state'].extend(stateE)
    res['sup'].extend(supE)
    return jsonify(res)


@UE.route('/logout', methods=['GET', 'POST'])
def logout():
    logout_user()
    res = {}
    res['state'] = 1
    logger.info('log out')
    return jsonify(res)


@UE.route('/QueryNow', methods=['POST', 'GET'])
def QueryNow():
    dicRes = {}
    # date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')[: 10]
    date = datetime(2018, 6, 27, 9, 54, 10, 68665).strftime('%Y-%m-%d %H:%M:%S')[: 10]
    if request.method == 'POST':
        a = request.get_data()
        a = str(a, encoding="utf-8")
        a = a.replace('\\n', '')
        a = a.replace('\\t', '')
        a = a.replace('\\r', '')
        dict1 = json.loads(a)
    eqp = dict1['eqp']
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


@UE.route('/QueryFault', methods=['POST', 'GET'])
def QueryFault():
    dic = {}
    dic['id'] = []
    dic['fault_time'] = []
    dic['fault_sencer'] = []
    dic['fault_reason'] = []
    dic['fault_state'] = []
    dic['fault_level'] = []
    if request.method == 'POST':
        a = request.get_data()
        a = str(a, encoding="utf-8")
        a = a.replace('\\n', '')
        a = a.replace('\\t', '')
        a = a.replace('\\r', '')
        dict1 = json.loads(a)
    eqp = dict1['eqp']
    facID = current_user.factoryID
    FaultList.__table__.name = facID + eqp + 'faultlist'
    result = db.session.query(FaultList).order_by(FaultList.id.desc()).limit(30)
    for item in result:
        dic['id'].append(str(item.id))
        dic['fault_time'].append(item.FaultTime.strftime('%Y-%m-%d %H:%M:%S'))
        dic['fault_sencer'].append(item.FaultSencer)
        dic['fault_reason'].append(codeToMes(item.FaultCode))
        dic['fault_state'].append('已修复' if item.FaultState == 0 else '未修复')
        dic['fault_level'].append(2 if item.FaultCode in [1, 3, 4] else 1)
    return jsonify(dic)


@UE.route('/push', methods=['POST', 'GET'])
def push():
    dic = {}
    print(client.enabled, client.get_expired_tokens)
    if request.method == 'POST':
        a = request.get_data()
        a = str(a, encoding="utf-8")
        a = a.replace('\\n', '')
        a = a.replace('\\t', '')
        a = a.replace('\\r', '')
        dict1 = json.loads(a)
    token = dict1['token']
    token.reverse()
    tokensend = ""
    for j in token:
        if int(j) < 16:
            j = '0' + hex(int(j))[2:]
        else:
            j = hex(int(j))[2:]
        tokensend += j
    res = client.send(tokensend, "push test")
    # res2 = client2.send(tokensend, "push test")
    print(res.tokens, type(client))
    # print(res2.tokens)
    # dic['state'] = client.enabled
    # dic['expired'] = client.get_expired_tokens
    return jsonify(dic)


@UE.route('/supInfo', methods=['POST', 'GET'])
def supInfo():
    dic = {}
    if request.method == 'POST':
        a = request.get_data()
        a = str(a, encoding="utf-8")
        a = a.replace('\\n', '')
        a = a.replace('\\t', '')
        a = a.replace('\\r', '')
        dict1 = json.loads(a)
    eqp = dict1['eqp']
    facID = current_user.factoryID
    FaultList.__table__.name = facID + eqp + 'faultlist'
    Supplier.__table__.name = facID + 'sup'
    Equipment.__table__.name = facID + 'eqp'
    supInfo = db.session.query(Supplier).filter(Equipment.id == eqp).first()
    dic['supplier'] = supInfo.id
    dic['conn'] = supInfo.contact
    dic['intro'] = supInfo.info
    return jsonify(dic)


@UE.route('/eqpInfo', methods=['POST', 'GET'])
def eqpInfo():
    dic = {}
    if request.method == 'POST':
        a = request.get_data()
        a = str(a, encoding="utf-8")
        a = a.replace('\\n', '')
        a = a.replace('\\t', '')
        a = a.replace('\\r', '')
        dict1 = json.loads(a)
    eqp = dict1['eqp']
    facID = current_user.factoryID
    Eqpinfo.__table__.name = facID + eqp + 'info'
    Info = db.session.query(Eqpinfo).order_by(Eqpinfo.id.desc()).first()
    dic['Num'] = str(Info.SencerNum)
    dic['Name'] = Info.SencerName
    dic['noLoad'] = Info.NoLoad_set
    dic['emptyLoad'] = Info.EmptyLoad_set
    dic['ExcV'] = Info.ExcV
    dic['Sensitivity'] = Info.Sensitivity
    dic['Resistance'] = Info.Resistance
    return jsonify(dic)


@UE.route('/historyQuery', methods=['POST', 'GET'])
def historyQuery():
    dic = {}
    dic['Tag1'] = []
    dic['Tag2'] = []
    dic['Tag3'] = []
    dic['Tag4'] = []
    if request.method == 'POST':
        a = request.get_data()
        a = str(a, encoding="utf-8")
        a = a.replace('\\n', '')
        a = a.replace('\\t', '')
        a = a.replace('\\r', '')
        dict1 = json.loads(a)
    eqp = dict1['eqp']
    startTime = dict1['faultTime']
    # result = NewData.query.filter(NewData.Timestamp > startTime, NewData.Timestamp < endTime).all()
    NewVal.__table__.name = current_user.factoryID + eqp + "newval" + startTime[:10]
    startTime = datetime.strptime(startTime, '%Y-%m-%d %H:%M:%S') 
    result = db.session.query(NewVal).filter(NewVal.Timestamp < startTime).limit(500).all()
    result2 = db.session.query(NewVal).filter(NewVal.Timestamp >= startTime).limit(500).all()
    result.extend(result2)
    for item in result:
        dic['Tag1'].append(getattr(item, 'WeightTag1'))
        dic['Tag2'].append(getattr(item, 'WeightTag2'))
        dic['Tag3'].append(getattr(item, 'WeightTag3'))
        dic['Tag4'].append(getattr(item, 'WeightTag4'))
    dic['sample'] = '1'
    dic['startTime'] = getattr(result[0], 'Timestamp').strftime('%Y-%m-%d %H:%M:%S')
    dic['faultTime'] = dict1['faultTime']
    dic['endTime'] = getattr(result[-1], 'Timestamp').strftime('%Y-%m-%d %H:%M:%S')
    return jsonify(dic)

# # Logout路由
# @UE.route('/logout')
# @login_required
# def logout():
#     logout_user()
#     flash('您已登出')
#     return redirect(url_for('main.index'))
