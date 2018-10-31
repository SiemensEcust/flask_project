#!/usr/bin/env python
# -*- coding: utf-8 -*-
from flask_wtf import Form
from wtforms import StringField, SubmitField, PasswordField, BooleanField, TextField, SelectField, TextAreaField
from wtforms.validators import Required, Length, Email, Regexp, ValidationError
from flask_login import current_user
from ..models import Equipment
from .. import db

class SetForm(Form):
    eqpName = StringField('设备号：', validators=[Required()], render_kw={"placeholder": ""})
    eqpLocation = StringField('设备所处工段：', validators=[Required()], render_kw={"placeholder": ""})
    supplier = StringField('供应商：', validators=[Required()], render_kw={"placeholder": ""})
    supplierInfo = StringField('供应商信息：', validators=[Required()], render_kw={"placeholder": ""})
    supplierCon = StringField('供应商联系方式：', validators=[Required()], render_kw={"placeholder": ""})
    Num = StringField('传感器数量：', validators=[Required()], render_kw={"placeholder": ""})
    Name = StringField('传感器位号：', validators=[Required()], render_kw={"placeholder": "不同位号间，以逗号分隔"})
    noLoad = StringField('传感器无负载读数：', validators=[Required()], render_kw={"placeholder": "数值间，以逗号分隔"})
    standardLoad = StringField('传感器标定读数：', validators=[Required()], render_kw={"placeholder": "数值间，以逗号分隔"})
    zeropoint = StringField('传感器零点：', validators=[Required()], render_kw={"placeholder": "数值间，以逗号分隔"})
    emptyLoad = StringField('传感器空载读数：', validators=[Required()], render_kw={"placeholder": "数值间，以逗号分隔"})    
    ExcV = StringField('激励电压(mV)：', validators=[Required()], render_kw={"placeholder": ""})
    Sensitivity = StringField('传感器灵敏度(mV)：', validators=[Required()], render_kw={"placeholder": ""})
    Resistance = StringField('传感器阻抗(Ω)：', validators=[Required()], render_kw={"placeholder": ""})
    Temp = StringField('记录时环境温度(℃)：', validators=[Required()], render_kw={"placeholder": ""})
    Wet = StringField('记录时环境湿度(%)：', validators=[Required()], render_kw={"placeholder": ""})
    submit = SubmitField('提交信息')

    def __init__(self, *args, **kwargs):
        super(SetForm, self).__init__(*args, **kwargs)


class OperationForm(Form):
    date = StringField('操作时间：', validators=[Required()], render_kw={"placeholder": "year-month-day hour:month"})
    operator= StringField('操作人：', validators=[Required()], render_kw={"placeholder": ""})
    eqp = SelectField('设备号', coerce=str)
    operate = TextAreaField('操作记录：', validators=[Required()], render_kw={"placeholder": "请填写操作记录"})
    standard = StringField('操作时标定值：', validators=[Required()], render_kw={"placeholder": ""})
    zero = StringField('操作时零点值：', validators=[Required()], render_kw={"placeholder": ""})
    submit = SubmitField('提交信息')

    def __init__(self, *args, **kwargs):
        super(OperationForm, self).__init__(*args, **kwargs)
        facID = current_user.factoryID
        Equipment.__table__.name = facID + "eqp"
        eqp = [e.id for e in db.session.query(Equipment).all()]
        self.eqp.choices = [(item, item) for item, item in zip(eqp, eqp)]


class HistoryForm(Form):
    startdate = StringField('起始时间：', validators=[Required()], render_kw={"placeholder": "year-month-day hour:month"})
    enddate = StringField('结束时间：', validators=[Required()], render_kw={"placeholder": "year-month-day hour:month"})
    eqp = SelectField('设备号', coerce=str)
    submit = SubmitField('查询')

    def __init__(self, *args, **kwargs):
        super(HistoryForm, self).__init__(*args, **kwargs)
        facID = current_user.factoryID
        Equipment.__table__.name = facID + "eqp"
        eqp = [e.id for e in db.session.query(Equipment).all()]
        self.eqp.choices = [(item, item) for item, item in zip(eqp, eqp)]
