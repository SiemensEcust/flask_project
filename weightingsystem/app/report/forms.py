#!/usr/bin/env python
# -*- coding: utf-8 -*-
from flask_wtf import Form
from wtforms import StringField, SubmitField, PasswordField, BooleanField, TextField, SelectField, TextAreaField
from wtforms.validators import Required, Length, Email, Regexp, ValidationError


class Queryform(Form):
    startdate = StringField('起始时间：', validators=[Required()], render_kw={"placeholder": "year-month-day hour:month"})
    enddate = StringField('结束时间：', validators=[Required()], render_kw={"placeholder": "year-month-day hour:month"})
    eqp = StringField('设备名称：', validators=[Required()], render_kw={"placeholder": ""})
    submit = SubmitField('查询')


class Titleform(Form):
    Supplier = StringField('供应商：', validators=[Required()], render_kw={"placeholder": ""})
    SencerName = StringField('传感器位号：', validators=[Required()], render_kw={"placeholder": ""})
    Noload = StringField('各传感器无负载读数(mV)：', validators=[Required()], render_kw={"placeholder": ""})
    Emptyload = StringField('各传感器空载读数(mV)：', validators=[Required()], render_kw={"placeholder": ""})
    ExcV = StringField('激励电压(mV)：', validators=[Required()], render_kw={"placeholder": ""})
    Sen = StringField('灵敏度(mV/V)：', validators=[Required()], render_kw={"placeholder": ""})


class ReportHead(Form):
    reportDate = StringField('报告时间：')
    docNo = StringField('文件编号：')
    customer = StringField('客户名称：')
    location = StringField('客户位置：')


class eqpDetails(Form):
    EqpID = StringField('设备名称：')
    seriolNo = StringField('设备系列号：')
