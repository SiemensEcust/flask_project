#!/usr/bin/env python
# -*- coding: utf-8 -*-
from flask import Flask
from flask_bootstrap import Bootstrap
from flask_sqlalchemy import SQLAlchemy
from config import config
from flask_login import LoginManager
from flask_pushjack import FlaskAPNS

bootstrap = Bootstrap()
db = SQLAlchemy()
client = FlaskAPNS()
# 注册Flask-login
login_manager = LoginManager()
login_manager.session_protection = 'strong'
login_manager.login_view = 'main.login'


def creat_app(config_name):
    app = Flask(__name__)
    app.config.from_object(config[config_name])
    config[config_name].init_app(app)
    bootstrap.init_app(app)
    db.init_app(app)
    login_manager.init_app(app)
    client.init_app(app)
    app.config.update(dict(
        APNS_CERTIFICATE='/var/www/weightingsystem/apns/apns_pro_nokey.pem',
        APNS_ENABLED=True,
        APNS_SANDBOX=False,
        APNS_DEFAULT_ERROR_TIMEOUT = 10,
        APNS_DEFAULT_EXPIRATION_OFFSET = 2592000))
    # 附加路由和自定义错误页面,蓝本导入示例
    from .main import main as main_blueprint
    from .user import user as user_blueprint
    from .enginer import engineer as engineer_blueprint
    from .report import report as report_blueprint
    from .UE import UE as UE_blueprint
    app.register_blueprint(main_blueprint)
    app.register_blueprint(user_blueprint, url_prefix='/user')
    app.register_blueprint(engineer_blueprint, url_prefix='/engineer')
    app.register_blueprint(report_blueprint, url_prefix='/report')
    app.register_blueprint(UE_blueprint, url_prefix='/UE')
    return app
