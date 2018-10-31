from flask import render_template, redirect, request, url_for, flash, jsonify
from flask_login import login_user, current_user
from flask_login import logout_user, login_required
from . import user
from ..models import User, Factory, Equipment, Role
from .forms import LoginForm, RegisterForm, UserEdit
from .. import db
from ..decorators import admin_required
import logging
import json
# from ..emailDev import send_email
logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
handler = logging.FileHandler("/var/www/weightingsystem/logs/flask.log")
handler.setLevel(level=logging.INFO)
logger.addHandler(handler)


@user.route('/login', methods=['GET', 'POST'])
def login():
    # 新增路由函数
    form = LoginForm()
    if form.validate_on_submit():
        user = User.query.filter_by(username=form.username.data).first()
        if user is not None and user.verify_password(form.password.data):
            login_user(user, form.remember_me.data)
            print(current_user.confirmed, request.args.get('next'))
            return redirect(request.args.get('next') or url_for('main.index'))
        flash('用户名或密码错误')
    return render_template('/user/login.html', form=form)


@user.route('/loginUE', methods=['GET', 'POST'])
def loginUE():
    res = {}
    if request.method == 'POST':
        a = request.get_data()
        a = str(a, encoding="utf-8")
        a = a.replace('\\n', '')
        a = a.replace('\\t', '')
        a = a.replace('\\r', '')
        dict1 = json.loads(a)
        logger.info('param', dict1['username'], dict1['password'])
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


# Logout路由
@user.route('/logout')
@login_required
def logout():
    logout_user()
    flash('您已登出')
    return redirect(url_for('main.index'))


# 新增发送邮件授权
@user.route('/register', methods=['GET', 'POST'])
def register():
    form = RegisterForm()
    if form.validate_on_submit():
        if User.query.filter_by(username=form.username.data).first():
            flash('工号已被注册')
            pass
        else:
            factory = Factory.query.filter_by(id=form.factoryID.data).first()
            if factory is None:
                flash('该工厂不存在，请先创建工厂')
            else:
                user = User(username=form.username.data,
                            password=form.password.data,
                            factoryID=form.factoryID.data,
                            role_id=form.role.data)
                db.session.add(user)
                db.session.commit()
                return redirect(url_for('.login'))
    return render_template('/user/register.html', form=form)


@user.route('/manage', methods=['GET', 'POST'])
@login_required
def manage():
    if current_user.role_id >= 3:
        factory = current_user.factoryID
        UserList = User.query.filter_by(factoryID=factory).order_by(User.role_id).all()
        return render_template('/user/manage.html', UserList=UserList)
    else:
        flash("无该权限")
        return redirect(url_for('main.index'))


@user.route('/editUser/<string:username>', methods=['GET', 'POST'])
@login_required
@admin_required
def editUser(username):
    if current_user.role_id >= 3:
        user = User.query.filter_by(username=username).first()
        form = UserEdit()
        Equipment.__table__.name = current_user.factoryID + 'eqp'
        if form.validate_on_submit():
            user.username = form.username.data
            user.role_id = form.role.data
            user.factoryID = form.factoryID.data
            for item in form.EqpID.data.split(","):
                exist = db.session.query(Equipment).filter(Equipment.id == item).first()
                print(not exist)
                if not exist:
                    flash("设备输入错误，无" + item)
                    return redirect(url_for('user.editUser', username=username))
            user.EqpID = form.EqpID.data
            user.confirmed = form.confirmed.data
            db.session.add(user)
            db.session.commit()
            flash("用户资料更新成功")
            return redirect(url_for('user.manage'))
        form.username.data = user.username
        form.role.data = user.role_id
        form.factoryID.data = user.factoryID
        form.EqpID.data = user.EqpID
        form.confirmed.data = user.confirmed
        return render_template('/user/UserEdit.html', form=form)
    else:
        flash("无该权限")
        return redirect(url_for('main.index'))



# @user.route('/Eqpquery', methods=['GET', 'POST'])
# def Eqpquery():
#     facID = request.form.get("facID", "")
#     Equipment.__table__.name = facID + 'eqp'
#     eqpList = db.session.query(Equipment).all()
#     eqpdic = {}
#     if eqpList:
#         for i, eqp in zip(range(1, len(eqpList) + 1), eqpList):
#             eqpdic[str(i)] = eqp.id
#     else:
#         flash('请先添加设备！')
#         eqpdic['state'] = 0
#     return jsonify(eqpdic)


# 新增邮件confirm
# @user.route('/confirm/<token>')
# @login_required
# def confirm(token):
#     if current_user.confirmed:
#         return redirect(url_for('main.index'))
#     if current_user.confirm(token):
#         flash('you have confirmed your account')
#     else:
#         flash('you have not confirmed your account')
#     return redirect(url_for('main.index'))


# @user.before_app_request
# def before_request():
#     if current_user.is_userenticated and request.endpoint[:5] != 'user.' and not current_user.confirmed:
#         return redirect(url_for('user.unconfirmed'))


# @user.route('/unconfirmed')
# def unconfirmed():
#     if current_user.is_anonymous or current_user.confirmed:
#         return redirect(url_for('main.index'))
#     return render_template('user/unconfirmed.html')


# @user.route('/confirm')
# @login_required
# def resend_confirmation():
#     token = current_user.generate_confirmation_token()
#     send_email(current_user.email, 'Confirm your account',
#                'user/email/confirm', user=current_user, token=token)
#     flash('confirmation has been resent')
#     return redirect(url_for('main.index'))
