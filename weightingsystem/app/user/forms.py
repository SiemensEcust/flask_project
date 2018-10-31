from flask_wtf import Form
from wtforms import StringField, SubmitField, PasswordField, BooleanField, SelectField
from wtforms.validators import Required, Length, Email, EqualTo, Regexp
from wtforms import ValidationError
from ..models import User, Role, Factory


class LoginForm(Form):
    username = StringField('用户名', validators=[Required(), Length(1, 64)])
    password = PasswordField('密码', validators=[Required()])
    remember_me = BooleanField('记住密码')
    submit = SubmitField('登录')


class RegisterForm(Form):
    username = StringField('用户名', validators=[Required(), Length(1, 64)])
    # username = StringField('Username', validators=[Required(), Regexp('^[A-Za-z][A-Za-z]*$', 0, 'name must be letter')])
    password = PasswordField('密码',
                             validators=[Required(),
                                         EqualTo('confirm',
                                         message='密码错误')])
    confirm = PasswordField('确认密码', validators=[Required()])
    factoryID = SelectField('工厂号', coerce=str)
    role = SelectField('登录权限', coerce=int)
    # eqp = SelectField('负责设备', coerce=int, choices=[], validators=[Required()])
    submit = SubmitField('注册')

    def __init__(self, *args, **kwargs):
        super(RegisterForm, self).__init__(*args, **kwargs)
        factorys = [f.id for f in Factory.query.all()]
        self.factoryID.choices = [(factory, factory) for factory, factory in zip(factorys, factorys)]
        self.role.choices = [(role.id, role.name) for role in Role.query.order_by(Role.name).all()]

    def validate_ID(self, field):
        if User.query.filter_by(id=field.data).first():
            raise ValidationError('工号已被注册')

    # def validate_username(self, field):
    #     if User.query.filter_by(username=field.data).first():
    #         raise ValidationError('Username has been register')


class UserEdit(Form):
    username = StringField('用户名', validators=[Required(), Length(1, 64)])
    role = SelectField('登录权限', coerce=int)
    factoryID = SelectField('工厂号', coerce=str)
    EqpID = StringField('可操作设备', validators=[Required()])
    confirmed = BooleanField('确认授权')
    submit = SubmitField('确认修改')

    def __init__(self, *args, **kwargs):
        super(UserEdit, self).__init__(*args, **kwargs)
        factorys = [f.id for f in Factory.query.all()]
        self.factoryID.choices = [(factory, factory) for factory, factory in zip(factorys, factorys)]
        self.role.choices = [(role.id, role.name) for role in Role.query.order_by(Role.name).all()]