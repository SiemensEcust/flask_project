#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from app import creat_app, db
from app.models import Role, User, Factory, Equipment, Supplier, countState, Thread, FaultList, Operation, NewVal, FaultMsg
from flask_script import Manager, Shell
from gevent import monkey
monkey.patch_all()

app = creat_app(os.getenv('FLASK_CONFIG') or 'default')
manager = Manager(app)


def make_shell_context():
    return dict(app=app, db=db, Role=Role, User=User, Factory=Factory,
                Equipment=Equipment, Supplier=Supplier, countState=countState,
                Thread=Thread, FaultList=FaultList, Operation=Operation,
                NewVal=NewVal, FaultMsg=FaultMsg)


manager.add_command("shell", Shell(make_context=make_shell_context))


@manager.command
def test():
    import unittest
    tests = unittest.TestLoader().discover('tests')
    unittest.TextTestRunner(verbosity=2).run(tests)


if __name__ == '__main__':
    manager.run()
