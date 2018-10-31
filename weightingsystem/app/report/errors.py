#!/usr/bin/env python
# -*- coding: utf-8 -*-
from flask import render_template
from . import report


@report.app_errorhandler(404)
def page_not_found(e):
    return render_template('404.html'), 404


@report.app_errorhandler(500)
def internal_sever_error(e):
    return render_template('500.html'), 500


@report.app_errorhandler(403)
def no_permission(e):
    return render_template('403.html'), 403