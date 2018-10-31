#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import datetime
from severclassyibu import sever
server = sever()
server.ReadVal()
while True:
    print(datetime.datetime.now())
    time.sleep(10)
