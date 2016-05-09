#!/usr/bin/python
# -*- coding: utf-8 -*-
import json
import sqlite3

import sys

reload(sys)
sys.setdefaultencoding("unicode-escape")
conn = sqlite3.connect('sqlite3.db')
cur = conn.cursor()
cur.execute('select * from reports')
with open('reports.json', 'w') as file:
    r = [dict((cur.description[i][0], str(value).encode('utf-8')) \
              for i, value in enumerate(row)) for row in cur.fetchall()]
    json.dump(r, file, indent=True, ensure_ascii=False, sort_keys=True)