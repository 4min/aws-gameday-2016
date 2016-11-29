#!/bin/bash
yum -y install wget git
pip install boto3 flask
wget https://raw.githubusercontent.com/4min/aws-gameday-2016/master/server_basic_working.py
chmod +x server.py
touch /var/run/gameday.pid
touch /var/log/gameday.log
nohup python /server_basic_working.py 85c25bd823 https://dashboard.cash4code.net/score >/var/log/gameday.log 2>&1 & echo \$! > /var/run/gameday.pid
