#!/bin/bash
yum -y install wget
pip install boto3 flask
wget https://s3.eu-central-1.amazonaws.com/unicorn-server-code/server.py
chmod +x server.py
touch /var/run/gameday.pid
touch /var/log/gameday.log
nohup python /server.py 85c25bd823 https://dashboard.cash4code.net/score >/var/log/gameday.log 2>&1 & echo \$! > /var/run/gameday.pid
