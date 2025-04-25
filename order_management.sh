#!/usr/bin/sh

## You may need to run this utility first:
## This will get rid of the Windows carriage returns
##     sed -i -e 's/\r$//' *sh
##     chmod 750 *sh
## Just a small script that will execute the python script.
## It is exits unexpectedly, it'll automatically restart it.
## Run it as: nohup ./order_management.sh &

while [ 1 ]
do
sleep 5
/usr/bin/python3 -u ./order_management.py >> order_management.log
done