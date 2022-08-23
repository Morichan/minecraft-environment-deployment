#!/bin/bash -e

aws s3 sync s3://${BUCKET_NAME}/backup/latest/world/ world/

# Change settings
sed -i "s/max-tick-time=[0-9]*/max-tick-time=-1/g" server.properties


trap "./terminate_minecraft_server.sh" EXIT

java -Xmx1024M -Xms1024M -jar server.jar nogui

sleep infinity
