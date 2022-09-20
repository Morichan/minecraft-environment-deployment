#!/bin/bash -e

aws s3 sync s3://${BUCKET_NAME}/backup/latest/world/ world/

# Change settings
sed -i "s/max-tick-time=[0-9]*/max-tick-time=-1/g" server.properties

if [ "${MINECRAFT_WHITE_LIST}" != '' ]; then
  sed -i "s/white-list=false/white-list=true/g" server.properties
  echo "\"${MINECRAFT_WHITE_LIST}\"" | \
    jq -r 'split(",") | [.[] | split(":") | {uuid: .[0], name: .[1]}]' > \
    whitelist.json

  cat whitelist.json | jq -c '{whitelist: .}'
fi


trap "./terminate_minecraft_server.sh" EXIT

java -Xmx1024M -Xms1024M -jar server.jar nogui

sleep infinity
