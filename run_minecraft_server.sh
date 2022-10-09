#!/bin/bash -e

# Check for zip file
if [[ $(aws s3 ls s3://${BACKUP_BUCKET_NAME}/minecraft-server/latest/world.tar.gz | wc -c) -ne 0 ]]; then
  echo "Restore from s3://${BACKUP_BUCKET_NAME}/minecraft-server/latest/world.tar.gz file."
  aws s3 cp s3://${BACKUP_BUCKET_NAME}/minecraft-server/latest/world.tar.gz world.tar.gz
  tar xf world.tar.gz
else
  echo "[Deprecated]: Restore from s3://${BUCKET_NAME}/backup/latest/world/ directory."
  aws s3 sync s3://${BUCKET_NAME}/backup/latest/world/ world/
fi

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

java -Xmx${MINECRAFT_MEMORY}M -Xms${MINECRAFT_MEMORY}M -jar server.jar nogui

sleep infinity
