#!/bin/bash

MINECRAFT_PID=$(ps aux | grep -e "[0-9:]*\sjava" | awk '{ print $2 }')

echo "Kill minecraft-server."
kill ${MINECRAFT_PID}
wait ${MINECRAFT_PID}
echo "Done minecraft-server."

tar cfa world.tar.gz world/ --warning=no-file-changed
aws s3 cp world.tar.gz s3://${BACKUP_BUCKET_NAME}/minecraft-server/latest/world.tar.gz

aws s3 sync crash-reports/ s3://${BUCKET_NAME}/backup/latest/crash-reports/

# Wait to push log to CloudWatch Logs
sleep 10
