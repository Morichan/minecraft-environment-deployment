#!/bin/bash

MINECRAFT_PID=$(ps aux | grep -e "[0-9:]*\sjava" | awk '{ print $2 }')

echo "Kill minecraft-server."
kill ${MINECRAFT_PID}
wait ${MINECRAFT_PID}
echo "Done minecraft-server."

aws s3 sync world/ s3://${BUCKET_NAME}/backup/latest/world/

# Wait to push log to CloudWatch Logs
sleep 10
