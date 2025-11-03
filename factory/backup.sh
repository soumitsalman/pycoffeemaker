S3_CREDENTIALS=""
S3_ENDPOINT=""
S3_BUCKET=""
PG_CONNECTION_STRING=""
LOCAL_DATAPATH=""
pg_dump --no-owner --no-acl --data-only -Fc $PG_CONNECTION_STRING > "$LOCAL_DATAPATH/catalog/backups/beansackdb_$(date +%Y%m%d_%H%M%S).dump"
~/go/bin/s5cmd --credentials-file=$S3_CREDENTIALS --endpoint-url=$S3_ENDPOINT sync $LOCAL_DATAPATH/catalog/backups/* s3://$S3_BUCKET/catalog/backups/
~/go/bin/s5cmd --credentials-file=$S3_CREDENTIALS --endpoint-url=$S3_ENDPOINT sync $LOCAL_DATAPATH/storage/* s3://$S3_BUCKET/storage/