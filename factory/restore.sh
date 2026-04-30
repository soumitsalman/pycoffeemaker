mkdir -p .cache

echo "=== Restoring ZVEC Classification Cache ==="
~/go/bin/s5cmd --endpoint-url https://t3.storage.dev --credentials-file ~/.aws/credentials cp s3://cafecito-archives-new/processingcache/clsstore/clsstore_backup.tar.gz .cache/
tar -xzf .cache/clsstore_backup.tar.gz
rm .cache/clsstore_backup.tar.gz

echo "=== Restoring PG State Cache ==="
~/go/bin/s5cmd --endpoint-url https://t3.storage.dev --credentials-file ~/.aws/credentials cp s3://cafecito-archives-new/processingcache/statestore/statestore.dump .cache/

sudo systemctl stop postgresql@17-main
sudo rm -rf /var/lib/postgresql/17/main
sudo mkdir -p /var/lib/postgresql/17
sudo tar -xzf .cache/statestore_backup.tar.gz -C /
sudo chown -R postgres:postgres /var/lib/postgresql/17/main
sudo chmod 700 /var/lib/postgresql/17/main
sudo systemctl start postgresql@17-main

rm .cache/statestore.dump
