sudo apt-get update
sudo apt-get install -y fuse libfuse-dev

curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
sudo apt install -y unzip
unzip awscliv2.zip
sudo ./aws/install

S3_ACCESS_KEY_ID=...
S3_SECRET_ACCESS_KEY=...

mkdir -p ~/.aws
cat > ~/.aws/credentials << EOF
[default]
aws_access_key_id=$S3_ACCESS_KEY_ID
aws_secret_access_key=$S3_SECRET_ACCESS_KEY
endpoint_url=https://t3.storage.dev
EOF

wget https://github.com/tigrisdata/tigrisfs/releases/download/v1.2.1/tigrisfs_1.2.1_linux_amd64.deb
sudo dpkg -i tigrisfs_1.2.1_linux_amd64.deb

BUCKET=<bucket>

systemctl --user start tigrisfs@$BUCKET
systemctl --user start tigrisfs@$BUCKET

MOUNT_POINT=~/.mount/tigrisfs

mkdir -p $MOUNT_POINT
sudo chown $USER:$USER $MOUNT_POINT
tigrisfs $BUCKET $MOUNT_POINT