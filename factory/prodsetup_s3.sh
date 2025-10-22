sudo snap install go --classic
go install github.com/peak/s5cmd/v2@v2.3.0

mkdir -p .aws
AWS_CREDENTIAL_FILE=".aws/credentials_prod"

echo "[default]" | sudo tee $AWS_CREDENTIAL_FILE > /dev/null
echo "aws_access_key_id=$S3_ACCESS_KEY_ID" | sudo tee -a $AWS_CREDENTIAL_FILE > /dev/null
echo "aws_secret_access_key=$S3_SECRET_ACCESS_KEY" | sudo tee -a $AWS_CREDENTIAL_FILE > /dev/null
