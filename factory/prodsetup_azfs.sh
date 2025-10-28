sudo apt update
sudo apt install cifs-utils -y
curl -sL https://aka.ms/InstallAzureCLIDeb | sudo bash

az login

RESOURCE_GROUP_NAME="coffeemaker-rg"
STORAGE_ACCOUNT_NAME="coffeemakerstorage"
FILE_SHARE_NAME="beansack-prod"

# This command assumes you have logged in with az login
HTTP_ENDPOINT=$(az storage account show \
    --resource-group $RESOURCE_GROUP_NAME \
    --name $STORAGE_ACCOUNT_NAME \
    --query "primaryEndpoints.file" --output tsv | tr -d '"')
echo "HTTP Endpoint: $HTTP_ENDPOINT"

SMB_PATH=$(echo $HTTP_ENDPOINT | cut -c7-${#HTTP_ENDPOINT})
echo "SMB Path: $SMB_PATH"

FILE_HOST=$(echo $SMB_PATH | tr -d "/")
echo "File Host: $FILE_HOST"

nc -zvw3 $FILE_HOST 445

MNT_ROOT="mnt"
MNT_PATH="$MNT_ROOT/$STORAGE_ACCOUNT_NAME/$FILE_SHARE_NAME"

sudo mkdir -p $MNT_PATH

# Create a folder to store the credentials for this storage account and
# any other that you might set up.
CREDENTIAL_ROOT=".az"
sudo mkdir -p $CREDENTIAL_ROOT

# Get the storage account key for the indicated storage account.
STORAGE_ACCOUNT_KEY=$(az storage account keys list \
    --resource-group $RESOURCE_GROUP_NAME \
    --account-name $STORAGE_ACCOUNT_NAME \
    --query "[0].value" --output tsv | tr -d '"')
echo "Storage Account Key: $STORAGE_ACCOUNT_KEY"

# Create the credential file for this individual storage account
SMB_CREDENTIAL_FILE="$CREDENTIAL_ROOT/$STORAGE_ACCOUNT_NAME.cred"
echo "Creating SMB credential file at: $SMB_CREDENTIAL_FILE"

echo "username=$STORAGE_ACCOUNT_NAME" | sudo tee $SMB_CREDENTIAL_FILE > /dev/null
echo "password=$STORAGE_ACCOUNT_KEY" | sudo tee -a $SMB_CREDENTIAL_FILE > /dev/null

echo "Contents of SMB credential file:"
cat $SMB_CREDENTIAL_FILE

# Change permissions on the credential file so only root can read or modify the password file.
# sudo chmod 600 $SMB_CREDENTIAL_FILE

# This command assumes you have logged in with az login
SMB_PATH=$(echo $HTTP_ENDPOINT | cut -c7-${#HTTP_ENDPOINT})$FILE_SHARE_NAME
echo "SMB Path: $SMB_PATH"
# mount the dir
SMB_CREDENTIAL_FILE=$(realpath "$SMB_CREDENTIAL_FILE")
MNT_PATH=$(realpath "$MNT_PATH")
sudo mount -t cifs $SMB_PATH $MNT_PATH -o credentials=$SMB_CREDENTIAL_FILE,serverino,nosharesock,actimeo=30,mfsymlinks

# Add an entry to /etc/fstab to mount the share at boot time
echo "$SMB_PATH $MNT_PATH cifs _netdev,nofail,credentials=$SMB_CREDENTIAL_FILE,serverino,nosharesock,actimeo=30,mfsymlinks" | sudo tee -a /etc/fstab > /dev/null

sudo mount -a