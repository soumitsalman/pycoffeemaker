sudo apt update
sudo apt upgrade -y 

# RUNTIME
sudo apt-get install -y software-properties-common
sudo apt install -y build-essential gcc g++ cmake python3 python3-venv python3-pip git

# DOCKER FOR POSTGRESQL
sudo apt-get install ca-certificates -y
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "${UBUNTU_CODENAME:-$VERSION_CODENAME}") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt-get update
sudo apt-get install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin -y

# DUCKLAKE/DUCKDB
curl https://install.duckdb.org | sh
echo "export PATH='/home/soumitsr/.duckdb/cli/latest':\$PATH" >> ~/.bashrc

# CODE
cd ~
git clone https://www.github.com/soumitsalman/pycoffeemaker.git
cd ~/pycoffeemaker/coffeemaker
git clone https://www.github.com/soumitsalman/pybeansack.git
git clone https://www.github.com/soumitsalman/nlp.git
cd ~/pycoffeemaker
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -r coffeemaker/pybeansack/requirements.txt

# 1. run attach on ducklake with datapath
# 2. DELETE FROM DELETE FROM ducklake_metadata WHERE key='data_path'
# 3. dbcache init-db postgresql://foo:bar@postgresql/mydb

# crontab

# docker run -d -p 5432:5432 --env-file ~/pycoffeemaker/.env -v ~/.beansack/catalog/data:/var/lib/postgresql/data postgres:17-alpine
# nohup ~/pycoffeemaker/.venv/bin/python3 ~/pycoffeemaker/run.py --mode COLLECTOR --batch_size 128 > ~/pycoffeemaker/collector.log 2>&1 &
# nohup ~/pycoffeemaker/.venv/bin/python3 ~/pycoffeemaker/run.py --mode INDEXER --batch_size 32 > ~/pycoffeemaker/indexer.log 2>&1 &
# nohup ~/pycoffeemaker/.venv/bin/python3 ~/pycoffeemaker/run.py --mode DIGESTOR --batch_size 8 > ~/pycoffeemaker/digestor.log 2>&1 &
# nohup ~/pycoffeemaker/.venv/bin/python3 ~/pycoffeemaker/run.py --mode COMPOSER > ~/pycoffeemaker/composer.log 2>&1 &
# nohup ~/pycoffeemaker/.venv/bin/python3 ~/pycoffeemaker/run.py --mode REFRESHER > ~/pycoffeemaker/composer.log 2>&1 &
# bash ~/pycoffeemaker/.backup.sh