sudo apt update
sudo apt upgrade -y 
sudo apt-get install -y software-properties-common
sudo apt install -y python3 python3-venv python3-pip git
git clone https://www.github.com/soumitsalman/pycoffeemaker.git
cd pycoffeemaker/coffeemaker
git clone https://www.github.com/soumitsalman/pybeansack.git
git clone https://www.github.com/soumitsalman/nlp.git
cd ~/pycoffeemaker
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -r coffeemaker/pybeansack/requirements.txt
pip install -r coffeemaker/nlp/requirements.txt

# sudo snap install go
# go install github.com/peak/s5cmd/v2@v2.3.0
# 1. run attach on ducklake with datapath
# 2. DELETE FROM DELETE FROM ducklake_metadata WHERE key='data_path'
# 3. dbcache init-db postgresql://foo:bar@postgresql/mydb

# crontab

# nohup /home/soumitsr/pycoffeemaker/.venv/bin/python3 /home/soumitsr/pycoffeemaker/run.py --mode COLLECTOR --batch_size 128 > /home/soumitsr/pycoffeemaker/collector.log 2>&1 &
# nohup /home/soumitsr/pycoffeemaker/.venv/bin/python3 /home/soumitsr/pycoffeemaker/run.py --mode INDEXER --batch_size 32 > /home/soumitsr/pycoffeemaker/indexer.log 2>&1 &
# nohup /home/soumitsr/pycoffeemaker/.venv/bin/python3 /home/soumitsr/pycoffeemaker/run.py --mode DIGESTOR --batch_size 8 > /home/soumitsr/pycoffeemaker/digestor.log 2>&1 &
# nohup /home/soumitsr/pycoffeemaker/.venv/bin/python3 /home/soumitsr/pycoffeemaker/run.py --mode COMPOSER > /home/soumitsr/pycoffeemaker/composer.log 2>&1 &
# nohup /home/soumitsr/pycoffeemaker/.venv/bin/python3 /home/soumitsr/pycoffeemaker/run.py --mode REFRESHER > /home/soumitsr/pycoffeemaker/composer.log 2>&1 &