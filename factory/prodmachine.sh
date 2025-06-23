sudo apt update
sudo apt upgrade -y 
sudo apt-get install -y software-properties-common
sudo apt install -y python3 python3-venv python3-pip git
git clone https://www.github.com/soumitsalman/pycoffeemaker.git
cd pycoffeemaker/coffeemaker
git clone https://www.github.com/soumitsalman/pybeansack.git
cd ~/pycoffeemaker
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install -r pybeansack/requirements.txt
