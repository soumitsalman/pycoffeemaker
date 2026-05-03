set -e

echo "=== Updating package lists and installing dependencies ==="
sudo apt update && sudo apt upgrade -y
sudo apt install -y build-essential python3.12-venv python3.12-dev

echo "=== Installing Go and s5cmd ==="
sudo snap install go --classic
go install github.com/peak/s5cmd/v2@latest

echo "=== Installing psql tools ==="

sudo rm -f /etc/apt/sources.list.d/pgdg.list
sudo apt-get update
sudo apt-get install -y curl ca-certificates gnupg lsb-release
sudo install -d -m 0755 /usr/share/keyrings
curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo gpg --dearmor -o /usr/share/keyrings/postgresql.gpg
echo "deb [signed-by=/usr/share/keyrings/postgresql.gpg] http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" | sudo tee /etc/apt/sources.list.d/pgdg.list >/dev/null
sudo apt-get update
sudo apt-get install -y postgresql-client-17 postgresql-contrib-17
# sudo apt-get install -y postgresql-17 postgresql-client-17 postgresql-contrib-17 postgresql-17-pgvector

# # Enable and start PostgreSQL on boot
# sudo systemctl enable postgresql
# sudo systemctl start postgresql

# # Setup database (CUSTOMIZE name/password if needed)
# echo "=== Setting up database and pgvector extension ==="
# sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'local';" 2>/dev/null || true
# sudo -u postgres psql -c "CREATE DATABASE statestore;" 2>/dev/null || true
# sudo -u postgres psql -c "CREATE DATABASE clsstore;" 2>/dev/null || true
# sudo -u postgres psql -d clsstore -c "CREATE EXTENSION IF NOT EXISTS vector;" || true

# # Allow password authentication for postgres user from localhost
# echo "=== Configuring authentication for postgres user ==="
# sudo sed -i 's/^local\s\+all\s\+postgres\s\+peer/local all postgres md5/' /etc/postgresql/17/main/pg_hba.conf

# # Reload PostgreSQL configuration to apply changes
# sudo systemctl restart postgresql

# echo "Authentication configured. You can now log in with: psql -h localhost -U postgres -d statestore"
# echo "PostgreSQL + pgvector is installed and will start on boot."