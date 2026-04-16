set -e

echo "=== Installing PostgreSQL and pgvector ==="

sudo apt-get install -y postgresql-17 postgresql-client-17 postgresql-contrib-17 postgresql-17-pgvector

# Enable and start PostgreSQL on boot
sudo systemctl enable postgresql
sudo systemctl start postgresql

# Setup database (CUSTOMIZE name/password if needed)
echo "=== Setting up database and pgvector extension ==="
sudo -u postgres psql -c "ALTER USER postgres PASSWORD 'local';" 2>/dev/null || true
sudo -u postgres psql -c "CREATE DATABASE statestore;" 2>/dev/null || true
sudo -u postgres psql -c "CREATE DATABASE clsstore;" 2>/dev/null || true
sudo -u postgres psql -d clsstore -c "CREATE EXTENSION IF NOT EXISTS vector;" || true

# Allow password authentication for postgres user from localhost
echo "=== Configuring authentication for postgres user ==="
sudo sed -i 's/^local\s\+all\s\+postgres\s\+peer/local all postgres md5/' /etc/postgresql/17/main/pg_hba.conf

# Reload PostgreSQL configuration to apply changes
sudo systemctl restart postgresql

echo "Authentication configured. You can now log in with: psql -h localhost -U postgres -d statestore"

echo "PostgreSQL + pgvector is installed and will start on boot."