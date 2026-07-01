set -e

MODE="gpu"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --mode)
            if [[ $# -lt 2 ]]; then
                echo "Error: --mode requires one of: io, cpu, gpu" >&2
                exit 1
            fi
            MODE="$2"
            shift 2
            ;;
        --mode=*)
            MODE="${1#*=}"
            shift
            ;;
        *)
            echo "Usage: $0 [--mode io|cpu|gpu]" >&2
            exit 1
            ;;
    esac
done

case "$MODE" in
    io|cpu|gpu)
        ;;
    *)
        echo "Error: invalid mode '$MODE'. Expected one of: io, cpu, gpu" >&2
        exit 1
        ;;
esac

echo "=== Updating package lists and installing dependencies ==="
sudo apt update && sudo apt upgrade -y
sudo apt install -y build-essential python3.12-venv python3-dev ninja-build cmake

echo "=== Installing AWS CLI ==="
sudo apt install -y unzip
curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip
unzip -q /tmp/awscliv2.zip -d /tmp
sudo /tmp/aws/install
rm -rf /tmp/aws /tmp/awscliv2.zip

echo "=== Download dependencies and make venv ==="
python3 -m venv .venv
source .venv/bin/activate
case "$MODE" in
    io)
        pip install -r requirements-io.txt
        ;;
    cpu)
        pip install -r requirements-cpu.txt
        ;;
    gpu)
        pip install -r requirements.txt
        ;;
esac
pip install -r pybeansack/requirements.txt
mkdir -p .cache/clscache .logs
# echo "=== Installing psql tools ==="

# sudo rm -f /etc/apt/sources.list.d/pgdg.list
# sudo apt-get update
# sudo apt-get install -y curl ca-certificates gnupg lsb-release
# sudo install -d -m 0755 /usr/share/keyrings
# curl -fsSL https://www.postgresql.org/media/keys/ACCC4CF8.asc | sudo gpg --dearmor -o /usr/share/keyrings/postgresql.gpg
# echo "deb [signed-by=/usr/share/keyrings/postgresql.gpg] http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" | sudo tee /etc/apt/sources.list.d/pgdg.list >/dev/null
# sudo apt-get update
# sudo apt-get install -y postgresql-client-17 postgresql-contrib-17
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