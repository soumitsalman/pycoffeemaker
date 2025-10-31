sudo apt install -y pgcopydb

ORIGINAL_PG=""
LOCAL_PG=""

FILTERS_FILE=/tmp/pgcopydb_filters.ini
cat > $FILTERS_FILE << 'EOF'
[include-only-schema]
public
EOF
FILTERS="--filters $FILTERS_FILE"

rm -rf /tmp/pgcopydb
pgcopydb copy db --source $ORIGINAL_PG --target $LOCAL_PG --no-owner --drop-if-exists --not-consistent $FILTERS
