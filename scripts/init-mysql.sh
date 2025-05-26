#!/bin/bash
set -e

# Wait for MySQL to be ready
until mysql -u root -p"$MYSQL_ROOT_PASSWORD" -e "SELECT 1" >/dev/null 2>&1; do
    echo "Waiting for MySQL to be ready..."
    sleep 1
done

# Create both databases
mysql -u root -p"$MYSQL_ROOT_PASSWORD" <<-EOSQL
    CREATE DATABASE IF NOT EXISTS VarejoBase CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    CREATE DATABASE IF NOT EXISTS DW_Varejo CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
    
    -- Grant privileges to the user for both databases
    GRANT ALL PRIVILEGES ON VarejoBase.* TO '$MYSQL_USER'@'%';
    GRANT ALL PRIVILEGES ON DW_Varejo.* TO '$MYSQL_USER'@'%';
    FLUSH PRIVILEGES;
EOSQL

echo "Databases created successfully!" 