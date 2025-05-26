docker compose up -d --build

docker compose exec python python scripts/migrate_products.py

docker compose exec python python scripts/etl_dw.py

docker compose exec python python scripts/olap_analysis.py
