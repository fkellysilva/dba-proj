import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import mysql.connector
from ZODB import DB
from ZODB.FileStorage import FileStorage
import transaction
from models.product import MySQLProduct, ZODBProduct

def get_mysql_connection():
    return mysql.connector.connect(
        host="mysql_db",  # Docker container name
        user="user",
        password="userpassword",
        database="VarejoBase"
    )

def migrate_products():
    os.makedirs('data', exist_ok=True)
    storage = FileStorage('data/products.fs')
    
    # Connect to MySQL
    mysql_conn = get_mysql_connection()
    cursor = mysql_conn.cursor(dictionary=True)
    
    # Connect to ZODB
    db = DB(storage)
    connection = db.open()
    root = connection.root()
    
    try:
        # Create products container if it doesn't exist
        if 'products' not in root:
            root['products'] = {}
        
        # Fetch all products from MySQL
        cursor.execute("SELECT * FROM produto")
        mysql_products = cursor.fetchall()
        
        # Migrate each product
        for mysql_product_data in mysql_products:
            mysql_product = MySQLProduct(**mysql_product_data)
            zodb_product = ZODBProduct(mysql_product)
            
            # Store in ZODB using codigo_produto as key
            root['products'][mysql_product.codigo_produto] = zodb_product
        
        # Commit the transaction
        transaction.commit()
        print(f"Successfully migrated {len(mysql_products)} products to ZODB")
        
    except Exception as e:
        print(f"Error during migration: {str(e)}")
        transaction.abort()
    finally:
        cursor.close()
        mysql_conn.close()
        connection.close()
        db.close()

if __name__ == "__main__":
    migrate_products() 