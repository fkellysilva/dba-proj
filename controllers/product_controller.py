from flask import Blueprint, jsonify
import mysql.connector
from ZODB import DB
from ZODB.FileStorage import FileStorage
import os
from contextlib import contextmanager

product_bp = Blueprint('product', __name__)

def get_mysql_connection():
    return mysql.connector.connect(
        host="mysql_db",
        user="user",
        password="userpassword",
        database="VarejoBase"
    )

@contextmanager
def get_zodb_connection():
    os.makedirs('data', exist_ok=True)
    storage = FileStorage('data/products.fs')
    db = DB(storage)
    connection = db.open()
    try:
        yield connection
    finally:
        connection.close()
        db.close()
        storage.close()

@product_bp.route('/', methods=['GET'])
def listar_produtos():
    try:
        # Get MySQL products
        mysql_conn = get_mysql_connection()
        cursor = mysql_conn.cursor(dictionary=True)
        cursor.execute("SELECT * FROM produto")
        products = cursor.fetchall()
        
        return jsonify({
            'status': 'success',
            'data': products,
            'total': len(products)
        })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'mysql_conn' in locals():
            mysql_conn.close()

@product_bp.route('/<codigo_produto>', methods=['GET'])
def detalhes_produto(codigo_produto):
    try:
        # Get product details from ZODB
        with get_zodb_connection() as zodb_conn:
            root = zodb_conn.root()
            zodb_products = root.get('products', {})
            
            if codigo_produto not in zodb_products:
                return jsonify({
                    'status': 'error',
                    'message': 'Produto não encontrado'
                }), 404
            
            product = zodb_products[codigo_produto]
            
            return jsonify({
                'status': 'success',
                'data': product.to_dict()
            })
        
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500 