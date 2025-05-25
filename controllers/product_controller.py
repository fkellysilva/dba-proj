from flask import Blueprint, jsonify
import mysql.connector
from ZODB import DB
from ZODB.FileStorage import FileStorage

product_bp = Blueprint('product', __name__)

def get_mysql_connection():
    return mysql.connector.connect(
        host="mysql_db",
        user="user",
        password="userpassword",
        database="VarejoBase"
    )

def get_zodb_connection():
    storage = FileStorage('products.fs')
    db = DB(storage)
    connection = db.open()
    return connection

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
        zodb_conn = get_zodb_connection()
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
    finally:
        if 'zodb_conn' in locals():
            zodb_conn.close() 