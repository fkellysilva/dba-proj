from models.base_model import BaseModel
import json
from datetime import datetime
import mysql.connector
import os
from ZODB import DB
from ZODB.FileStorage import FileStorage
from contextlib import contextmanager

class MySQLProduct:
    def __init__(self, id_produto, codigo_produto, nome_produto, descricao, 
                 id_categoria, marca, preco_atual, unidade_medida, ativo):
        self.id_produto = id_produto
        self.codigo_produto = codigo_produto
        self.nome_produto = nome_produto
        self.descricao = descricao
        self.id_categoria = id_categoria
        self.marca = marca
        self.preco_atual = preco_atual
        self.unidade_medida = unidade_medida
        self.ativo = ativo

    def to_dict(self):
        return {
            'id_produto': self.id_produto,
            'codigo_produto': self.codigo_produto,
            'nome_produto': self.nome_produto,
            'descricao': self.descricao,
            'id_categoria': self.id_categoria,
            'marca': self.marca,
            'preco_atual': float(self.preco_atual),
            'unidade_medida': self.unidade_medida,
            'ativo': bool(self.ativo)
        }

class ZODBProduct(BaseModel):
    def __init__(self, mysql_product=None):
        super().__init__()
        if mysql_product:
            self.id_produto = mysql_product.id_produto
            self.codigo_produto = mysql_product.codigo_produto
            self.nome_produto = mysql_product.nome_produto
            self.descricao = mysql_product.descricao
            self.id_categoria = mysql_product.id_categoria
            self.marca = mysql_product.marca
            self.preco_atual = mysql_product.preco_atual
            self.unidade_medida = mysql_product.unidade_medida
            self.ativo = mysql_product.ativo
            self.caracteristicas = {}  # Free JSON field
            self.created_at = datetime.now()
            self.updated_at = datetime.now()
        else:
            self.id_produto = None
            self.codigo_produto = None
            self.nome_produto = None
            self.descricao = None
            self.id_categoria = None
            self.marca = None
            self.preco_atual = None
            self.unidade_medida = None
            self.ativo = True
            self.caracteristicas = {}
            self.created_at = datetime.now()
            self.updated_at = datetime.now()

    def to_dict(self):
        return {
            'id_produto': self.id_produto,
            'codigo_produto': self.codigo_produto,
            'nome_produto': self.nome_produto,
            'descricao': self.descricao,
            'id_categoria': self.id_categoria,
            'marca': self.marca,
            'preco_atual': float(self.preco_atual) if self.preco_atual else None,
            'unidade_medida': self.unidade_medida,
            'ativo': bool(self.ativo),
            'caracteristicas': self.caracteristicas,
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }

class Product:
    @staticmethod
    def get_mysql_connection():
        return mysql.connector.connect(
            host="mysql_db",
            user=os.getenv('MYSQL_USER', 'user'),
            password=os.getenv('MYSQL_PASSWORD', 'userpassword'),
            database="VarejoBase"
        )

    @staticmethod
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

    @classmethod
    def get_all(cls):
        try:
            conn = cls.get_mysql_connection()
            cursor = conn.cursor(dictionary=True)
            
            cursor.execute("""
                SELECT p.*, c.nome_categoria as categoria_nome
                FROM produto p
                LEFT JOIN categoria c ON p.id_categoria = c.id_categoria
                WHERE p.ativo = TRUE
            """)
            
            products = cursor.fetchall()
            return products
            
        except Exception as e:
            raise Exception(f"Error fetching products: {str(e)}")
        
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close()

    @classmethod
    def get_by_code(cls, code):
        try:
            # First try to get from ZODB for detailed info
            with cls.get_zodb_connection() as connection:
                root = connection.root()
                products = root.get('products', {})
                
                if code in products:
                    return products[code].to_dict()
            
            # If not in ZODB, get basic info from MySQL
            conn = cls.get_mysql_connection()
            cursor = conn.cursor(dictionary=True)
            
            cursor.execute("""
                SELECT p.*, c.nome_categoria as categoria_nome
                FROM produto p
                LEFT JOIN categoria c ON p.id_categoria = c.id_categoria
                WHERE p.codigo_produto = %s AND p.ativo = TRUE
            """, (code,))
            
            product = cursor.fetchone()
            return product
            
        except Exception as e:
            raise Exception(f"Error fetching product {code}: {str(e)}")
        
        finally:
            if 'cursor' in locals():
                cursor.close()
            if 'conn' in locals():
                conn.close() 