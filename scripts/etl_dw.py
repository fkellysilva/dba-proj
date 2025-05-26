import mysql.connector
import pandas as pd
from datetime import datetime, timedelta
import calendar
import os

# Database connection settings
SOURCE_CONFIG = {
    'host': 'mysql_db',
    'user': 'user',
    'password': 'userpassword',
    'database': 'VarejoBase'
}

DW_CONFIG = {
    'host': 'mysql_db',
    'user': 'user',
    'password': 'userpassword',
    'database': 'DW_Varejo'
}

def connect_to_db(config):
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor(dictionary=True)
    return conn, cursor

def load_dim_tempo(dw_conn, start_date, end_date):
    cursor = dw_conn.cursor()
    
    # Generate dates
    date_range = pd.date_range(start=start_date, end=end_date)
    
    for date in date_range:
        id_tempo = int(date.strftime('%Y%m%d'))
        dia = date.day
        mes = date.month
        ano = date.year
        trimestre = (mes - 1) // 3 + 1
        dia_semana = calendar.day_name[date.weekday()]
        
        cursor.execute("""
        INSERT IGNORE INTO dim_tempo 
        (id_tempo, data, dia, mes, ano, trimestre, dia_semana)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (id_tempo, date.date(), dia, mes, ano, trimestre, dia_semana))
    
    dw_conn.commit()

def load_dim_categoria(src_cursor, dw_conn):
    cursor = dw_conn.cursor()
    
    # Extract categories
    src_cursor.execute("SELECT * FROM categoria")
    categorias = src_cursor.fetchall()
    
    for cat in categorias:
        cursor.execute("""
        INSERT IGNORE INTO dim_categoria 
        (id_categoria, nome_categoria, descricao)
        VALUES (%s, %s, %s)
        """, (cat['id_categoria'], cat['nome_categoria'], cat.get('descricao')))
    
    dw_conn.commit()

def load_dim_produto(src_cursor, dw_conn):
    cursor = dw_conn.cursor()
    
    # Extract products with category information
    src_cursor.execute("""
        SELECT p.*, c.nome_categoria 
        FROM produto p
        LEFT JOIN categoria c ON p.id_categoria = c.id_categoria
    """)
    produtos = src_cursor.fetchall()
    
    for prod in produtos:
        cursor.execute("""
        INSERT IGNORE INTO dim_produto 
        (id_produto, nome_produto, marca, id_categoria, unidade_medida)
        VALUES (%s, %s, %s, %s, %s)
        """, (prod['id_produto'], prod['nome_produto'], prod['marca'], 
              prod['id_categoria'], prod['unidade_medida']))
    
    dw_conn.commit()

def load_dim_loja(src_cursor, dw_conn):
    cursor = dw_conn.cursor()
    
    src_cursor.execute("SELECT * FROM loja")
    lojas = src_cursor.fetchall()
    
    for loja in lojas:
        cursor.execute("""
        INSERT IGNORE INTO dim_loja 
        (id_loja, nome_loja, cidade, estado)
        VALUES (%s, %s, %s, %s)
        """, (loja['id_loja'], loja['nome_loja'], loja['cidade'], loja['estado']))
    
    dw_conn.commit()

def load_dim_cliente(src_cursor, dw_conn):
    cursor = dw_conn.cursor()
    
    src_cursor.execute("SELECT * FROM cliente")
    clientes = src_cursor.fetchall()
    
    for cliente in clientes:
        cursor.execute("""
        INSERT IGNORE INTO dim_cliente 
        (id_cliente, nome_cliente, cidade, estado)
        VALUES (%s, %s, %s, %s)
        """, (cliente['id_cliente'], cliente['nome'], 
              cliente['cidade'], cliente['estado']))
    
    dw_conn.commit()

def load_fato_vendas(src_cursor, dw_conn):
    cursor = dw_conn.cursor()
    
    # Extract sales data with all necessary information
    src_cursor.execute("""
        SELECT 
            v.id_venda,
            DATE(v.data_venda) as data_venda,
            iv.id_produto,
            v.id_loja,
            v.id_cliente,
            iv.quantidade,
            iv.preco_unitario * iv.quantidade as valor_total,
            iv.desconto as desconto_total,
            v.forma_pagamento
        FROM venda v
        JOIN item_venda iv ON v.id_venda = iv.id_venda
    """)
    vendas = src_cursor.fetchall()
    
    for venda in vendas:
        # Create time dimension key
        id_tempo = int(venda['data_venda'].strftime('%Y%m%d'))
        
        cursor.execute("""
        INSERT IGNORE INTO fato_vendas 
        (id_tempo, id_produto, id_loja, id_cliente, 
         quantidade, valor_total, desconto_total, forma_pagamento)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (id_tempo, venda['id_produto'], venda['id_loja'], 
              venda['id_cliente'], venda['quantidade'], venda['valor_total'],
              venda['desconto_total'], venda['forma_pagamento']))
    
    dw_conn.commit()

def load_fato_precos(src_cursor, dw_conn):
    cursor = dw_conn.cursor()
    
    # Extract price data from products, promotions and suppliers
    src_cursor.execute("""
        SELECT 
            p.id_produto,
            p.id_categoria,
            p.preco_atual as preco_normal,
            pp.preco_promocional,
            pf.preco_compra,
            CASE 
                WHEN pp.preco_promocional IS NOT NULL 
                THEN ((p.preco_atual - pp.preco_promocional) / p.preco_atual) * 100
                ELSE ((p.preco_atual - pf.preco_compra) / p.preco_atual) * 100
            END as margem_lucro,
            pp.id_promocao IS NOT NULL as em_promocao,
            CURDATE() as data_atual
        FROM produto p
        LEFT JOIN produto_promocao pp ON p.id_produto = pp.id_produto
        LEFT JOIN produto_fornecedor pf ON p.id_produto = pf.id_produto
        WHERE p.ativo = TRUE
    """)
    precos = src_cursor.fetchall()
    
    for preco in precos:
        # Get or create time dimension record
        data = preco['data_atual']
        id_tempo = int(data.strftime('%Y%m%d'))
        
        cursor.execute("""
        INSERT INTO fato_precos 
        (id_tempo, id_produto, id_categoria, preco_normal, 
         preco_promocional, preco_compra, margem_lucro, em_promocao)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            id_tempo,
            preco['id_produto'],
            preco['id_categoria'],
            preco['preco_normal'],
            preco['preco_promocional'],
            preco['preco_compra'],
            preco['margem_lucro'],
            preco['em_promocao']
        ))
    
    dw_conn.commit()

def load_fato_estoque(src_cursor, dw_conn):
    cursor = dw_conn.cursor()
    
    # Extract inventory data
    src_cursor.execute("""
        SELECT 
            e.*,
            CURDATE() as data_atual,
            COALESCE(
                (SELECT SUM(iv.quantidade) 
                 FROM item_venda iv 
                 JOIN venda v ON iv.id_venda = v.id_venda 
                 WHERE iv.id_produto = e.id_produto 
                 AND v.id_loja = e.id_loja 
                 AND v.data_venda >= DATE_SUB(CURDATE(), INTERVAL 30 DAY)
                ), 0) as vendas_30_dias
        FROM estoque e
    """)
    estoques = src_cursor.fetchall()
    
    for estoque in estoques:
        # Get or create time dimension record
        data = estoque['data_atual']
        id_tempo = int(data.strftime('%Y%m%d'))
        
        # Calculate days of inventory
        vendas_diarias = estoque['vendas_30_dias'] / 30
        dias_estoque = int(estoque['quantidade_atual'] / vendas_diarias) if vendas_diarias > 0 else 999
        
        # Determine inventory status
        if estoque['quantidade_atual'] <= estoque['quantidade_minima']:
            status = 'Crítico' if estoque['quantidade_atual'] == 0 else 'Baixo'
        elif estoque['quantidade_atual'] >= estoque['quantidade_maxima']:
            status = 'Excesso'
        else:
            status = 'Normal'
        
        cursor.execute("""
        INSERT INTO fato_estoque 
        (id_tempo, id_produto, id_loja, quantidade_atual, 
         quantidade_minima, quantidade_maxima, dias_estoque, status_estoque)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            id_tempo,
            estoque['id_produto'],
            estoque['id_loja'],
            estoque['quantidade_atual'],
            estoque['quantidade_minima'],
            estoque['quantidade_maxima'],
            dias_estoque,
            status
        ))
    
    dw_conn.commit()

def get_mysql_connection(database):
    return mysql.connector.connect(
        host="mysql_db",
        user=os.getenv('MYSQL_USER', 'user'),
        password=os.getenv('MYSQL_PASSWORD', 'userpassword'),
        database=database
    )

def etl_process():
    try:
        # Conexões com os bancos
        source_conn = get_mysql_connection("VarejoBase")
        dw_conn = get_mysql_connection("DW_Varejo")
        
        source_cursor = source_conn.cursor(dictionary=True)
        dw_cursor = dw_conn.cursor(dictionary=True)

        # ETL para dimensões
        print("Carregando dimensões...")
        load_dim_categoria(source_cursor, dw_conn)
        load_dim_produto(source_cursor, dw_conn)
        load_dim_loja(source_cursor, dw_conn)
        load_dim_cliente(source_cursor, dw_conn)
        
        # Carregar dimensão tempo com um período de 5 anos
        start_date = datetime(2020, 1, 1)
        end_date = datetime(2025, 12, 31)
        load_dim_tempo(dw_conn, start_date, end_date)

        # ETL para fatos
        print("Carregando fatos...")
        load_fato_vendas(source_cursor, dw_conn)
        load_fato_precos(source_cursor, dw_conn)
        load_fato_estoque(source_cursor, dw_conn)

        print("ETL concluído com sucesso!")

    except Exception as e:
        print(f"Error during ETL process: {str(e)}")
    finally:
        if 'source_cursor' in locals():
            source_cursor.close()
        if 'source_conn' in locals():
            source_conn.close()
        if 'dw_cursor' in locals():
            dw_cursor.close()
        if 'dw_conn' in locals():
            dw_conn.close()

if __name__ == "__main__":
    etl_process() 