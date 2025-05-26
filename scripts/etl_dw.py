import mysql.connector
import pandas as pd
from datetime import datetime
import calendar

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
        (id_produto, nome_produto, marca, categoria, unidade_medida)
        VALUES (%s, %s, %s, %s, %s)
        """, (prod['id_produto'], prod['nome_produto'], prod['marca'], 
              prod['nome_categoria'], prod['unidade_medida']))
    
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

def main():
    start_time = datetime.now()
    print(f"Starting ETL process at {start_time}")

    try:
        # Connect to source and DW
        src_conn, src_cursor = connect_to_db(SOURCE_CONFIG)
        dw_conn, _ = connect_to_db(DW_CONFIG)

        # Load dimensions
        print("\nLoading time dimension...")
        load_dim_tempo(dw_conn, '2020-01-01', '2024-12-31')  # Adjust date range as needed

        print("\nLoading category dimension...")
        load_dim_categoria(src_cursor, dw_conn)

        print("\nLoading product dimension...")
        load_dim_produto(src_cursor, dw_conn)

        print("\nLoading store dimension...")
        load_dim_loja(src_cursor, dw_conn)

        print("\nLoading customer dimension...")
        load_dim_cliente(src_cursor, dw_conn)

        print("\nLoading sales fact table...")
        load_fato_vendas(src_cursor, dw_conn)

        end_time = datetime.now()
        duration = end_time - start_time
        print(f"\nETL Process Completed Successfully.")
        print(f"Total duration: {duration}")

    except Exception as e:
        print(f"Error during ETL process: {str(e)}")
    finally:
        # Close connections
        if 'src_conn' in locals():
            src_conn.close()
        if 'dw_conn' in locals():
            dw_conn.close()

if __name__ == '__main__':
    main() 