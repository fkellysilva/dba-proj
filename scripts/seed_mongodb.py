import mysql.connector
import pymongo
import os
from datetime import datetime, timedelta
import random

# MySQL connection
mysql_config = {
    'host': 'mysql_db',
    'user': os.getenv('MYSQL_USER', 'user'),
    'password': os.getenv('MYSQL_PASSWORD', 'userpassword'),
    'database': 'VarejoBase'
}

# MongoDB connection
mongo_config = {
    'host': 'mongodb',
    'port': 27017,
    'username': os.getenv('MONGO_INITDB_ROOT_USERNAME', 'admin'),
    'password': os.getenv('MONGO_INITDB_ROOT_PASSWORD', 'adminpassword')
}

# Sample comments for random generation
sample_comments = [
    "Ótimo produto, superou minhas expectativas!",
    "Boa relação custo-benefício.",
    "Produto de qualidade, recomendo!",
    "Entrega rápida e produto bem embalado.",
    "Atendeu perfeitamente minhas necessidades.",
    "Design moderno e funcional.",
    "Excelente durabilidade.",
    "Produto conforme descrito.",
    "Muito satisfeito com a compra.",
    "Vale cada centavo investido."
]

# Sample user names for random generation
sample_users = [
    "João Silva",
    "Maria Santos",
    "Pedro Oliveira",
    "Ana Costa",
    "Carlos Souza",
    "Lucia Pereira",
    "Roberto Almeida",
    "Patricia Lima",
    "Fernando Santos",
    "Julia Rodrigues"
]

def get_mysql_connection():
    return mysql.connector.connect(**mysql_config)

def get_mongo_connection():
    client = pymongo.MongoClient(
        f"mongodb://{mongo_config['username']}:{mongo_config['password']}@{mongo_config['host']}:{mongo_config['port']}"
    )
    return client

def generate_random_date(start_date, end_date):
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    return start_date + timedelta(days=random_number_of_days)

def generate_product_image_url(product_code, category):
    return f"https://storage.googleapis.com/varejo-images/{category.lower()}/{product_code.lower()}.jpg"

def seed_mongodb():
    try:
        # Connect to MySQL
        mysql_conn = get_mysql_connection()
        mysql_cursor = mysql_conn.cursor(dictionary=True)
        
        # Connect to MongoDB
        mongo_client = get_mongo_connection()
        db = mongo_client.varejo
        
        # Clear existing collections
        db.product_comments.drop()
        db.product_images.drop()
        
        # Get products from MySQL
        mysql_cursor.execute("""
            SELECT p.codigo_produto as Codigo, 
                   p.nome_produto as Nome, 
                   c.nome_categoria as Categoria
            FROM produto p
            JOIN categoria c ON p.id_categoria = c.id_categoria
            WHERE p.ativo = TRUE
        """)
        products = mysql_cursor.fetchall()
        
        if not products:
            print("No products found in MySQL database. Make sure to populate the 'produto' table first.")
            return
        
        # Generate and insert data for each product
        start_date = datetime.now() - timedelta(days=365)
        end_date = datetime.now()
        
        for product in products:
            # Generate 3-7 comments for each product
            num_comments = random.randint(3, 7)
            comments = []
            
            for _ in range(num_comments):
                comment = {
                    'product_code': product['Codigo'],
                    'user_name': random.choice(sample_users),
                    'comment': random.choice(sample_comments),
                    'rating': random.randint(4, 5),  # Generating mostly positive reviews
                    'date': generate_random_date(start_date, end_date)
                }
                comments.append(comment)
            
            # Insert comments
            if comments:
                db.product_comments.insert_many(comments)
            
            # Generate and insert image data
            image_data = {
                'product_code': product['Codigo'],
                'product_name': product['Nome'],
                'main_image_url': generate_product_image_url(product['Codigo'], product['Categoria']),
                'thumbnail_url': generate_product_image_url(product['Codigo'], product['Categoria']) + '?size=thumb',
                'alt_text': f"Imagem do produto {product['Nome']}",
                'category': product['Categoria']
            }
            db.product_images.insert_one(image_data)
        
        # Create indexes
        db.product_comments.create_index('product_code')
        db.product_images.create_index('product_code', unique=True)
        
        # Print summary
        print(f"MongoDB seeded successfully!")
        print(f"Total products processed: {len(products)}")
        print(f"Total comments added: {db.product_comments.count_documents({})}")
        print(f"Total images added: {db.product_images.count_documents({})}")
        
    except Exception as e:
        print(f"Error seeding MongoDB: {str(e)}")
    
    finally:
        if 'mysql_conn' in locals():
            mysql_conn.close()
        if 'mongo_client' in locals():
            mongo_client.close()

if __name__ == "__main__":
    seed_mongodb() 