from pymongo import MongoClient

def get_mongodb_connection():
    client = MongoClient(
        host="mongodb",
        port=27017,
        username="admin",
        password="adminpassword"
    )
    return client['varejo_db']

def get_comentarios_collection():
    db = get_mongodb_connection()
    return db['comentarios']

def get_imagens_collection():
    db = get_mongodb_connection()
    return db['imagens_produtos'] 