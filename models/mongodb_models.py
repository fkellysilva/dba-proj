from pymongo import MongoClient
import os

class MongoDBConnection:
    _instance = None

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            cls._instance = MongoClient(
                f"mongodb://{os.getenv('MONGO_INITDB_ROOT_USERNAME', 'admin')}:"
                f"{os.getenv('MONGO_INITDB_ROOT_PASSWORD', 'adminpassword')}@mongodb:27017"
            )
        return cls._instance

class ProductComment:
    @staticmethod
    def get_comments_by_product_code(product_code):
        client = MongoDBConnection.get_instance()
        db = client.varejo
        return list(db.product_comments.find(
            {"product_code": product_code},
            {"_id": 0}  # Exclude MongoDB _id from results
        ))

class ProductImage:
    @staticmethod
    def get_images_by_product_code(product_code):
        client = MongoDBConnection.get_instance()
        db = client.varejo
        return db.product_images.find_one(
            {"product_code": product_code},
            {"_id": 0}  # Exclude MongoDB _id from results
        ) 