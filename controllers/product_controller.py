from flask import jsonify
from models.product import Product
from models.mongodb_models import ProductComment, ProductImage

class ProductController:
    @staticmethod
    def get_all_products():
        try:
            products = Product.get_all()
            if not products:
                return jsonify({"message": "No products found", "data": []}), 200
            return jsonify({"message": "Products retrieved successfully", "data": products}), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @staticmethod
    def get_product_by_code(code):
        try:
            product = Product.get_by_code(code)
            if product:
                return jsonify({"message": "Product found", "data": product}), 200
            return jsonify({"error": "Product not found"}), 404
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @staticmethod
    def get_product_comments(code):
        try:
            # First check if product exists
            product = Product.get_by_code(code)
            if not product:
                return jsonify({"error": "Product not found"}), 404

            # Get comments from MongoDB
            comments = ProductComment.get_comments_by_product_code(code)
            return jsonify({
                "message": "Comments retrieved successfully",
                "data": {
                    "product_code": code,
                    "total_comments": len(comments),
                    "comments": comments
                }
            }), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500

    @staticmethod
    def get_product_images(code):
        try:
            # First check if product exists
            product = Product.get_by_code(code)
            if not product:
                return jsonify({"error": "Product not found"}), 404

            # Get images from MongoDB
            images = ProductImage.get_images_by_product_code(code)
            if not images:
                return jsonify({"error": "No images found for this product"}), 404

            return jsonify({
                "message": "Images retrieved successfully",
                "data": images
            }), 200
        except Exception as e:
            return jsonify({"error": str(e)}), 500 