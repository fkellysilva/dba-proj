# routes.py
# Add your route definitions here in the future. 

from flask import Blueprint
from controllers.product_controller import ProductController

# Create blueprint
api = Blueprint('api', __name__)

# Product routes
@api.route('/produtos', methods=['GET'])
@api.route('/produtos/', methods=['GET'])
def get_all_products():
    return ProductController.get_all_products()

@api.route('/produtos/<code>', methods=['GET'])
@api.route('/produtos/<code>/', methods=['GET'])
def get_product_by_code(code):
    return ProductController.get_product_by_code(code)

@api.route('/produtos/<code>/comentarios', methods=['GET'])
@api.route('/produtos/<code>/comentarios/', methods=['GET'])
def get_product_comments(code):
    return ProductController.get_product_comments(code)

@api.route('/produtos/<code>/imagens', methods=['GET'])
@api.route('/produtos/<code>/imagens/', methods=['GET'])
def get_product_images(code):
    return ProductController.get_product_images(code) 