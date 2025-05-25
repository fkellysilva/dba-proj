from flask import Flask, jsonify
from controllers.main_controller import main_bp
from controllers.product_controller import product_bp

app = Flask(__name__)

# Register blueprints
app.register_blueprint(main_bp)
app.register_blueprint(product_bp, url_prefix='/api/produtos')

@app.route('/')
def home():
    return jsonify({'message': 'Welcome to the Flask API!'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3333, debug=True) 