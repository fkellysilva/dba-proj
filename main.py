from flask import Flask
from routes import api
from flask_cors import CORS

app = Flask(__name__)
CORS(app, resources={
    r"/api/*": {
        "origins": "*",  # In production, replace with specific domain
        "methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS"],
        "allow_headers": ["Content-Type", "Authorization"]
    }
})

# Register blueprints
app.register_blueprint(api, url_prefix='/api')

@app.route('/')
def index():
    return {
        'status': 'ok',
        'message': 'API is running'
    }

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3333, debug=True) 