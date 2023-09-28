from flask import Flask
from flask_cors import CORS

app = Flask(__name__)

# Konfigurasi CORS untuk mengizinkan permintaan dari 'http://localhost:3000'
# CORS(app, resources={r"/api/*": {"origins": "http://localhost:3000"}})
CORS(app, resources={r"/api/*": {"origins": "*"}})

# Impor blueprint `api` dari file `routes`
from app.routes import api

# Daftarkan blueprint `api`
app.register_blueprint(api)

if __name__ == "__main__":
    app.run(debug=True)
