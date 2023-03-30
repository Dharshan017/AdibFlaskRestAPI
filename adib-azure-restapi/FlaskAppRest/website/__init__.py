from flask import Flask
from os import path
from flask_cors import CORS
import urllib.parse
from flask_sqlalchemy import SQLAlchemy
# from flask_marshmallow import Marshmallow

db = SQLAlchemy()
# ma = Marshmallow()

def create_app():
    app = Flask(__name__)
    CORS(app, supports_credentials=True)
    from .data_accumulator import views
    from .nbp_insights import insights
    app.register_blueprint(views, url_prefix='/')
    app.register_blueprint(insights, url_prefix='/insights')
    # mysql-db connection
    username='adib_transcation_root'
    password = urllib.parse.quote_plus('Crayon@Data123#')
    host='adib-transcation.mysql.database.azure.com'
    database_name='adib_transcations'
    app.config['SQLALCHEMY_DATABASE_URI'] = f"mysql+pymysql://{username}:{password}@{host}/{database_name}"
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    db.init_app(app)
    # ma.init_app(app)

    return app
