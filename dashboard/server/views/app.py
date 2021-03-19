from flask import Flask, render_template
from flask_restful import Resource, Api
from security import authenticate, identity
from user import  User ,UserRegister
# from create_app import app
# app = Flask(__name__, static_folder='../frontend/build', static_url_path='/')
# api = Api(app)
# app.config['SECRET_KEY'] = 'xyahsdf23423'

from flask import Blueprint, Response, jsonify

api_bp = Blueprint("api", __name__, url_prefix="/api")
@api_bp.route("/test", methods=["GET"])
def index():
    """Defines the main website view"""
    return jsonify("hello12 world")
    

# @app.route('/')
# def index():
#     return app.send_static_file('index.html')
# @app.route('/')
# def index():
#     return app.send_static_file('index.html')
# class Data(Resource):
#     def get(self,name):
#         return {'student':name}
# api.add_resource(Data,'/d/<string:name>')

# api.add_resource(UserRegister, '/register')
# http://127.0.0.1:8000/d/nizami