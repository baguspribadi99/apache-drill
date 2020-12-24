from flask import Flask, request
from flask_cors import CORS, cross_origin
from pyDrill import *

app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'


@app.route("/")
@cross_origin()
def home():
    return home_res()


@app.route("/country")
@cross_origin()
def detail():
    return get_detail_country_query(request.args.get("iso"))


@app.route("/countries")
@cross_origin()
def countries():
    return countries_res()


@app.route("/indonesia")
@cross_origin()
def indonesia():
    return coba_coba()
