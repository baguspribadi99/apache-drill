from flask import Flask, request
from pyDrill import *

app = Flask(__name__)


@app.route("/")
def home():
    return home_res()


@app.route("/country")
def detail():
    return get_detail_country_query(request.args.get("iso"))


@app.route("/countries")
def countries():
    return countries_res()
