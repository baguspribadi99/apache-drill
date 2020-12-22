from flask import Flask, request
from pyDrill import *

app = Flask(__name__)


@app.route("/")
def home():
    return get_all_countries_query()

@app.route("/country")
def detail():
    return get_detail_country_query(request.args.get("country"))