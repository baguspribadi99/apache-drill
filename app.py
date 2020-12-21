from flask import Flask
from pyDrill import *

app = Flask(__name__)


@app.route("/")
def home():
    return get_all_countries_query()
