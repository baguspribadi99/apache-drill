from flask import Flask
from pyDrill import *

app = Flask(__name__)


@app.route("/")
def home():
    return test_query()
