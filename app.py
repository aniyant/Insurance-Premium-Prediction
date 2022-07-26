from flask import Flask
from insurance.exception import InsuranceException
from insurance.logger import logging
import os,sys

app = Flask(__name__)

@app.route('/',methods=['GET','POST'])
def index():
    try:
        logging.info("app is running")
        return "app is running voila:"

    except Exception as e:
        raise InsuranceException(e,sys) from e


if __name__ == "__main__":
    app.run()