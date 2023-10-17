#!/usr/bin/env python
from configs.logger import log
from datetime import timedelta
from flask import Flask
from configs.config import CONFIG
from flask_jwt_extended import JWTManager
from routes.routes import Routes

app = Flask(__name__)
app.debug = CONFIG['DEBUG']

# JWT token configuration
app.config["JWT_SECRET_KEY"] = CONFIG['JWT_SECRET']
app.config["JWT_ACCESS_TOKEN_EXPIRES"] = timedelta(minutes=CONFIG['JWT_EXPIRY_MINS'])
jwt = JWTManager(app)

# Routes
Routes.config(app)

# @app.teardown_appcontext
# def shutdown_session(exception=None):
#     DBConnection().get_session().remove()
#     # pass

# use_reloader=False in options
if __name__ == "__main__":
    log.info('Starting service')
    app.run(port=CONFIG['APP_PORT'])
