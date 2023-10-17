from utils.utils import AppUtils
from configs.config import CONFIG
from flask import request
from flask_jwt_extended import create_access_token, verify_jwt_in_request
# from flask_jwt_extended import get_jwt_identity


class Auth:

    @classmethod
    def oauth_token(cls):
        """
        Get OAuth token for service calls
        """
        params = request.json
        key = params.get('key', None)
        secret = params.get('secret', None)
        token = ""
        if key == CONFIG['AUTH_KEY'] and secret == CONFIG['AUTH_SECRET']:
            token = create_access_token(identity={"role": "service_client", "uid": 0})
        else:
            return AppUtils.make_response([], 400, {"error": "Invalid Credentials"})
        return AppUtils.make_response([{"token": token}])


class AuthorizationMiddleware(object):

    @staticmethod
    def resolve(next_callback, root, info, **args):
        if not verify_jwt_in_request():
            return AppUtils.make_response([], 400, {"error": "Invalid Credentials"})
        return next_callback(root, info, **args)


# JWT Token
# https://flask-jwt-extended.readthedocs.io/en/stable/basic_usage/
# https://github.com/vimalloc/flask-jwt-extended/issues/404
