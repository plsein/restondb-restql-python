from auth.auth import Auth, AuthorizationMiddleware
from db.database import DBConnection
from graph.schema import schema
from flask_graphql import GraphQLView
from flask_jwt_extended import jwt_required
from services.service import Service


class Routes:

    @classmethod
    def config(cls, app):

        # Index
        @app.route("/")
        def index():
            return Service.index()

        # GraphQL
        app.add_url_rule('/graphql',
                         view_func=GraphQLView.as_view('graphql',
                                                       schema=schema,
                                                       graphiql=True,
                                                       batch=True,
                                                       get_context=lambda: {'session': DBConnection().get_session()},
                                                       middleware=[AuthorizationMiddleware()]
                                                       )
                         )

        @app.route("/schema", methods=['GET'])
        def schema_graphql():
            return Service.graphqlSchema()

        @app.route("/schema.json", methods=['GET'])
        def schema_json():
            return Service.graphqlJsonSchema()

        @app.route("/oauth/token", methods=['POST'])
        def token():
            return Auth.oauth_token()

        @app.route("/api/select", methods=['POST'])
        @jwt_required()
        def select():
            return Service.select()

        @app.route("/api/add", methods=['POST'])
        @jwt_required()
        def add():
            return Service.add()

        @app.route("/api/edit", methods=['POST'])
        @jwt_required()
        def edit():
            return Service.edit()

        @app.route("/api/update", methods=['POST'])
        @jwt_required()
        def update():
            return Service.update()

        @app.route("/api/delete", methods=['POST'])
        @jwt_required()
        def delete():
            return Service.delete()

        @app.route("/api/remove", methods=['POST'])
        @jwt_required()
        def remove():
            return Service.remove()

        @app.route('/upload', methods=['POST'])
        @jwt_required()
        def upload():
            return Service.upload()
