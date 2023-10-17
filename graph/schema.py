import json
import graphene
from configs.logger import log
from configs.constants import FILTER_TYPES
from graphene import relay
from graphene_sqlalchemy_auto import QueryObjectType
from graphene_sqlalchemy_filter import FilterSet
from db.database import DBConnection
from graphene_sqlalchemy_mutations.mutations import MutationObjectType
from graph.graphine import ExtendedConnection, BaseSchemaObjectType, AggregationConnectionField
from models.models import db_models
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from utils.utils import AppUtils
# from flask_sqlalchemy import SQLAlchemy

# db = SQLAlchemy()
Base = declarative_base()
Session = sessionmaker()

# print(db_models)
schema_classes = {}
query_schema = {}
mutation_schema = {}
mutation_class_body = {}
mutation_class_dict = {}
include_mutations = []

for name, db_model in db_models.items():
    try:
        schema_classes[name] = type(name, (BaseSchemaObjectType,), {
            'Meta': type('Meta', (object,), {'model': db_model, 'interfaces': (relay.Node, ), 'connection_class': ExtendedConnection})
        })
        filter_fields = {}
        for column in db_model.__table__.columns:
            filter_fields[str(column).replace(name+'.', '')] = FILTER_TYPES
        filter_class = type(name.capitalize()+'Filter', (FilterSet,), {
            'Meta': type('Meta', (object,), {'model': db_model, 'fields': filter_fields})
        })
        query_schema[name] = AggregationConnectionField(schema_classes[name].connection, filters=filter_class())
        # agg_class = type(name.capitalize() + 'Agg', (AggSet,), {
        #     'Meta': type('Meta', (object,), {'model': db_model, 'fields': filter_fields})
        # })
        # query_schema['all_'+name] = FilterableConnectionField(schema_classes[name].connection, filters=filter_class())
        # query_schema[name] = AggregationConnectionField(schema_classes[name].connection, filters=filter_class(), aggs=agg_class())
    except Exception as ex:
        log.error(name, ex)


class CQuery(QueryObjectType):
    class Meta:
        declarative_base = DBConnection().get_base()
        exclude_models = []   # exclude models ["User"]


class Mutation(MutationObjectType):
    class Meta:
        declarative_base = DBConnection().get_base()
        session = Session()     # mutate used
        # include_object = []     # you can use yourself mutation UserCreateMutation, UserUpdateMutation


query_schema['Meta'] = type('Meta', (object,), {'declarative_base': Base, 'exclude_models': []})
Query = type('Query', (QueryObjectType,), query_schema)
# mutation_schema['Meta'] = type('Meta', (object,), {'declarative_base': DBConnection().get_base(), 'session': Session(), 'include_object': include_mutations})
# Mutation = type('Mutation', (MutationObjectType,), mutation_schema)
# Query = type('Query', (graphene.ObjectType,), query_schema)
# print('Query: ', Query.agg_zones)
schema = graphene.Schema(query=Query, mutation=Mutation, auto_camelcase=False)
# schema_str = schema_printer.print_schema(schema)
AppUtils.to_file('schema.json', json.dumps(schema.introspect()))
AppUtils.to_file('schema.graphql', str(schema))

# GraphQL Federation
# https://stackoverflow.com/questions/56867951/problems-integrating-python-graphene-with-apollo-federation
