import graphene
from graphene import Connection, Int, JSONString, String
from graphene_sqlalchemy import SQLAlchemyObjectType
from graphene_sqlalchemy_filter import FilterableConnectionField
from sqlalchemy import inspect
from sqlalchemy.sql import text
from typing import (  # noqa: F401; pragma: no cover
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Type,
    Tuple,
    Union,
)
from db.database import DBConnection
from models.models import db_models


class BaseSchemaObjectType(SQLAlchemyObjectType):
    class Meta:
        abstract = True

    pk = graphene.Int()

    @classmethod
    def get_node(cls, info, id):
        return cls.get_query(info).filter(cls._meta.model.id == id).first()

    @classmethod
    def get_id_name(cls, info):
        return cls._meta.model.id

    # @resolve_only_args
    def resolve_pk(self, info, **kwargs):
        return inspect(self).identity[0]


class ExtendedConnection(Connection):
    class Meta:
        abstract = True

    total_count = Int()
    edge_count = Int()
    custom = graphene.Field(JSONString, description="Custom",
                            fields=graphene.List(String, default_value=[]),
                            table=graphene.List(String, default_value=[]),
                            innerJoin=graphene.List(String, default_value=[]),
                            leftJoin=graphene.List(String, default_value=[]),
                            where=graphene.List(String, default_value=[]),
                            group=graphene.List(String, default_value=[]),
                            having=graphene.List(String, default_value=[]),
                            sort=graphene.List(String, default_value=[]),
                            limit=graphene.List(String, default_value=[]),
                            offset=graphene.List(String, default_value=[]),
                            )

    @classmethod
    def resolve_total_count(cls, root, info, **kwargs):
        return root.length

    @classmethod
    def resolve_edge_count(cls, root, info, **kwargs):
        return len(root.edges)

    @classmethod
    def resolve_custom(cls, root, info, **kwargs):
        result = []
        # print('kwargs:', kwargs)
        columns = []
        column_names = []
        field_names = []
        from_model = db_models[kwargs['table'][0]]
        query = DBConnection().get_session().query(from_model).select_from(from_model)
        for field in kwargs['fields']:
            field_name = field
            if ' as ' in field:
                field_name = field[field.find(" as ") + 4:]
            if '(' not in field:
                columns.append(text(field))
                column_names.append(field_name)
            # query = query.add_columns(text(field))
            field_names.append(field_name)
        # field_names = column_names + fields
        if len(columns) < 1:
            columns.append(from_model.c.id)
        # query = select(columns)
        # for col in columns:
        query = query.with_entities(*columns)
        # query.select_from(from_model)
        for field in kwargs['fields']:
            if '(' in field:
                query = query.add_columns(text(field))
        arg_names = kwargs.keys()
        if 'innerJoin' in arg_names:
            for innerJoinTable in kwargs['innerJoin']:
                query = query.join(db_models[innerJoinTable])
        if 'leftJoin' in arg_names:
            for leftJoinTable in kwargs['leftJoin']:
                query = query.outerjoin(db_models[leftJoinTable])
        if 'where' in arg_names:
            query = query.filter(text(kwargs['where'][0]))
        if 'group' in arg_names:
            query = query.group_by(text(','.join(kwargs['group'])))
        if 'having' in arg_names:
            query = query.having(text(kwargs['having'][0]))
        if 'sort' in arg_names:
            query = query.order_by(text(','.join(kwargs['sort'])))
        if 'limit' in arg_names:
            query = query.limit(int(kwargs['limit'][0]))
        if 'offset' in arg_names:
            query = query.offset(int(kwargs['offset'][0]))
        # print('query: ', query)
        recs = query.all()  # .values(*field_names)
        # print('recs: ', recs)
        # print('field_names: ', field_names)
        for row in recs:
            # r = row[0].__dict__
            rec = {}
            # for k, v in r.items():
            #    if k != '_sa_instance_state':
            #        rec[k] = v
            idx = 0
            # rl = []
            for field_name in field_names:
                rec[field_name] = row[idx]
                # rl.append(row[idx])
                idx = idx + 1
            result.append(rec)
        return result


# class Query(graphene.ObjectType):
#     pass


class AggregationConnectionField(FilterableConnectionField):
    DEFAULT_AGG_ARG: str = 'aggs'
    aggs: dict = {}

    def __init_subclass__(cls):
        super().__init_subclass__()

    def __init__(self, connection, *args, **kwargs):
        if AggregationConnectionField.DEFAULT_AGG_ARG in kwargs.keys():
            AggregationConnectionField.aggs = kwargs[AggregationConnectionField.DEFAULT_AGG_ARG]
            self.aggs = AggregationConnectionField.aggs
        super(AggregationConnectionField, self).__init__(connection, *args, page=graphene.Int(),
                                                         per_page=graphene.Int(), **kwargs)
        # print('aggs: ', self.aggs)

    @classmethod
    def get_query(cls, model, info: 'ResolveInfo', sort=None, page=None, per_page=None, **args):
        """Standard get_query with filtering."""
        query = super(AggregationConnectionField, cls).get_query(model, info, sort, **args)
        request_filters = args.get(super().filter_arg)
        request_aggs = args.get(cls.DEFAULT_AGG_ARG)
        if request_filters:
            filter_set = cls.get_filter_set(info)
            query = filter_set.filter(info, query, request_filters)
        # print('filters0: ', request_filters)
        # print('aggs0: ', request_aggs)
        # print('model: ', model.__table__.c)
        # if request_aggs and 'group' in request_aggs.keys():
        #     for c in model.__table__.c:
        #         for g in request_aggs['group']:
        #             if g == str(c) or g in str(c):
        #                 query = query.group_by(c)
        #     if 'having' in request_aggs.keys():
        #         # print('h: ', request_aggs['having'])
        #         query = query.having(text(request_aggs['having']))
        # query = query.add_column(text('sum(zones.zone_id) as sum_of_zoneid'))
        if per_page:
            query = query.limit(per_page)
        if page:
            query = query.offset((page - 1) * per_page)
        # print('query: ', query)
        return query


# class AggSet(InputObjectType):
#     model = None
#     fields = None  # graphene.List(String, required=True, value=graphene.List(String, default_value=[]), description="List of field names")
#     # input = InputField(String)
#     aggregation = InputField(String, description="Aggregation functions")
#     field = InputField(String, description="Aggregation functions")
#     group = InputField(graphene.List(String), description="Group by fields")
#     having = InputField(String, description="Group conditions")


# https://stackoverflow.com/questions/11530196/flask-sqlalchemy-query-specify-column-names
# https://www.geeksforgeeks.org/querying-and-selecting-specific-column-in-sqlalchemy/
# https://github.com/graphql-python/graphene-sqlalchemy/issues/285
