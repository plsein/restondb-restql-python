from sqlalchemy import text
from db.database import DBConnection
from models.models import db_models
from graph.schema import Query
from utils.utils import AppUtils


class SelectQueryBuilder:

    defaultLimit = 10

    selectClauseFnSeq = ['select', 'inner', 'left', 'where', 'group', 'having', 'sort', 'limit', 'offset', 'bind']

    initialResp = {'msg': {}, 'sql_query': ''}

    @classmethod
    def mergeResp(cls, resp, res):
        if isinstance(resp, dict) and isinstance(res, dict):
            if 'msg' in resp.keys() and 'msg' in res.keys() and isinstance(resp['msg'], dict):
                resp['msg'] = resp['msg'].update(res['msg'])
            if 'sql_query' in res.keys() and isinstance(res['sql_query'], Query):
                resp['sql_query'] = res['sql_query']
            return resp
        return {}

    @classmethod
    def table(cls, table, ret_table: bool = False):
        resp = SelectQueryBuilder.initialResp
        if not table or not isinstance(table, str):
            resp['msg']['table'] = "table must be a string"
        elif len(table) > 0 and table in db_models.keys():
            from_model = db_models[table]
            if ret_table:
                return from_model
            query = DBConnection().get_session().query(from_model).select_from(from_model)
            resp['sql_query'] = query
        else:
            resp['msg']['table'] = "table must be a valid string"
        return resp

    @classmethod
    def fields(cls, fields):
        field_names = []
        if fields and isinstance(fields, list):
            for field in fields:
                field_name = field
                if ' as ' in field:
                    field_name = field[field.find(" as ") + 4:]
                if '(' not in field:
                    field_names.append(field_name)
                if '(' in field:
                    field_names.append(field_name)
        return field_names

    @classmethod
    def select(cls, fields, table):
        columns = []
        field_names = []
        resp = SelectQueryBuilder.initialResp
        res = SelectQueryBuilder.table(table)
        from_model = SelectQueryBuilder.table(table, True)
        resp = SelectQueryBuilder.mergeResp(resp, res)
        if not fields or not isinstance(fields, list):
            resp['msg']['fields'] = "fields must be an array"
        else:
            for field in fields:
                field_name = field
                if ' as ' in field:
                    field_name = field[field.find(" as ") + 4:]
                if '(' not in field:
                    columns.append(text(AppUtils.escape_string(field)))
                    field_names.append(field_name)
            if len(columns) < 1:
                columns.append(list(from_model.__table__.primary_key)[0].name)
            resp['sql_query'] = resp['sql_query'].with_entities(*columns)
            for field in fields:
                field_name = field
                if ' as ' in field:
                    field_name = field[field.find(" as ") + 4:]
                if '(' in field:
                    resp['sql_query'] = resp['sql_query'].add_columns(text(AppUtils.escape_string(field)))
                    field_names.append(field_name)
        return resp

    @classmethod
    def inner(cls, resp, inner):
        if inner:
            if not isinstance(inner, list):
                resp['msg']['inner'] = "inner must be an array"
                # return utils.make_response(401, "inner must be an array", [])
            else:
                for innerJoinTable in inner:
                    if innerJoinTable not in db_models.keys():
                        resp['msg']['inner'] = "inner join table not found"
                    resp['sql_query'] = resp['sql_query'].join(db_models[innerJoinTable])
        return resp

    @classmethod
    def left(cls, resp, left):
        if left:
            if not isinstance(left, list):
                resp['msg']['left'] = "left must be an array"
            else:
                for leftJoinTable in left:
                    if leftJoinTable not in db_models.keys():
                        resp['msg']['left'] = "left join table not found"
                    resp['sql_query'] = resp['sql_query'].outerjoin(db_models[leftJoinTable])
        return resp

    @classmethod
    def where(cls, resp, where):
        if where:
            if not isinstance(where, str):
                resp['msg']['where'] = "where must be a string"
            else:
                resp['sql_query'] = resp['sql_query'].filter(text(AppUtils.escape_string(where)))
        return resp

    @classmethod
    def group(cls, resp, group):
        if group:
            if not isinstance(group, list):
                resp['msg']['group'] = "group must be an array"
            else:
                resp['sql_query'] = resp['sql_query'].group_by(text(AppUtils.escape_string(', '.join(group))))
        return resp

    @classmethod
    def having(cls, resp, having):
        if having:
            if not isinstance(having, str):
                resp['msg']['having'] = "having must be a string"
            else:
                resp['sql_query'] = resp['sql_query'].having(text(AppUtils.escape_string(having)))
        return resp

    @classmethod
    def sort(cls, resp, sort):
        if sort:
            if not isinstance(sort, list):
                resp['msg']['sort'] = "sort must be an array"
            else:
                resp['sql_query'] = resp['sql_query'].order_by(text(AppUtils.escape_string(', '.join(sort))))
        return resp

    @classmethod
    def limit(cls, resp, limit):
        if limit and not isinstance(limit, int):
            resp['msg']['limit'] = "limit must be an integer"
        else:
            if int(limit) < 1:
                limit = SelectQueryBuilder.defaultLimit
            resp['sql_query'] = resp['sql_query'].limit(int(limit))
        return resp

    @classmethod
    def offset(cls, resp, offset):
        if offset and not isinstance(offset, int):
            resp['msg']['offset'] = "offset must be an integer"
        else:
            if int(offset) > 0:
                resp['sql_query'] = resp['sql_query'].offset(int(offset))
        return resp

    @classmethod
    def bind(cls, resp, bind):
        if bind:
            if isinstance(bind, dict):
                resp['sql_query'] = resp['sql_query'].params(bind)
            else:
                resp['msg']['bind'] = "bind must be key value pairs"
        return resp

    @classmethod
    def build(cls, params):
        resp = {}
        if not isinstance(params, dict):
            resp['msg']['params'] = "params must be an array"
        elif len(params) > 0:
            for fn in SelectQueryBuilder.selectClauseFnSeq:
                fnc = getattr(SelectQueryBuilder, fn)
                if fn == 'select':
                    resp = fnc(params['fields'], params['table'])
                else:
                    resp = fnc(resp, params[fn])
        return resp
