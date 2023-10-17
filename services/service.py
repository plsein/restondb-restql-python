from utils.utils import AppUtils
from flask import request
from db.dbaccess import DbAccess
from db.pg_select_query_builder import PgSelectQueryBuilder
from validations.validate import Validate


class Service:

    @classmethod
    def index(cls):
        return AppUtils.make_response([{"index": "/"}])

    @classmethod
    def graphqlSchema(cls):
        return AppUtils.make_response(AppUtils.read_file('schema.graphql', is_json=False), is_json=False)

    @classmethod
    def graphqlJsonSchema(cls):
        return AppUtils.make_response(AppUtils.read_file('schema.json'))

    @classmethod
    def select(cls):
        """
        Sample input json: {
          "fields": ["sum(zone_id) as sum_zone_id", "sum(zone_id)/:div as half_sum_zone_id", "cloud_regions.region_name as region_name", "cloud_service_providers.provider_name"],
          "table": "zones",
          "inner": ["cloud_regions"],
          "left": ["cloud_service_providers"],
          "where": "zone_id > :zoneId and region_name like :regionName",
          "group": ["zone_id", "cloud_regions.region_name", "cloud_service_providers.provider_name"],
          "having": "sum(zone_id) > :zoneIdSum",
          "sort": ["zone_name asc"],
          "bind": {"div":2, "zoneId":0, "regionName": "%north%", "zoneIdSum": 100},
          "limit": 2,
          "offset": 1
        }
        """
        params = {}
        req_params = request.json
        params['fields'] = req_params.get('fields', None)
        params['table'] = req_params.get('table', None)
        params['inner'] = req_params.get('inner', None)
        params['left'] = req_params.get('left', None)
        params['where'] = req_params.get('where', None)
        params['group'] = req_params.get('group', None)
        params['having'] = req_params.get('having', None)
        params['sort'] = req_params.get('sort', None)
        params['limit'] = req_params.get('limit', None)
        params['offset'] = req_params.get('offset', None)
        params['bind'] = req_params.get('bind', None)

        result = []
        try:
            resp = PgSelectQueryBuilder.build(params)
            if 'msg' in resp.keys() and isinstance(resp['msg'], dict) and len(resp['msg'].keys()) > 0:
                return AppUtils.make_response(400, resp['msg'], [])
            recs = DbAccess.select(resp['sql_query'])
            field_names = PgSelectQueryBuilder.fields(params['fields'])
            for row in recs:
                rec = {}
                idx = 0
                for field_name in field_names:
                    rec[field_name] = row[idx]
                    idx = idx + 1
                result.append(rec)
        except Exception as e:
            return AppUtils.make_response(AppUtils.getErrorDetails(e), 500, {"error": "Internal Error"})
        return AppUtils.make_response(result)

    @classmethod
    def add(cls):
        """
        Sample input json: {
            "table": "zones",
            "records": [{
                "zone_name": "test zone 104",
                "region_id": 41
            },{
                "zone_name": "test zone 105",
                "region_id": 41
            }]
        }
        """
        params = request.json
        table = params.get('table', None)
        records = params.get('records', None)
        resp = []
        msg = {}
        try:
            if table:
                if not isinstance(table, str):
                    return AppUtils.make_response([], 401, {"error": "table must be a string"})
                if records:
                    if not isinstance(records, list):
                        return AppUtils.make_response([], 401, {"error": "records must be an array"})
                    vmsg = Validate.check(records, table)
                    if len(vmsg) > 0:
                        msg['validations'] = vmsg
                        return AppUtils.make_response([], 400, msg)
                    DbAccess.insert(records, table)
            else:
                return AppUtils.make_response([], 400, {"error": "Invalid Input"})
        except Exception as e:
            return AppUtils.make_response(e, 500, {"error": "Internal Error"})
        return AppUtils.make_response(resp)

    @classmethod
    def edit(cls):
        params = request.json
        table = params.get('table', None)
        objects = params.get('objects', None)
        resp = []
        msg = {}
        try:
            if table:
                if not isinstance(table, str):
                    return AppUtils.make_response([], 401, {"error": "table must be a string"})
                if objects:
                    if not isinstance(objects, list):
                        return AppUtils.make_response([], 401, {"error": "objects must be an array"})
                    vmsg = Validate.check(objects, table)
                    if len(vmsg) > 0:
                        msg['validations'] = vmsg
                        return AppUtils.make_response([], 400, msg)
                    DbAccess.edit(objects, table)
            else:
                return AppUtils.make_response([], 400, {"error": "Invalid Input"})
        except Exception as e:
            return AppUtils.make_response(e, 500, {"error": "Internal Error"})
        return AppUtils.make_response(resp)

    @classmethod
    def update(cls):
        """
        Sample input json: {
          "objects": [{
            "table": "zones",
            "where": "zone_id=138",
            "record":{
              "zone_name": "test zone 104",
              "region_id": 41
            }
          },{
            "table": "zones",
            "where": "zone_id=139",
            "record": {
                "zone_name": "test zone 105",
                "region_id": 41
            }
          }]
        }
        """
        params = request.json
        objects = params.get('objects', None)
        resp = []
        msg = {}
        idx = 0
        try:
            if objects:
                if not isinstance(objects, list):
                    msg[''+str(idx)]['objects'] = "objects must be present as an array"
                    # return utils.make_response(401, [{"error": "objects must be present as an array"}], [])
                for obj in objects:
                    msg['' + str(idx)] = {}
                    if 'table' not in obj or not isinstance(obj['table'], str):
                        msg[''+str(idx)]['table'] = "table must be present as a string"
                    if 'where' not in obj or not isinstance(obj['where'], str):
                        msg[''+str(idx)]['where'] = "where must be present as a string"
                    if 'record' not in obj or not isinstance(obj['record'], dict):
                        msg[''+str(idx)]['record'] = "record must be present as key value pairs"
                    if 'bind' in obj and not isinstance(obj['bind'], dict):
                        msg[''+str(idx)]['bind'] = "bind must be present as key value pairs"
                    table = obj['table']
                    where = obj['where']
                    bind = obj['bind']
                    data = dict(obj['record'])
                    # data = {}
                    # for key in record.keys():
                        # data[db_models[table].__table__.c.__getattr__(key)] = record[key]
                        # data[db_models[table].__table__.c.__getattr__(key).name] = record[key]
                    vmsg = Validate.check([data], table)
                    if len(vmsg) > 0:
                        msg[''+str(idx)]['validations'] = vmsg['0']
                    if len(data.keys()) > 0 and (str(idx) not in msg or len(msg[''+str(idx)]) < 1):
                        DbAccess.update(data, table, where, bind)
                    if len(msg[''+str(idx)]) < 1:
                        del msg[''+str(idx)]
                    idx = idx + 1
            else:
                return AppUtils.make_response([], 400, {"error": "Invalid Input"})
            if len(msg) > 0:
                return AppUtils.make_response([], 400, msg)
        except Exception as e:
            return AppUtils.make_response(e, 500, {"error": "Internal Error"})
        return AppUtils.make_response(resp)

    @classmethod
    def delete(cls):
        params = request.json
        table = params.get('table', None)
        ids = params.get('ids', None)
        resp = []
        try:
            if table:
                if not isinstance(table, str):
                    return AppUtils.make_response([], 401, {"error": "table must be a string"})
                if ids:
                    if not isinstance(ids, list):
                        return AppUtils.make_response([], 401, {"error": "ids must be an array"})
                    DbAccess.delete(ids, table)
            else:
                return AppUtils.make_response([], 400, {"error": "Invalid Input"})
        except Exception as e:
            return AppUtils.make_response(e, 500, {"error": "Internal Error"})
        return AppUtils.make_response(resp)

    @classmethod
    def remove(cls):
        """
        Sample input json: {
          "objects": [{
            "table": "zones",
            "where": "zone_id=138"
          },{
            "table": "zones",
            "where": "zone_id=139"
          }]
        }
        """
        params = request.json
        objects = params.get('objects', None)
        resp = []
        msg = []
        idx = 0
        try:
            if objects:
                if not isinstance(objects, list):
                    msg[idx]['objects'] = "objects must be present as an array"
                    # return utils.make_response(401, [{"error": "objects must be present as an array"}], [])
                for obj in objects:
                    if 'table' not in obj or not isinstance(obj['table'], str):
                        msg[idx]['table'] = "table must be present as a string"
                    if 'where' not in obj or not isinstance(obj['where'], str):
                        msg[idx]['where'] = "where must be present as a string"
                    if 'bind' in obj and not isinstance(obj['bind'], dict):
                        msg[idx]['bind'] = "bind must be present as key value pairs"
                    table = obj['table']
                    where = obj['where']
                    bind = obj['bind']
                    if idx not in msg or len(msg[idx]) < 1:
                        DbAccess.remove(table, where, bind)
                    idx = idx + 1
            else:
                return AppUtils.make_response([], 400, {"error": "Invalid Input"})
            if len(msg) > 0:
                return AppUtils.make_response([], 400, msg)
        except Exception as e:
            return AppUtils.make_response(e, 500, {"error": "Internal Error"})
        return AppUtils.make_response(resp)

    @classmethod
    def upload(cls):
        if request.method == 'POST':
            type_val = request.form.get('type', '')
            id_val = request.form.get('id', '')
            field = request.form.get('field', '')
            file = request.files['file']
            AppUtils.file_upload(type_val, id_val, field, file)
            return AppUtils.make_response([], 200, {"status": "ok"})
        else:
            return AppUtils.make_response([], 401, {"error": "File not found"})
