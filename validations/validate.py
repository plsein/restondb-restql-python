import json
from db.dbaccess import DbAccess
from utils.utils import AppUtils
import validations
# in use
from . import *


class Validate:

    rules = {}

    @classmethod
    def getTableRules(cls, table: str, schema: str = ''):
        if table in Validate.rules and isinstance(Validate.rules[table], dict):
            return Validate.rules[table]
        comments = DbAccess.columnComments(table, schema)
        comments_data = AppUtils.listDict(comments)
        Validate.rules[table] = {}
        if isinstance(comments_data, list) and len(comments_data) > 0:
            for rec in comments_data:
                v = {}
                if isinstance(rec, dict) and 'column_comment' in rec and len(rec['column_comment']) > 0:
                    v = json.loads(rec['column_comment'])
                if 'Validations' in v:
                    Validate.rules[table][rec['column_name']] = v['Validations']
        return Validate.rules[table]

    @classmethod
    def check(cls, data: list, table: str):
        msg = {}
        idx = 0
        checks = {}
        if isinstance(table, str) and len(table) > 0:
            if table in Validate.rules and isinstance(Validate.rules[table], dict):
                checks = Validate.rules[table]
            else:
                checks = Validate.getTableRules(table)
        if isinstance(data, list) and len(data) > 0 and isinstance(checks, dict) and len(checks) > 0:
            for rec in data:
                msg[''+str(idx)] = {}
                if isinstance(rec, dict) and len(rec) > 0:
                    for key, val in rec.items():
                        msg[''+str(idx)][key] = {}
                        if key in checks and isinstance(checks[key], dict) and len(checks[key]) > 0:
                            for check, value in checks[key].items():
                                msg[''+str(idx)][key][check] = getattr(getattr(validations, check), check).validate(rec, value)
                                if len(msg[''+str(idx)][key][check]) < 1:
                                    del msg[''+str(idx)][key][check]
                        if len(msg[''+str(idx)][key]) < 1:
                            del msg[''+str(idx)][key]
                if len(msg[''+str(idx)]) < 1:
                    del msg[''+str(idx)]
                idx = idx + 1
        return msg
