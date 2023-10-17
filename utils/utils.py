# utils
import json
import re
import flask
from werkzeug.utils import secure_filename
from configs.config import CONFIG
import sys
import traceback


class SingletonMetaClass(type):
    """
    Singleton Meta Class
    """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(SingletonMetaClass, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class AppUtils:

    @classmethod
    def getErrorDetails(cls, e: BaseException):
        ex_type, ex_value, ex_traceback = sys.exc_info()
        # Extract unformatter stack traces as tuples
        trace_back = traceback.extract_tb(ex_traceback)
        # Format stacktrace
        stack_trace = list()
        for trace in trace_back:
            stack_trace.append("File : %s , Line : %d, Func.Name : %s, Message : %s" % (
                trace[0], trace[1], trace[2], trace[3]
            ))
        return {"Error": {"type:": ex_type.__name__, "msg": ex_value, "trace": stack_trace}}

    @classmethod
    def to_file(cls, path, data):
        with open(path, 'w') as f:
            f.write(data)

    @classmethod
    def read_file(cls, path, is_json=True):
        with open(path, 'r') as f:
            if is_json:
                return json.load(f)
            else:
                return f.read()

    @classmethod
    def make_response(cls, data: any, code: int = 200, msg: any = None, is_json=True):
        if is_json:
            if not msg:
                msg = {"status": "ok"}
            if int(code) < 200 or int(code) >= 300:
                if CONFIG['DEBUG']:
                    data = [{"Exception": data.__str__()}]
                else:
                    data = []
            resp = flask.make_response(json.dumps({"code": code, "msg": msg, "data": data}))
            resp.mimetype = 'application/json'
            return resp
        else:
            return data

    @classmethod
    def file_upload(cls, type_val, id_val, field, file):
        file.save(f"uploads/{type_val}/{id_val}/{field}/{secure_filename(file.filename)}")

    @classmethod
    def escape_string(cls, text):
        # Uncomment after proper testing
        # regex_slash = {"search": re.escape("\\"), "replace": re.escape("\\\\")}
        # Avoid sql comment
        # This will fail sql query if comment is not quoted and thus prevent sql injection
        regex_hyphen = {"search": r"--", "replace": "- - "}
        # Yet another avoid sql comment
        # This will fail sql query if comment is not quoted and thus prevent sql injection
        regex_hyphen_ya = {"search": r"--", "replace": "&minus;&minus;"}
        # Avoid multiple sql statements at once
        # This will fail sql query if semicolon is not quoted and thus prevent sql injection
        regex_semicolon = {"search": r";", "replace": "&#59;"}
        regex_list = [regex_semicolon, regex_hyphen, regex_hyphen_ya]  # regex_slash
        # Avoid odd occurances of quote
        # This will fail sql query if quote is not closed properly and thus prevent sql injection
        if text.count("'") % 2 != 0:
            regex_quote = {"search": r"'", "replace": "''"}
            regex_list.append(regex_quote)
        # Replace bad characters
        # This will fail sql query if bad characters not at wrong place otherwise the query will execute safely
        for regex_q in regex_list:
            text = re.sub(regex_q["search"], regex_q["replace"], text)
        return text

    @classmethod
    def listDict(cls, result_set: list):
        return [row._asdict() for row in result_set]


metadic = {}


def _generatemetaclass(bases, metas, priority):
    trivial = lambda m: sum([issubclass(M, m) for M in metas], m is type)
    # hackish!! m is trivial if it is 'type' or, in the case explicit
    # metaclasses are given, if it is a superclass of at least one of them
    metabs = tuple([mb for mb in map(type, bases) if not trivial(mb)])
    metabases = (metabs + metas, metas + metabs)[priority]
    if metabases in metadic:  # already generated metaclass
        return metadic[metabases]
    elif not metabases:  # trivial metabase
        meta = type
    elif len(metabases) == 1:  # single metabase
        meta = metabases[0]
    else:  # multiple metabases
        metaname = "_" + ''.join([m.__name__ for m in metabases])
        meta = makecls()(metaname, metabases, {})
    return metadic.setdefault(metabases, meta)


def makecls(*metas, **options):
    """Class factory avoiding metatype conflicts. The invocation syntax is
    makecls(M1,M2,..,priority=1)(name,bases,dic). If the base classes have
    metaclasses conflicting within themselves or with the given metaclasses,
    it automatically generates a compatible metaclass and instantiate it.
    If priority is True, the given metaclasses have priority over the
    bases' metaclasses"""

    priority = options.get('priority', False)  # default, no priority
    return lambda n, b, d: _generatemetaclass(b, metas, priority)(n, b, d)
