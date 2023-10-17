from configs.config import CONFIG
from db.database import DBConnection
from models.models import db_models
from sqlalchemy import text
from utils.utils import AppUtils


class DbAccess:

    @classmethod
    def columnComments(cls, table: str, schema: str = 'public'):
        db_schemas = 'public'
        if isinstance(schema, str) and len(schema) > 0:
            db_schemas = schema
        else:
            db_schemas = "'" + "','".join(CONFIG['DB_SCHEMA'].split(",")) + "'"
        table_name = AppUtils.escape_string(table)
        sql_query = text("SELECT table_schema, table_name, column_name, COL_DESCRIPTION("
                         + " (table_schema||'.'||table_name)::regclass::oid, ordinal_position) AS column_comment "
                         + " FROM information_schema.columns "
                         + " WHERE table_schema IN (" + db_schemas + ") AND table_name='" + table_name + "' "
                         + " AND col_description( "
                         + " (table_schema||'.'||table_name)::regclass::oid, ordinal_position "
                         + " ) IS NOT null")
        # AND column_name IN("+table_fields+")
        query = DBConnection().get_session().execute(sql_query)
        return query.all()

    @classmethod
    def select(cls, query):
        return query.all()

    @classmethod
    def insert(cls, records: list, table: str):
        DBConnection().get_session().bulk_insert_mappings(db_models[table], records)
        # DBConnection().get_session().commit()

    @classmethod
    def edit(cls, objects: list, table: str):
        DBConnection().get_session().bulk_update_mappings(db_models[table], objects)
        # DBConnection().get_session().commit()

    @classmethod
    def update(cls, data: dict, table: str, where: str, bind: dict):
        DBConnection().get_connection().execute(
            db_models[table].__table__.update().values(data).where(text(where)),
            bind
        )

    @classmethod
    def delete(cls, ids: list, table: str):
        DBConnection().get_session().query(db_models[table]).filter(
            db_models[table].__table__.c[list(db_models[table].__table__.primary_key)[0].name].in_(ids)
        ).delete()
        # return DBConnection().get_session().commit()

    @classmethod
    def remove(cls, table: str, where: str, bind: dict):
        DBConnection().get_connection().execute(
            db_models[table].__table__.delete().where(text(where)),
            bind
        )
