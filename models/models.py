
from sqlalchemy import Table, Column, func
from sqlalchemy.sql.type_api import UserDefinedType
from db.database import DBConnection
# from config import CONFIG


# database table model classes
db_classes = {}
db_models = {}


class Point(UserDefinedType):

    def get_col_spec(self):
        return "POINT"

    def bind_expression(self, bindvalue):
        return func.Point(bindvalue, type_=self)

    def column_expression(self, col):
        return func.Point(col, type_=self)

    def bind_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            assert isinstance(value, tuple)
            lat, lng = value
            # return "POINT(%s %s)" % (lng, lat)
            return "(%s,%s)" % (lng, lat)
        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is None:
                return None
            #m = re.match(r'^POINT\((\S+) (\S+)\)$', value)
            #lng, lat = m.groups()
            lng, lat = value.strip('( )').split(',')
            # lng, lat = value[6:-1].split()  # 'POINT(135.00 35.00)' => ('135.00', '35.00')
            return (float(lat), float(lng))
        return process


class Geography(UserDefinedType):

    def get_col_spec(self):
        return "GEOGRAPHY"

    def bind_expression(self, bindvalue):
        return func.ST_GeographyFromText(bindvalue, type_=self)

    def column_expression(self, col):
        return func.ST_AsText(col, type_=self)

    def bind_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            assert isinstance(value, tuple)
            lat, lng = value
            return "POINT(%s %s)" % (lng, lat)
            # return "(%s,%s)" % (lng, lat)
        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is None:
                return None
            #m = re.match(r'^POINT\((\S+) (\S+)\)$', value)
            #lng, lat = m.groups()
            # lng, lat = value.strip('( )').split(',')
            lng, lat = value[6:-1].split()  # 'POINT(135.00 35.00)' => ('135.00', '35.00')
            return (float(lat), float(lng))
        return process


class Geometry(UserDefinedType):

    def get_col_spec(self):
        return "GEOMETRY"

    def bind_expression(self, bindvalue):
        return func.ST_GeomFromText(bindvalue, type_=self)

    def column_expression(self, col):
        return func.ST_AsText(col, type_=self)

    def bind_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            assert isinstance(value, tuple)
            lat, lng = value
            return "POINT(%s %s)" % (lng, lat)
            # return "(%s,%s)" % (lng, lat)
        return process

    def result_processor(self, dialect, coltype):
        def process(value):
            if value is None:
                return None
            #m = re.match(r'^POINT\((\S+) (\S+)\)$', value)
            #lng, lat = m.groups()
            # lng, lat = value.strip('( )').split(',')
            lng, lat = value[6:-1].split()  # 'POINT(135.00 35.00)' => ('135.00', '35.00')
            return (float(lat), float(lng))
        return process


class BaseTable:
    """
    Base Table
    """

    def __init__(self, table_name):
        self._conn = DBConnection().get_connection()
        self._table = Table(table_name, DBConnection().get_meta(), autoload=True, postgresql_ignore_search_path=True)
        has_location = [True if 'location' in c.name and str(c.type) == 'NULL' else False for c in self._table.columns]
        if True in has_location:
            self._table = Table(table_name, DBConnection().get_meta(), Column('location', Point), Column('location_geom', Geometry), Column('location_geog', Geography), extend_existing=True, postgresql_ignore_search_path=True)

    def obj(self):
        """
        Returns: Table
        """
        return self._table

    def get(self, q):
        """
        Returns: Scalar Result
        """
        return self._conn.execute(q).scalar()

    def fetch(self, q):
        """
        Returns: Results
        """
        return self._conn.execute(q).fetchall()

    def exec(self, q, values=None):
        """
        Returns: Results
        """
        if values is None:
            return self._conn.execute(q)
        else:
            return self._conn.execute(q, values)

    @classmethod
    def to_list_of_dicts(cls, resultset):
        # return [{tuple[0]: tuple[1] for tuple in row.items()} for row in resultset]
        return [dict(row) for row in resultset]


table_names = list(DBConnection().get_meta().tables.keys())
for name, db_class in DBConnection().get_base().classes.items():
    db_models[name] = DBConnection().get_base().classes[name]
    #db_models[name] = Table(name, DBConnection().get_meta(), autoload=True, postgresql_ignore_search_path=True)
    #has_location = [True if 'location' in c.name and str(c.type) == 'NULL' else False for c in db_models[name].columns]
    #if True in has_location:
    #    db_models[name] = Table(name, DBConnection().get_meta(), Column('location', Point), Column('location_geom', Geometry), Column('location_geog', Geography), extend_existing=True, postgresql_ignore_search_path=True)
    # print(DBConnection().get_session().query(db_models[name]).all())

# for name in table_names:
#     table_name = name.replace(CONFIG['DB_SCHEMA']+'.', '')
#     try:
#         db_classes[table_name.replace('_','')] = BaseTable(table_name)
#         db_models[table_name.replace('_','')] = type(table_name.replace('_',''), (DBConnection().get_base(),), {
#             '__tablename__': name,
#             '__table__': db_classes[table_name.replace('_','')].obj()
#         })
#         print(DBConnection().get_session().query(db_models[table_name.replace('_','')]).all())
#     except Exception as ex:
#         print(table_name, ex)

# Show the metadata
# for t in Base.metadata.sorted_tables:
#         print(f"\nTable {t.name}:")
#         for c in t.columns:
#             print(f"{c} ({c.type})")

# for schema_name in DBConnection().get_inspector().get_schema_names():
#     for table_name in DBConnection().get_inspector().get_table_names(schema=schema_name):
#         if schema_name == 'public':
#             db_classes[table_name] = BaseTable(table_name)
#         else :
#             db_classes[schema_name+'_'+table_name] = BaseTable(table_name)

# try:
#     DBConnection().get_base().prepare()
#     # pass
# except Exception as ex:
#     print(table_name, ex)
