from sqlalchemy import text
from sqlalchemy import create_engine, inspect, MetaData
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from configs.config import CONFIG
from utils.utils import SingletonMetaClass


class DBConnection(metaclass=SingletonMetaClass):
    """
    DB Connection Class
    """

    def __init__(self):
        self.__engine = create_engine("postgresql://%s:%s@%s:%s/%s"
                               % (CONFIG['DB_USER'], CONFIG['DB_PASS'], CONFIG['DB_HOST'], CONFIG['DB_PORT'], CONFIG['DB_NAME']),
                               client_encoding="UTF-8", pool_recycle=CONFIG['DB_POOL_RECYCLE'],
                               connect_args={'options': '-csearch_path={}'.format(CONFIG['DB_SCHEMA']+',public')})
        self.__conn = self.__engine.connect().execution_options(autocommit=True)    # executemany_mode='batch'
        self.__conn.execute(text("SET search_path TO "+CONFIG['DB_SCHEMA']+",public"))
        self.__meta = MetaData(bind=self.__engine, schema=CONFIG['DB_SCHEMA'])
        self.__meta.reflect(bind=self.__engine)
        # self.__base = declarative_base()
        self.__base = automap_base(metadata=self.__meta)    # declarative_base()
        self.__base.prepare(self.__engine, reflect=True, schema=CONFIG['DB_SCHEMA'])
        # self.__base.metadata = self.__meta
        # self.__base.prepare(autoload_with=self.__engine)
        #self.__session = scoped_session(sessionmaker(autocommit=True, autoflush=True, executemany_mode='batch', bind=self.__engine))
        # self.__base.query = self.__session.query_property()
        self.__inspector = inspect(self.__engine)
        self.__session = Session(self.__engine, autocommit=True)

    def get_connection(self):
        """
        Get connection object
        :return: connection object
        """
        return self.__conn

    def get_meta(self):
        """
        Get metadata object
        :return: db metadata object
        """
        return self.__meta

    def get_base(self):
        """
        Get base object
        :return: db base object
        """
        return self.__base

    def get_session(self):
        """
        Get session object
        :return: db session object
        """
        return self.__session

    def get_inspector(self):
        """
        Get inspector object
        :return: db inspection object
        """
        return self.__inspector

#    @classmethod
#    def to_list_of_dicts(cls, resultset):
#        """
#        Get list of dicts for db result sets
#        :return: list of dicts
#        """
        # return [{tuple[0]: tuple[1] for tuple in row.items()} for row in resultset]
        # return [dict(row) for row in resultset]
#        return [row._asdict() for row in resultset]
