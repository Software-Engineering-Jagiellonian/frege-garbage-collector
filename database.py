from logging import Logger

from sqlalchemy import Boolean, func, case
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Column, Integer, String, Sequence, ForeignKey

from database_connection import DatabaseConnectionParameters

Base = declarative_base()


class RepositoryLanguageTable(Base):
    __tablename__ = "repository_language"
    id = Column(Integer, Sequence('repository_language_id_seq'), primary_key=True)
    repository_id = Column(String)
    language_id = Column(Integer)
    present = Column(Boolean)
    analyzed = Column(Boolean)


class Database:
    def __init__(self, database_parameters: DatabaseConnectionParameters, logger: Logger):
        self.database_parameters = database_parameters
        self.log = logger
        self.engine = create_engine(f'postgresql://{self.database_parameters.username}:'
                                    f'{self.database_parameters.password}@{self.database_parameters.host}:'
                                    f'{self.database_parameters.port}/{self.database_parameters.database}')
        self.connection = None

    def connect(self):
        self.connection = self.engine.connect()
        self.Session = sessionmaker(bind=self.connection)

    def mark_language_as_analyzed(self, repository_id: str, language_id: int):
        session = self.Session()
        row = session.query(RepositoryLanguageTable).filter_by(repository_id=repository_id, language_id=language_id). \
            first()
        if row is None:
            self.log.error('Try to mark as analyzed nonexistent repository ' +
                           f'{repository_id} and language id {language_id}')
            return
        row.present = True
        row.analyzed = True
        session.commit()

    def are_all_present_languages_analyzed(self, repository_id: str):
        session = self.Session()
        present_count = session.query(func.sum(case([(RepositoryLanguageTable.present == True, 1)], else_=0))). \
            filter_by(repository_id=repository_id).scalar()
        analyzed_count = session.query(func.sum(case([(RepositoryLanguageTable.analyzed == True, 1)], else_=0))). \
            filter_by(repository_id=repository_id).scalar()
        session.commit()

        self.log.debug(f'Present count in repository {repository_id}: {str(present_count)}')
        self.log.debug(f'Analyzed count in repository {repository_id}: {str(analyzed_count)}')

        if present_count is None or analyzed_count is None:
            self.log.error('SQL query return no result for analyzed and/or resent repository languages count! ' +
                           'This mean probably wrong repository id (given from analyzer)')
            return False
        return (present_count - analyzed_count) <= 0
