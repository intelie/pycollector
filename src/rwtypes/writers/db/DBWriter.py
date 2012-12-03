# coding: utf-8
import logging
import traceback

import sys; sys.path.append('..')
import __meta__; __meta__.load_paths()
from __writer import Writer
from datetime import datetime
from MessageEntity import Base
from MessageEntity import MessageEntity
from third.sqlalchemy import create_engine, Integer, Float, String, DateTime
from third.sqlalchemy.orm import sessionmaker
from third.sqlalchemy.engine.reflection import Inspector

class DBWriter(Writer):

    def start_session(self):
        self.engine = create_engine(self.connection % (self.user,
                                                    self.passwd,
                                                    self.host,
                                                    self.database),
                                                    echo=False)

        Session = sessionmaker(bind=self.engine)
        self.session = Session()

    def do_insert(self, msg):

        # verify if table already exists
        self.start_session()
        inspector = Inspector.from_engine(self.engine)

        if not (MessageEntity.__tablename__ in inspector.get_table_names()):
            Base.metadata.create_all(self.engine)

        try:
            obj = MessageEntity(msg)

            print obj
            return

            self.session.add(obj)
            self.session.commit()
        except Exception:
            self.log.error('Error during database insertion')
            self.log.error(traceback.format_exc())

    def setup(self):
        self.log = logging.getLogger('pycollector')

        self.required_confs = ['user', 'passwd', 'host', 'database']
        self.check_required_confs()

        if self.checkpoint_enabled:
            relf.set_current_checkpoint()

    def write(self, msg):
        try:
            self.do_insert(msg)

            return True
        except Exception:
            self.log.error('error inserting on database')
            self.log.error(traceback.format_exc())
            return False

