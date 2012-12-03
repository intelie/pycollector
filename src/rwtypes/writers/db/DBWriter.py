# coding: utf-8
import logging
import traceback

import sys; sys.path.append('..')
import __meta__; __meta__.load_paths()
from __writer import Writer
from MessageEntity import MessageEntity
from third.sqlalchemy import create_engine
from third.sqlalchemy.orm import sessionmaker

class DBWriter(Writer):

    def start_session(self):
        engine = create_engine("mysql://%s:%s@%s/%s" % (self.user,
                                                    self.passwd,
                                                    self.host,
                                                    self.database),
                                                    echo=False)

        Session = sessionmaker(bind=engine)
        self.session = Session()

    def do_insert(self, msg):
        try:
            self.start_session()
            obj = MessageEntity(msg)
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
            self.do_insert(str(msg))

            return True
        except Exception:
            self.log.error('error inserting on database')
            self.log.error(traceback.format_exc())
            return False
