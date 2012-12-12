# coding: utf-8
import logging
import traceback

import sys; sys.path.append('..')
import __meta__; __meta__.load_paths()
from __writer import Writer
from datetime import datetime
from MessageEntity import MessageEntity
from third.sqlalchemy import create_engine
from third.sqlalchemy.orm import sessionmaker

class DBWriter(Writer):

    def start_session(self):
        self.engine = create_engine(self.connection % (self.user,
                                                    self.passwd,
                                                    self.host,
                                                    self.database),
                                                    echo=False)

        Session = sessionmaker(bind=self.engine)
        self.session = Session()


    def set_current_checkpoint(self, value=None):
        if value != None:
            self.current_checkpoint = value
        else:
            self.current_checkpoint = self.last_checkpoint or {'pos': 0}


    def do_insert(self, msg):

        try:
            message = MessageEntity(msg)

            self.session.add(message)
            self.session.commit()

        except Exception as e:
            self.log.error('Error during database insertion: %s' % e)
            self.log.error(traceback.format_exc())

    def setup(self):
        self.log = logging.getLogger('pycollector')

        self.start_session()

        self.required_confs = ['user', 'passwd', 'host', 'database']
        self.check_required_confs()

        if self.checkpoint_enabled:
            self.set_current_checkpoint()
            pass

    def write(self, msg):
        try:
            self.do_insert(msg)

            return True
        except Exception:
            self.log.error('error inserting on database')
            self.log.error(traceback.format_exc())
            return False

    def __del__(self):
        try:
            self.session.close()
        except Exception:
            pass

