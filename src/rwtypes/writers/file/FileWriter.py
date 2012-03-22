import logging
import traceback

from __writer import Writer


class FileWriter(Writer):
    """Conf:
        - filepath (optional): location of file,
            e.g. /home/user/myfile (default: /tmp/timestamp)"""

    def setup(self):
        self.log = logging.getLogger('pycollector')

        self.required_confs = ['filepath']
        self.check_required_confs()
        self.f = open(self.filepath, 'a+')

    def write(self, msg):
        try:
           self.f.write(str(msg))
           self.f.flush()

           return True
        except Exception:
            self.log.error('error writing file')
            self.log.error(traceback.format_exc())
            return False
