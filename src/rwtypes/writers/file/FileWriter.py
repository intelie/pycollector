from __writer import Writer

class FileWriter(Writer):
    """Conf:
        - filepath (optional): location of file,
            e.g. /home/user/myfile (default: /tmp/timestamp)"""

    def setup(self):
        if hasattr(self, 'filepath'):
            self.f = open(self.filepath, 'a+')
        else:    
            self.f = open('/tmp/%s' % time.time(), 'a+')

    def write(self, msg):
        try:
           self.f.write(msg)

           #force flush
           self.f.flush()

           return True
        except Exception, e:
            print e
            return False
