from __writer import Writer

class FileWriter(Writer):
    def setup(self):
        if self.filepath:
            self.f = open(self.filepath, 'a+')
        else:    
            print 'Provide a file path in conf file'

    def write(self, msg):
        try:
           print msg
           self.f.write(msg)

           #force flush
           self.f.flush()

           return True
        except Exception, e:
            print e
            return False
