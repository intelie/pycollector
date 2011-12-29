import os

from __reader import Reader

class AdhocReader(Reader):
    def read(self):
        try:
            self.store(os.popen('acpi').read())   
        except Exception, e:
            print 'error reading'
            print e
