import filetail

from __reader import Reader


class LogReader(Reader):
    def setup(self):
        self.tail = filetail.Tail(self.log_path, max_sleep=1)
        
    def read(self):
        while True:
            line  = self.tail.nextline()
            self.store(line)


