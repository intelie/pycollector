from third import filetail

from __reader import Reader


class LogReader(Reader):
    def setup(self):
        self.tail = filetail.Tail(self.log_path, max_sleep=1)
        
    def read(self):
        while True:
            line = self.tail.nextline()

            #XXX: remove \n?
            line = line.strip()

            if hasattr(self, 'delimiter'):
                values = line.split(self.delimiter)

                if hasattr(self, 'columns'):
                    column_values = dict(zip(self.columns, values))
                    #XXX: if we have columns, save it as a dict
                    self.store(column_values)
                    continue

                #XXX: if we have just a delimiter, save the list
                self.store(values)
                continue

            self.store(line)
