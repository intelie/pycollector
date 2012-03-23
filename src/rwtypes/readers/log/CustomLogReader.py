

class CustomLogReader(LogReader):
    def to_add(self):
        return {'client': self.logpath.split('.')[1],
                'service': self.logpath.split('/')[3],
                'service_type': self.logpath.split('/')[5]}
