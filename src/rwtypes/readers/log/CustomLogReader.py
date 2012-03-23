import traceback

from LogReader import LogReader


class CustomLogReader(LogReader):
    def to_add(self):
        return {'client': self.logpath.split('.')[1],
                'service': self.logpath.split('/')[3],
                'service_type': self.logpath.split('/')[5]}

    def sum_filter(self):
        data = self.current_line
        try:
            if data['x-event'] == 'disconnect':
                return True
            elif data['x-category'] == 'stream' and \
                data['x-event'] == 'destroy' and \
                data['c_proto'].find('rtmp') < 0 and \
                data['x-suri'][:4].find('rtmp') < 0:
                return True
        except Exception, e:
            self.log.debug("Error applying sum filter.")
            self.log.debug(traceback.format_exc())
        return False
