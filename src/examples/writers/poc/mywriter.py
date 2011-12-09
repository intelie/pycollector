from __writer import *


class MyWriter(Writer):
    def setup(self):
        #state saved during writer processing
        #useful for aggregating
        self.state = {}

        self.csv = open('access.csv', 'r+')


    def write(self, msg):

        #get msg values
        datetime = msg['datetime']
        datetime = ''.join(datetime.split(':')[1:4]).replace(' +', '').replace(']', '') #bad code, but works

        #first time, saving a new key in state
        if len(self.state) == 0:
            self.state[datetime] = {'count' : 1} 
            return True

        #aggregating if already exists
        if datetime in self.state:
            self.state[datetime]['count'] += 1
            return True

        #record if aggregating finished
        else:
            for dt in self.state:
                self.csv.write("%s,%s\n" % (dt, self.state[dt]['count']))
            #next state
            self.state = {}
            self.state[datetime] = {'count' : 1}
            
        return True

