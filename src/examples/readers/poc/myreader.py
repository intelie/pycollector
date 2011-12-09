import filetail

from __reader import *
import re


class MyReader(Reader):
    def setup(self):
        self.tail = filetail.Tail("/tmp/http.log", max_sleep=1)
        self.csv = open('access.csv', 'w+')

    def read(self):
        while True:
            #readline
            line = self.tail.nextline()

            #split columns
            column_values = line.split('\t')

            #get interesting column values
            datetime = column_values[3]
            http_verb = column_values[6]

            #store with your business logic
            if http_verb == "GET":
                self.store({'datetime': datetime})

