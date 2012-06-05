import os
import time
import logging
import socket

from subprocess import Popen, PIPE

from __reader import Reader
from __message import Message

class MtrReader(Reader):
    """Conf:
        - host (required): target host
        - samples: number of samples
    """
    def setup(self):
        self.log = logging.getLogger('pycollector')
        self.check_conf(['host'])
        self.samples = 5

    def check_conf(self, items):
        for item in items:
            if not hasattr(self, item):
                self.log.error('%s not defined in conf.yaml.' % item)
                exit(-1)

    def read(self):
        try:
            target_host = str(self.host)
            # print "mtr -c %d --split %s" % (self.samples, self.host)
            
            p = Popen("mtr -c %d --split %s" % (self.samples, self.host), shell=True, stdout=PIPE, stderr=PIPE, close_fds=True)
            stdout, stderr = p.communicate('through stdin to stdout')
            if p.returncode > 0:
                if p.returncode == 127: # File not found, lets print path
                    self.log.error("Unable to find mtr, check that it is installed")
                else:
                    self.log.error("Unable to run mtr: %s" % (stderr.strip()))
                return False

            mtr_output = {}
            maxHop = 0
            for line in stdout.split("\n"):
                if not line:
                    continue
                line.strip()
                
                (hop, address, loss, received, sent, best, avg, worst) = line.split(" ")

                dataId = "%s-%s" % (hop, address)
                maxHop = max(int(hop), maxHop)

                mtr_output[dataId] = dict(hop_address=address, hop=hop,
                        samples=self.samples,
                        loss_packed=loss, sent_packets=sent, avg_rtt=avg, best_rtt=best,
                        worst_rtt=worst, host=target_host)

            for k in mtr_output:
                data = k
                m = Message(content=mtr_output[k])
                self.store(m)

            #date = time.strftime("%b %d %H:%M:%S")
            return True
        except Exception, e:
            self.log.error('error reading')
            self.log.error(e)
            return False

