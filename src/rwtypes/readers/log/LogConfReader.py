import re

from __exceptions import ConfigurationError


class LogConfReader:
    @classmethod
    def validate_conf(cls, conf):
        counts = conf.get('counts', [])
        sums = conf.get('sums', [])
        confs = counts + sums
        for c in confs:
            if 'groupby' in c and \
                not 'match' in c['groupby']:
                raise ConfigurationError("'groupby' must have a 'match' regexp entry in your conf.")

            if 'groupby' in c and \
                not 'column' in c['groupby']:
                raise ConfigurationError("'groupby' must have a 'column' entry in your conf.")

            if 'groupby' in c:
                try:
                    groups = re.compile(c['groupby']['match']).groups
                except Exception, e:
                    raise ConfigurationError("'match' regexp: %s is not valid." % c['groupby']['match'])

                if groups != 1:
                    raise ConfigurationError("'groupby' must contain a 'match' regexp with exactly one group in your conf.")

