import re

from __exceptions import ConfigurationError


class LogConfReader:
    @classmethod
    def validate_conf(cls, conf):
        counts = conf.get('counts', [])
        sums = conf.get('sums', [])
        #XXX: refactor me
        for sum in sums:
            if 'groupby' in sum and \
                not 'match' in sum['groupby']:
                raise ConfigurationError("'groupby' must have a 'match' regexp entry in your conf.")

            if 'groupby' in sum and \
                not 'column' in sum['groupby']:
                raise ConfigurationError("'groupby' must have a 'column' entry in your conf.")

            try:
                if re.compile(sum['groupby']['match']).groups != 1:
                    raise ConfigurationError("'groupby' must contain a 'match' regexp with exactly one group in your conf.")
            except Exception, e:
                raise ConfigurationError("'match' regexp is not valid.")

        for count in counts:
            if 'groupby' in count and \
                not 'match' in count['groupby']:
                raise ConfigurationError("'groupby' must have a 'match' regexp entry in your conf.")

            if 'groupby' in count and \
                not 'column' in count['groupby']:
                raise ConfigurationError("'groupby' must have a 'column' entry in your conf.")

            try:
                if re.compile(count['groupby']['match']).groups != 1:
                    raise ConfigurationError("'groupby' must contain a 'match' regexp with exactly one group in your conf.")
            except Exception, e:
                raise ConfigurationError("'match' regexp is not valid.")
