from __exceptions import ConfigurationError

class LogConfReader:
    @classmethod
    def validate_conf(cls, conf):
        counts = conf.get('counts', [])
        sums = conf.get('sums', [])
        for sum in sums:
            if 'groupby' in sum and \
                not 'match' in sum['groupby']:
                raise ConfigurationError("'groupby' must have a 'match' regexp entry in your conf.")

            if 'groupby' in sum and \
                not 'column' in sum['groupby']:
                raise ConfigurationError("'groupby' must have a 'column' entry in your conf.")

