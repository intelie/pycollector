from __exceptions import ConfigurationError

class LogConfReader:
    @classmethod
    def validate_conf(cls, conf):
        if 'groupby' in conf and \
            not 'match' in conf['groupby']:
            raise ConfigurationError("'groupby' must have a 'match' regexp entry in your conf.")

        if 'groupby' in conf and \
            not 'column' in conf['groupby']:
            raise ConfigurationError("'groupby' must have a 'column' entry in your conf.")

