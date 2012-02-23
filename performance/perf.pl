# File: perf.pl
# Description: pycollector performance tests
#              this script outputs a csv file with metrics
# Usage: perl perf.pl
# Dependencies (binaries): pidstat
# Notes: Tested under perl 5.12.4


use feature qw(say);
use IO::Handle;


my $pycollector_path='/home/kaiser/workspace/pycollector';

sub start_pycollector {
    `$pycollector_path/src/./pycollector --stop`;
    `$pycollector_path/src/./pycollector --start`
}

sub get_pid {
    substr(`ps aux | grep -v grep | grep -E "pycollector .*start" | awk '{print \$2}'`, 0, -1);
}

sub get_total_memory_data {
    ('TotalMem' => substr(`free -b | grep Mem: | awk '{print \$2/1048576}'`, 0, -1));
}

sub get_file_descriptors_data {
    my $pid = get_pid;
    ('FileDescriptors' => substr(`sudo ls -l /proc/$pid/fd | wc -l`, 0, -1));
}

sub get_pidstat_data {
    my $pid = shift;
    my $pidstat_data = `pidstat -hdru -p $pid 1 1`;
    my @pidstat_lines = split('\n', $pidstat_data);

    my @pidstat_keys = split(' ', @pidstat_lines[2]);

    #getting rid of '#' from pidstat output
    my @keys = @pidstat_keys[1..@pidstat_keys-1]; 
    my @values = split(' ', @pidstat_lines[3]);

    map {$_.strip} @keys;
    map {$_.strip} @values;

    my %data;
    @data{@keys} = @values;

    delete $data{Command};

    return %data;
}

sub get_data {
     my $data = {get_pidstat_data(get_pid), 
              get_total_memory_data,
              get_file_descriptors_data};
     return $data;
}

sub run {
    my $data = get_data;

    my $io = IO::Handle->new();
    open(my $fh, '>>output.csv');
    $io->fdopen($fh, 'w');

    #header of csv
    $io->say(join(',', keys(%$data)));
 
    while (1) {
        $data = get_data;
        $io->say(join(',', values(%$data)));
        $io->flush;
        sleep 5;
    }
}

say "Starting test...";
start_pycollector;
run;
