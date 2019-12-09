#! /usr/bin/perl

# Script to 1) read STDIN containing the k8s template yaml; and
#           2) open the pipe definition file specified in arg $1, to replace
#              into the value {{ pipeDefinition }} found in the template; and
#           3) perform replacements using $2 which is assumed to be a list of the form:
#              k=v,k=v,...
# Print the result to STDOUT.

use strict;
use warnings;

my $pipe_file=shift;
my $values_list=shift;  # k=v,k=v,...

# Read all of the pipe definition file.

open my $pf, "<", "$pipe_file" or die "Error opening file $pipe_file: $!";
my @pipe = <$pf>;

for my $idx (0 .. $#pipe) {  # for each line in the pipe...fix the yaml positioning.
    $pipe[$idx] =~ s/^/    /
}
my $pipe_str = join('', @pipe);  # flatten the array of pipe lines

# Load the template from STDIN.

local $/;  # unset the new line separator.
my $template = <STDIN>;  # read all of STDIN.

# Replace pipe definition into the template.

$template =~ s/\{\{ pipeDefinition \}\}/$pipe_str/;

# Replace other supplied values using supplied k=v list.

my @values = split /,/, $values_list;
foreach my $kv (@values) {  # for each key=value in the list of values...
    my ($k, $v) = split /=/, $kv;
    $template =~ s/\{\{ $k \}\}/$v/g;
}

print $template
