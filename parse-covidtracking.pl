#! /usr/bin/env perl

use strict;
use warnings;

use Mojo::Base -strict, -async_await;
use Mojo::UserAgent;
use Mojo::URL;
use Mojo::JSON;
use Mojo::Graphite::Writer;

use Getopt::Long qw(:config gnu_getopt no_auto_abbrev no_ignore_case);
use Pod::Usage;

my @options = (
  'help|?',
  'manpage|m',
  'host|h=s',
  'scope|s=s',
  'filter|f=s',
  'prefix|P=s',
  'output|o=s',
);

# collect arguments and print help information if necessary
my %opt = ();
GetOptions(\%opt, @options) or die (pod2usage(1));

pod2usage(1) if delete($opt{help});
pod2usage( -verbose =>2 ) if delete $opt{manpage};

#set up defaults
$opt{host}      //= "covidtracking.com";
$opt{scope}     //= "states/current";
$opt{prefix}    //= "covid19.covidtracking_com";
$opt{output}    //= "pretty";
$opt{interval}  ||= $ENV{COLLECTD_INTERVAL} || 10;
$opt{graphite}  //= "10.2.1.12";

my $apiTarget = "https://$opt{host}/api/v1/$opt{scope}.json";
if ($opt{filter}) {
  $apiTarget = $apiTarget."?".$opt{filter};
}

my $apiResult = Mojo::UserAgent->new->get("$apiTarget")->result->json;
$apiResult = [ $apiResult ] if ref $apiResult eq 'HASH';
my @fields = (
  "positive",
  "negative",
  "pending",
  "hospitalized",
  "death",
  "total",
);

if ( $opt{output} eq "pretty" ) {
  foreach my $state (@{$apiResult}) {
    print "State: $state->{state}\n";
    foreach my $field (@fields) {
      my $data = $state->{$field} //= "0";
      print "  $field: $data\n";
    }
  }
}

if ( $opt{output} eq "collectd" ) {
  foreach my $state (@{$apiResult}) {
    foreach my $field (@fields) {
      my $data = $state->{$field} //= "0";
      outputCollectd("$state->{state}", "$field", "$data");
    }
  }
}


sub outputCollectd {
  my $state     = shift;
  my $paramater = shift;
  my $value     = shift;
  my $timestamp = time();
  # Clean up output for ingestion
  # remove any non-numeric values
  $value =~ s/[^0-9\.]//g;
  # output data
  printf("PUTVAL \"$opt{prefix}/$state/gauge-$paramater\" interval=$opt{interval} $timestamp:$value\n");
}

if ( $opt{output} eq "graphite" ) {
  my @metrics;
  foreach my $stateRef (@$apiResult) {
    my $state = $stateRef->{state};
    my $timestamp = time();
    foreach my $field (@fields) {
      my $data = $stateRef->{$field} //= "0";
      push @metrics, "$opt{prefix}.$state.$field $data $timestamp";
    }
  }
  #  foreach my $metric (@metrics) {print "$metric\n"};
  writeToGraphite(@metrics)->catch(sub{warn pop})->wait;
}
async sub writeToGraphite {
  my @metrics = @_;
  my $graphite = Mojo::Graphite::Writer->new(address => $opt{graphite})or die;
  my $connection = await $graphite->connect;
  $connection->timeout(0);
  await $graphite->write(@metrics);
  return 1;
}

