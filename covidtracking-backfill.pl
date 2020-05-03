#! /usr/bin/env perl

use strict;
use warnings;

use DateTime;
use Mojo::Base -strict, -async_await;
use Mojo::UserAgent;
use Mojo::URL;
use Mojo::JSON;
use Mojo::Graphite::Writer;
use Term::ProgressBar;

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
$opt{scope}     //= "states/daily";
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
print "Received ". @$apiResult . " objects back\n";
my @fields = (
  "positive",
  "negative",
  "pending",
  "hospitalized",
  "death",
  "total",
);

my $total = (@$apiResult * 86400);
my $progress = 0;
my $progress_bar = Term::ProgressBar->new({
    name    => 'progress',
    count   => $total,
    ETA     => 'linear',
  });

foreach my $entryRef (@$apiResult) {
  my $date= $entryRef->{date};
  my $state = $entryRef->{state};
  $date =~ /(\d{4})(\d{2})(\d{2})/ or die;
  my $dt = DateTime->new( year => $1, month => $2, day => $3, time_zone => 'Etc/GMT');
  $dt = $dt->subtract(days => -1); # the API returns yesterday's values as today
  $date = $dt->epoch;
  my $offset = 0;
  my @metrics;
  while ($offset < 86400) { # iterate over each minute in the day
    my $timestamp = ($date + $offset);
    foreach my $field (@fields) {
      my $data = $entryRef->{$field} //= "0";
      push @metrics, "$opt{prefix}.$state.$field $data $timestamp";
    }
    $progress+=60;
    $progress_bar->update($progress);
    $offset+=60;
  }
  writeToGraphite(@metrics)->catch(sub{warn pop})->wait;
}
#foreach (@metrics) { print "$_\n"};
async sub writeToGraphite {
  my @payload = @_;
  my $graphite = Mojo::Graphite::Writer->new(address => $opt{graphite})or die;
  my $connection = await $graphite->connect;
  $connection->timeout(10);
  await $graphite->write(@payload);
  return 1;
}

