use strict;
use warnings;
use feature 'say';
use diagnostics;
use Parallel::ForkManager;
#use Benchmark::Timer;
use DBI;
use Cpanel::JSON::XS qw(encode_json decode_json);
my $start = time;
#Get user input
say "Please enter your MySQL user name";
my $username = <STDIN>;
say "Please enter your MySQL password";
my $password = <STDIN>;
say "Name Your Datbase";
my $db = <STDIN>;
say "How many cores do you want to use?";
my $Nforks = <STDIN>;
chomp ($username, $password, $db, $Nforks);
my $dir = "./";
my $fm = Parallel::ForkManager->new($Nforks);
my $dsn = "DBI:mysql:Driver={SQL Server}";
my %attr = (PrintError=>0, RaiseError=>1);
my $dbh = DBI->connect($dsn,$username,$password, \%attr);
$dbh->{mysql_enable_utf8} = 1;
my @ddl = (

	"CREATE DATABASE IF NOT EXISTS $db;",

	"USE $db;",
	"CREATE TABLE IF NOT EXISTS data (
	id varchar(50),
  parentid varchar(50),
  sub varchar(50),
  author varchar(50),
	body longtext,
	score int,
	ups int,
  gilded boolean,
  date date,
  time varchar(50),
  PRIMARY KEY (id)
	         ) ENGINE=InnoDB;",

);

for my $sql(@ddl){
  $dbh->do($sql);
}
say "All tables created successfully!";
say "Sorting...Hang on, this could take a while.";
my @fps = (glob("$dir/JSON/*"));

####################
####Main Routine####
####################
foreach (my $i = 0; $i < @fps; $i++) {
  my $fp = ($fps[$i]);
  open my $fh, "<", $fp or die "can't read open '$fp': $_";
  say "Reading in file $fp";
  while (<$fh>) {
    my $json = $_;
    my $decoded = decode_json $json;
    my $body = $decoded->{'body'};
    my $author = $decoded->{'author'};
    my $id = $decoded->{'id'};
    my $sub = $decoded->{'subreddit'};
    my $parentid = $decoded->{'parent_id'};
    my $ups = $decoded->{'ups'};
    my $score = $decoded->{'score'};
    my $gilded = $decoded->{'gilded'};
    my $dt = $decoded->{'created_utc'};
    my ($date, $time) = &DT ($dt);
    $body = &specialCharacters($body);
    next unless $body;
    next if $body =~ /\A\s*\Z/;
  	next if $body =~ /\[deleted\]/;
    my $sql = "INSERT INTO data(body, author, id, parentid, ups, score, gilded, date, time, sub)
    VALUES(?,?,?,?,?,?,?,?,?,?)";
    my $stmt = $dbh->prepare($sql);
    $stmt->execute($body, $author, $id, $parentid, $ups, $score, $gilded, $date, $time, $sub);

  }

    my $duration = time - $start;
	  if ($duration > 3600){
		    $duration = $duration / 3600;
		    say "\tI've been running for $duration hours now";
		}elsif($duration > 60){
				$duration = $duration / 60;
				say "\tI've been running for $duration minutes now";
		}else{
				say "\tI've been running for $duration seconds now";
			}
	$fm->finish;
	}

$fm-> wait_all_children;


sub DT{
      my $date = shift;
			my ($sec,$min,$hour,$mday,$mon,$year,$wday,$yday,$isdst) = localtime($date);
			$mon++;
			$sec  = "0$sec"  if length($sec)==1;
			$min  = "0$min"  if length($min)==1;
			$hour = "0$hour" if length($hour)==1;
			$mday = "0$mday" if length($mday)==1;
			$date = ($year+1900)."-".$mon."-".$mday;
			my $time = $hour.":".$min.":".$sec;
			my $stamp = $date." ".$time;
			return ($date, $time);
}


sub specialCharacters {
	my $body = shift;
	# get rid of links
	$body =~ s/\[(.*?)\]\( ?https?:.*?\)/$1/g; 		# [text](http://link.com)
	$body =~ s/\( ?https?:.*?\)//g; 				# (http://link.com)
	$body =~ s/http\S*?\Z/ /g; 						# end of line
	$body =~ s/http\S*?\s/ /g; 						# any other free-standing ones
	# All html
	$body =~ s{\&lt;}{<}g;
	$body =~ s{\&gt;}{>}g;
	$body =~ s{\&amp;}{\&}g;
	$body =~ s{\&nbsp;}{ }g;

	$body =~ s{\&[0-9a-z]+?;}{ };

	$body =~ s{\\u[0-9a-g]+}{}g;

	# All escape characters
	$body =~ s{\\\"}{"}g;
	$body =~ s{\\n}{ }g;
	$body =~ s{\\r}{ }g;
	$body =~ s{\\t}{ }g;


	return $body;
}
