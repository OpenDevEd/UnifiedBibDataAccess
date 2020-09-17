package My::UnifiedBibDataAccess;
 
use Exporter qw(import); 

my @exportedItems = qw(uba ubaFile ubaShow prettyJson ubaUsage protoStat protoFields utify utifyItem lutify readRIS);
our @EXPORT_OK = @exportedItems;

use warnings;
use strict;
use feature qw{ say };
use open IO => ':encoding(UTF-8)', ':std';
binmode STDOUT, ':encoding(UTF-8)';
use Encode qw( encode_utf8 decode_utf8 );
use Encode::Guess;
use Encoding::FixLatin qw(fix_latin);
use utf8;
use 5.18.2;
use feature 'unicode_strings';
use JSON qw( decode_json  encode_json );
use Data::Dumper;
use URI::Escape;
use My::Utilities qw(pauseEvery);

my $me = "UnifiedBibDataAccess";

my $verbose = "";

my %repoalias = qw(
ES ESSA
GS GoogleScholar
ER Zotero
ERIC Zotero
eric Zotero
JL Zotero
Jolis Zotero
jolis Zotero
jolisprimo Zotero
Z Zotero
DO DOAJ
doaj DOAJ
CR CrossRef
crossref CrossRef
SC Elsevier
scopus Elsevier
SD Elsevier
sciencedirect Elsevier
BJ BibJson
PQED ProQuestXML
PQEDXML ProQuestXML
PQEDJSON ProQuestJSON
PQEDTEXT TEXT
PQER ProQuestXML
PQERXML ProQuestXML
PQERJSON ProQuestJSON
PQERTEXT TEXT
pq ProQuestJSON
pqeducation ProQuestJSON
pqeric ProQuestJSON
ProQuestERIC ProQuestXML
ProQuestERICXML ProQuestXML
ProQuestERICJSON ProQuestJSON
ProQuestERICTEXT TEXT
ProQuestEducation ProQuestXML
ProQuestEducationXML ProQuestXML
ProQuestEducationJSON ProQuestJSON
ProQuestEducationTEXT TEXT
WS WebOfScienceXML
WSXML WebOfScienceXML
WSJSON WebOfScienceJSON
WSTEXT TEXT
wosed WebOfScienceJSON
wosso WebOfScienceJSON
scielo WebOfScienceJSON
    );

foreach (keys %repoalias) {
    $repoalias{$repoalias{$_}} = $repoalias{$_};
};

my %process = (
    # Text-based formats
    ESSA => \&processESSA,
    GoogleScholar => \&processGoogleScholar,
    # json-based formats
    Zotero  => \&processZotero,
    Elsevier => \&processElsevier,
    DOAJ => \&processDOAJ,
    BibJson => \&processBibJson,
    CrossRef => \&processCrossRef,
    # XML-based formats
    ProQuest => \&processProQuestXML,
    ProQuestXML => \&processProQuestXML,
    ProQuestJSON => \&processProQuestJSON,
    WebOfScience => \&processWebOfScienceXML,
    WebOfScienceXML => \&processWebOfScienceXML,
    WebOfScienceJSON => \&processWebOfScienceJSON,
    TEXT => \&processTEXT
    );

my @protoFields = (
    "title", 
    "additionalTitles", 
    "authors", 
    "year", 
    "publicationType", 
    "containerName", 
    "doi", 
    "isbn", 
    "links",
    "citation",
    "keywords", 
    "abstract",
    "identifier", 
    "itemdata", 
    "itemdatatype",
    "itemdatahandler"
    );

my @protoStat = ("total","pageSize","page","ubaHandler");

my %protoFields = ();
my %protoStat = ();

foreach (@protoFields) {
    $protoFields{$_} = "";
};
$protoFields{"itemdatatype"} = "json";

foreach (@protoStat) {
    $protoStat{$_} = "";
};


sub whichpm() {
    #usage $0 Some::Module 
    my $file = $_[0];
    my $f;
    my $m;
    ($f=($m=$file).".pm")=~s{::}{/}g;
    eval "require $m" and return $INC{$f}.$/;
};

sub whichpmpath() {
    my $me = $_[0];
    my $here = &whichpm("My::$me");
    $here =~ s/\/$me\.pm//;
    $here =~ s/\n//;
    return $here;
};


# Find $xmljson:
my $here = &whichpmpath($me);

my $xmljson = "$here/xml2json.js";
die("No $xmljson") if !-e $xmljson;

#important
1;

# ------------- functions --------------------
sub protoFields() {
    return @protoFields;
};

sub protoStat() {
    return @protoStat;
};

sub ubaUsage() {
    
say "

Usage:

use My::UnifiedBibDataAccess qw(@exportedItems);
&ubaUsage();
# supply data type
print &ubaShow(&uba(\"ESSA\",\$string)); 
# detect type from filename
print &ubaShow(&ubaFile(\"?\",\$filename)); 
# or detect file type from a string, such as the filename: 
print &ubaShow(&uba(\$filename,\$f)); 

The possible types are shown below.

my \@items = &ubaFile(\"GS\",\$somefile);
          = &uba(\"SD\",\$anystring);
print &ubaShow(\@items); 

";
foreach (keys %repoalias) {
    if (!$process{$repoalias{$_}}) {
	print "WARNING: No subrouting associated with $repoalias{$_}\n";
    };
};
say "Available types (associated with functions):\n\t";
say join("\n\t",sort keys %process);
say "\nAvailable types (aliases): ";
foreach (sort keys %repoalias) {
    next if $_ eq $repoalias{$_};
    say "\t$_ -> $repoalias{$_}";
};


};

sub ubaFile() {
    (my $type, my $file) = @_;    
    if ($type eq "" || $type eq "?") {
	foreach my $k (keys %repoalias) {
	    if ($file =~ m/\_$k\-/ || $file =~ m/\/$k\./) {
		# say "Detected type: $k -> $repoalias{$k}";
		$type = $repoalias{$k};
	    };
	};
    };
    if ($type =~ m/(ProQuest|WebOfScience)/i) {
	my $repo = $1;
	if (
	    ($file =~ m/\.(te?xt)$/i && $type !~ m/(TEXT)/)
	    ||
	    ($file =~ m/\.(xml)$/i && $type !~ m/(XML)/)
	    ||
	    ($file =~ m/\.(json)$/i && $type !~ m/(JSON)/)
	    )
	{
	    print "Repository $repo with type $type. However, file has extention $1 and type does not indicate $2. Expect an error.";
	};

    };
    open F,"$file" or die("Sorry,  $me\:ubaFile could not open file $_");
    my @f = <F>;
    close F;
    my $f = join("",@f);
    if ($type ne "GoogleScholar") {
	$f =~ s/\n__META__\n.*$//s;
    };
    return &uba($type,$f);
};

sub ubaShow() {
    my @items = @_;
    if ($items[0] eq "0") {
	print STDERR "UnifiedBibDataAccess::ubaShow: Error in input\n";	
    };
    my %stat = %{$items[0]};
    my $str = "";
    foreach (@protoStat) {
	$str .= "$_: $stat{$_}\n";
    };
    my $n = 0;
    foreach (@items[1..$#items]) {
	$str .= "---- number: $n ----\n";
	$n++;
	my %x = %{$_};
	foreach (@protoFields) {
	    if ($x{$_} ne "") {
		if ($_ eq "itemdata") {
		    if ($x{itemdatatype} eq "json") {
			$str .= "$_ (decoded json / Dumper):\n".Dumper(decode_json(encode_utf8($x{itemdata})))."\n";
		    } elsif ($x{itemdatatype} eq "text") {
			$str .= "$_ (text):".popOut($x{itemdata})."\n";
		    } else {
			$str .= "$_ (unknown): $x{$_}\n";
		    };
		} elsif ($_ eq "links") {		
		    $str .= "$_: ".popOut($x{$_})."\n";
		} else {
		    $str .= "$_: $x{$_}\n";
		};
	    } else {
		$str .= "$_: <MISSING>\n";
	    };
	};
    };
    return $str;
};

sub popOut() {
    my $z = $_[0];
    $z =~ s/\n$//s;
    if ($z =~ m/\n/s) {
	my @a = split /\n/,$_[0];
	my $a =  "\n";
	foreach (@a) {
	    $a .=  "\t|\t$_\n";
	};
	return $a;
    } else {
	return $z;
    };
};

sub uba() {
    (my $type, my $string) = @_;
    # This fn expects a type - but if a filename-type string is given as type, the type is detected from the string.
    # Moreover, __META__ is removed from the string if necessary.
    if ($type =~ /^\?/ || $type =~ /\W/) {
	my $found = "";
	foreach my $k (keys %repoalias) {
	    # if more than one matches... it becomes a bit random....
	    if ($type =~ m/\_$k\-/ || $type =~ m/\/$k\./) {
		### say "Detected type: $k -> $repoalias{$k}";
		$found = $repoalias{$k};
	    };
	};
	$type = $found if $found;
	if ($type ne "GoogleScholar") {
	    $string =~ s/\n__META__\n.*$//s;
	};
    };
    #say "$type, $string";
    if ($repoalias{$type}) {
	$type = $repoalias{$type};
    };
    #print STDERR "-->$type\n";
    if ($process{$type}) {
	# &getData($jstring,$repo,$term_string,$file);
	print "PROCESSING WITH \$process{$type}\n" if $verbose;
	my @item = $process{$type}->($string);	
	${$item[0]}{ubaHandler} = $type;
	foreach (@item[1..$#item]) {
	    ${$_}{itemdatahandler} = $type;
	};
	return @item;
	# each field is a hash:
	# %{$item[0]} is the statistics
	# %{$item[1..$#item]} are the fields
    } else {
	return ("0");
    };
};

sub prettyJson() {
    use IPC::Open2;
    use Symbol;
    my $WTR = gensym();  # get a reference to a typeglob
    my $RDR = gensym();  # and another one
    my $pid = open2($RDR, $WTR, 'python -m json.tool');
    print $WTR $_[0];
    close($WTR);    # finish sending all output to sort(1)    
    my $out = join "",<$RDR>;
    return $out;
    waitpid($pid, 0);    
};

#----------------------- internal functions -----------------------------

sub fixTitle() {
    my $a = $_[0];
    $a = lc($a);
    $a =~ s/^\s*//;
    $a =~ s/\s*$//;
    return $a;
};

sub getValue() {
    if ($#_ == 0) {
	if (defined $_[0]) {
	    return $_[0];
	} else {
	    return "";
	};
    } elsif ($#_ > 0) {
	my @a = ();
	foreach (@_) {
	    if (defined $_) {
		push @a,$_;
	    } else {
		push @a,"";
	    };
	};
	return @a;
    } else {
	return "";
    };
};
sub getArrayFromScalar() {
    if ($#_ == 0) {	
	if (defined $_[0]) {
	    return @{$_[0]};
	} else {
	    return ("");
	};
    } else {
	return &getValue(@_);
    };
};


sub makeArrayFromRef() {
    if ($#_ == -1) {
	die("You must pass a reference to makeArrayFromRef");
	return 0;
    } elsif ($#_ == 0) {	
	if ($_[0]) {
	    if (ref $_[0] eq "ARRAY") {
		return @{$_[0]};
	    } elsif (ref $_[0] eq "SCALAR") {
		return (${$_[0]});
	    } elsif (ref $_[0] eq "HASH") {
		return ($_[0]);
	    } else {
		say("You must pass a reference to an ARRAY, SCALAR, HASH to makeArrayFromRef, not ",ref $_[0],"->",%{$_[0]});
		return 0;
	    };
	} else {
	    say("Undefined valued passed to makeArrayFromRef.");
	    return ();
	};
    } else {
	die("You must pass a single reference to makeArrayFromRef");
	return 0;
    };
};


sub makeArrayFromRefHint() {
    if ($#_ == 1) {	
	if ($_[0]) {
	    if (ref $_[0] eq "ARRAY") {
		return @{$_[0]};
	    } elsif (ref $_[0] eq "SCALAR") {
		return (${$_[0]});
	    } elsif (ref $_[0] eq "HASH") {
		return ($_[0]);
	    } else {
		say("You must pass a reference to an ARRAY, SCALAR, HASH to makeArrayFromRefHint, not ",ref $_[0],"->",%{$_[0]});
		return 0;
	    };
	} else {
	    say("Undefined valued passed to makeArrayFromRefHint. Hint: $_[1]");
	    return ();
	};
    } else {
	die("You must pass a single reference and one hint to makeArrayFromRefHint");
	return 0;
    };
};


#--------------------- parsing ----------------------------

# Text-based formats:
sub processTEXT() {
    my %stat = %protoStat; 
    my @items = ();
    my @oitems = ();
    my $text = $_[0];
    #TODO should split text on RECORD:: or similar... @items = split /.../,...;
    if ($text =~ m/\nRECORD\:\:\n/s) {
	say "SPLITTING INPUT TEXT";
	@items = split /\nRECORD\:\:\n/s, $text;
	$stat{total} = $#items+1;
	push @oitems,\%stat;
    } else {
	say "KEEPING INPUT TEXT (TEXT)";
	$stat{total} = 1;
	push @oitems,\%stat;
	@items = ($text);
    };
    foreach (@items) {
	my %x = %protoFields;
	$x{itemdata} = $_;
	$x{itemdatatype} = "text";
	my @a = split /\n/,$_;
	foreach my $line (@a) {
	    foreach my $k (keys %x) {
		(my $m = $k) =~ s/s$//;
		if ($line =~ m/^(?:$m)s?(?:\:?\s+)(\S.*)$/i) {
		    $x{$k} = $1;
		};
	    };
	    #proquest
	    if ($line =~ m/^(?:daterange)(?:\:?\s+)d(\d\d\d\d)/) {
		$x{year} = $1;
	    };
	};
	push @oitems, \%x;
    };
    return @oitems;
};

sub myExists {
    my $hash = shift;
    my $key = shift;
    return 0 if ( ref $hash ne ref {} || ! exists $hash->{$key} );
    # return deep_exists( $hash->{$key}, @_ ) if @_;
    return 1;
}

sub getIDericjolis() {
    my $u = $_[0];
    #say "IN $u";
    if ($u =~ m/eric/ && $u =~ s/.*id\=(\w+\d+)(\&.*)?$/$1/s) {
	$u = "eric:$u";
    } elsif ($u =~ m/oclcNumber/ && $u =~ s/.*oclcNumber\=(\d+)(\&.*)?$/$1/s) {
	$u = "jolis:$u";
    } elsif ($u =~ m/\/oclc\// && $u =~ s/.*\Qjointbankfundlibrary.on.worldcat.org\E\/(?:oclc\/)?(\d+).*?$/$1/s) {
	# https://jointbankfundlibrary.on.worldcat.org/oclc/
	# jointbankfundlibrary.on.worldcat.org
	$u = "jolis:$u";
    } else {
	$u = "";
    };
    #print "DETECTED ID: $u\n";
    $u =~ s/\n//;
    #say "OUT $u";
    return $u;
};

sub processZotero() {
    # say "Zotero";
    # if ($repo eq "eric" || $repo eq "jolis")
    # Only one response.... but a Zotero response might have more
    my %stat = %protoStat; 
    my @items = ();
    my @oitems = ();
    #if (defined $json->{'items'}) {
    my $json;
    #say Dumper(decode_json(encode_utf8($_[0])));
    my $sessionmode = 0;
    $json = decode_json(encode_utf8($_[0]));    
    if (myExists($json,'items')) {
	# print "items...\n";
	if (myExists($json,'session')) {
	    # This is a session record
	    $sessionmode = 1;
	    my %items = %{$json->{'items'}};
	    foreach (keys %items) {
		my $id = &getIDericjolis($_);
		# print "JOLIS/ERIC: $id <- $_\n";
		#https://eric.ed.gov/?q=TVET+Africa&id=EDxxxxx
		#/share/citation.ris?oclcNumber=823380425&databaseIds=143%2C199%2C233%2C245%2C203%2C217%2C239%2C638%2C283%2C251%2C197%2C285
		push @items, {"url" => $_, "title" => $items{$_}, "dc:identifier" => $id};
	    };
	} else {
	    #say "This is a standard listing, in which \$json\-\>{items} is an array of items.";
	    # This is a standard listing, in which $json->{'items'} is an array of items.
	    #say Dumper($json);
	    eval { 
		@items = @{$json->{'items'}};
	    };	    
	    if ($@) {
		print "processZotero: Error in processAny: $@"."   while processing.\n";
		say Dumper($json);
	    };
	};
    } else {
	# say "Bare record";
	# This is a single record, without surrounding 'items'
	$json = decode_json("{\"items\": ".encode_utf8($_[0])."}");
	@items = @{$json->{'items'}};
    };
    $stat{total} = $#items;
    push @oitems,\%stat;
    #print Dumper(\@items);    
    #say "Session $sessionmode";
    #say Dumper($_[0]);
    foreach (@items) {
	# say "ITEM:";
	my %x;
	%x = %protoFields;
	$x{itemdata} = decode_utf8(encode_json($_));
	$x{doi} = &getValue($_->{DOI});
	$x{isbn} = &getValue($_->{ISBN});
	$x{title} = &getValue($_->{title});
	$x{publicationType} = &getValue($_->{'itemType'});
	$x{containerName} = &getValue($_->{'publicationTitle'});
	$x{identifier} = &getValue($_->{'dc:identifier'});
	if  ((!$x{identifier} || $x{identifier} eq "") && $x{url} && $x{url} =~ m/eric|jolis|jointbankfundlibrary/s) {
	    $x{identifier} = &getIDericjolis($x{url});
	};
	$x{year} = &getValue($_->{'date'});
	$x{year} =~ s/.*\b(\d\d\d\d)\b.*/$1/g;
	$x{abstract} = &getValue($_->{'abstractNote'});
	my @xxx;
	my @x;
	if ($_->{'creators'}) {
	    @xxx = &getArrayFromScalar($_->{'creators'});
	    foreach my $auth (@xxx) { 
		$x{authors} .= "[".&getValue($auth->{'creatorType'})."] ".&getValue($auth->{'lastName'}).", ".&getValue($auth->{'firstName'})."; ";
	    };
	};
	if ($_->{'tags'}) {
	    @x = &getArrayFromScalar($_->{'tags'});
	    foreach(@x) { 
		$x{keywords} .= &getValue($_->{'tag'})."; ";
	    };
	};	
	if ($_->{'attachments'}) {
	    @x = &getArrayFromScalar($_->{'attachments'});
	    foreach(@x) { 
		$x{links} .= "url\t".&getValue($_->{'url'})."\n";
	    };
	};
	if ($_->{'url'}) {
	    $x{links} .= "url\t".&getValue($_->{'url'})."\n";
	};
	if ((!$x{identifier}  || $x{identifier} eq "") && $x{links} && $x{links} =~ m/eric|jolis|jointbankfundlibrary/s) {
		$x{identifier} = &getIDericjolis($x{links});
	} else {
#	    print "id= ". $x{identifier};
	};
	# ISSN also available, but not helpful to organise by serial number.
	# print "$x{doi} $x{isbn} $x{title}\n";       
	push @oitems, \%x;
    };
    return @oitems;
};

sub processCrossRef() {
    my %stat = %protoStat; 
    my @items = ();
    my @oitems = ();
    my $json = decode_json(encode_utf8($_[0]));
    $stat{total} = $json->{'message'}->{'total-results'};
    push @oitems,\%stat;
    @items = @{$json->{'message'}->{'items'}};
    foreach (@items) {
	my %x = %protoFields;
	# add json string of the item
	$x{itemdata} = decode_utf8(encode_json($_));
	#	if ($repo eq "crossref") {
	$x{doi} = $_->{'DOI'} if $_->{'DOI'};
	my @isbn = @{$_->{'ISBN'}} if $_->{'ISBN'};
	$x{year} = $_->{'created'}->{'date-time'} if $_->{'created'}->{'date-time'};
	my @titles = @{$_->{'title'}} if $_->{'title'};	    
	$x{title} = $titles[0] if @titles;
	if ($#titles > 0) {
	    $x{additionalTitles} = join "; ",@titles[1..$#titles];
	};
	my @ctitles = @{$_->{'container-title'}} if $_->{'container-title'};
	$x{containerName} = $ctitles[0] if @titles;
	($x{abstract} = $_->{'abstract'}) =~ s/\n// if $_->{'abstract'};
	$x{links} = "Link\t".$_->{'URL'} if $_->{'URL'};	

	push @oitems, \%x;
    };
    return @oitems;
};

sub processElsevier() {
    #    } elsif ($repo eq "sciencedirect" || $repo eq "scopus") {
    my %stat = %protoStat; 
    my @items = ();
    my @oitems = ();
    my $json = decode_json(encode_utf8($_[0]));
    # print Dumper($json);
    if ($json->{'search-results'}) {
	$stat{page} = $json->{'search-results'}->{'opensearch:startIndex'};
	$stat{pageSize} = $json->{'search-results'}->{'opensearch:itemsPerPage'};
	$stat{total} = $json->{'search-results'}->{'opensearch:totalResults'};
	@items = @{$json->{'search-results'}->{'entry'}};
    } else {
	$stat{total} = "1";
	$json = decode_json("{\"items\": [".encode_utf8($_[0])."]}");
	@items = @{$json->{'items'}};
    };
    push @oitems,\%stat;
    foreach (@items) {
	my %x = %protoFields;
	$x{abstract} = $_->{'dc:description'} if $_->{'dc:description'};
	$x{title} = $_->{'dc:title'} if $_->{'dc:title'};
	$x{doi} = $_->{'prism:doi'} if $_->{'prism:doi'};
	foreach (@{$_->{'link'}}) {
	    if ($_->{'@ref'} eq "scidir") {
		$x{links} .= "Link\t$_->{'@href'}\n";
	    };
	};
	if ($_->{'dc:identifier'}) {
	    $x{identifier} =  $_->{'dc:identifier'};
	};
	# ScienceDirect
	if ($_->{'eid'} && !$x{identifier}) {
	    $x{identifier} =  $_->{'eid'};
	};
	if ($_->{'authors'}) {
	    # ScienceDirect
	    my %authors = %{$_->{'authors'}};
	    foreach my $authtype (keys %authors) {
		my @a = @{$authors{$authtype}};
		$x{authors} .= "\[$authtype\] ";
		foreach (@a) {
		    $x{authors} .= $_->{'surname'} if $_->{'surname'} ;
		    $x{authors} .= ", ".$_->{'given-name'} if $_->{'given-name'};
		    if ($_->{'surname'} || $_->{'given-name'}) {
			$x{authors} .= "; ";
		    };
		};
	    };
	} elsif ($_->{'author'}) {
	    # Scopus
	    my @authors = @{$_->{'author'}};
	    foreach (@authors) {
		$x{authors} .= $_->{'surname'} if $_->{'surname'} ;
		$x{authors} .= ", ".$_->{'given-name'} if $_->{'given-name'};
		if ($_->{'surname'} || $_->{'given-name'}) {
		    $x{authors} .= "; ";
		};
	    };
	} else {
	    $x{authors} = "[NA]";
	};
	$x{year} = $_->{"prism:coverDisplayDate"};
	if ($x{year}) {
	    $x{year} =~ s/^.*\b(\d\d\d\d)\b.*$/$1/;
	    $x{year} =~ s/\b(\d\d\d\d)\d+/$1/;
	};
	$x{publicationType} = $_->{"prism:aggregationType"};
	$x{containerName} = $_->{"prism:publicationName"};
	## add json string of the item
	$x{itemdata} = decode_utf8(encode_json($_));
	($x{keywords} = &getValue($_->{'authkeywords'})) =~ s/ \| /\; /sg;
	push @oitems, \%x;
    };
    return @oitems;
};

sub processDOAJ() {
    my %stat = %protoStat; 
    my @items = ();
    my @oitems = ();
    my $json = decode_json(encode_utf8($_[0]));
    # print Dumper($json);
    if ($json->{'results'}) {
	$stat{total} = $json->{'total'};
	$stat{pageSize} = $json->{'pageSize'};
	$stat{page} = $json->{'page'};
	@items = @{$json->{'results'}};
    } else {
	$stat{total} = 1;
	$json = decode_json("{\"items\": [".encode_utf8($_[0])."]}");
	@items = @{$json->{'items'}};
    };
    push @oitems,\%stat;
    foreach (@items) {
	my %x = %protoFields;
	## add json string of the item
	$x{itemdata} = decode_utf8(encode_json($_));
	$x{itemdatatype} = "json";
	# if ($repo eq "doaj") {
	# DOAJ
	my %y = %{$_};
	$x{title} = $y{'bibjson'}{'title'} if $y{'bibjson'}{'title'};
	if ($y{'bibjson'}{'identifier'}) {
	    my @i = @{$y{'bibjson'}{'identifier'}};
	    my $i = "";
	    foreach $i (@i) {
		if ($i->{'type'} eq "doi") {
		    $x{doi} = $i->{'id'};
		};
	    };
	};
	my $k = "";
	$x{links} .= "Link\thttps://doaj.org/article/$y{id}\n" if $y{id} ne "";
	foreach $k (@{$y{'bibjson'}{'link'}}) {
	    my $kct = "";
	    if ($k->{content_type}) {
		$kct = $k->{content_type};
	    };
	    my $kurl = $k->{url};
	    if ($kurl =~ m|//dx.doi.org/(.*)$|) {
		if ($x{doi} eq "") {
		    $x{doi} = $1;
		};
	    };
	    $x{links} .=  "Link\t$kct;$k->{type}\t$kurl\n";
	};
	my %z = %{$y{'bibjson'}};
	$x{"additionalTitles"} = &getValue($z{"additionalTitles"});
	$x{"ctitle"} = &getValue($z{"ctitle"});
 	my @x = &getArrayFromScalar($z{'author'});
	# should use an array here??
	$x{authors} = "[author] ";
	foreach(@x) {
	    if ($_) {
		$x{authors} .= &getValue($_->{'name'})."; ";
	    };
	};
	$x{"year"} = &getValue($z{"year"});
	$x{"publicationType"} = &getValue($z{"publicationType"});
	$x{"containerName"} = &getValue($z{"journal"}{"title"});
	$x{"isbn"} = &getValue($z{"isbn"});
	$x{"keywords"} = join("; ",&getArrayFromScalar($z{"keywords"}));
	$x{"abstract"} = &getValue($z{"abstract"});
	@x = &getArrayFromScalar($z{'tags'});
	push @oitems, \%x;
    };
    return @oitems;
};

# Template
sub processBibJson() {
    my %stat = %protoStat; 
    my @items = ();
    my @oitems = ();
    my $json = decode_json(encode_utf8($_[0]));
    # print Dumper($json);
    if ($json->{'results'}) {
	$stat{total} = &getValue($json->{'total'});
	$stat{pageSize} = &getValue($json->{'pageSize'});
	$stat{page} = &getValue($json->{'page'});
	@items = @{$json->{'results'}};
    } else {
	$stat{total} = 1;
	$json = decode_json("{\"items\": [".encode_utf8($_[0])."]}");
	@items = @{$json->{'items'}};
    };
    push @oitems,\%stat;
    foreach (@items) {
	my %x = %protoFields;
	## add json string of the item
	$x{itemdata} = decode_utf8(encode_json($_));
	$x{itemdatatype} = "json";
	$x{"title"} = &getValue($_->{"title"});
	$x{"additionalTitles"} = &getValue($_->{"additionalTitles"});
	$x{"authors"} = &getValue($_->{"authors"});
 	my @x = &getArrayFromScalar($_->{'creators'});
	foreach(@x) { 
	    $x{authors} .= "[".&getValue($_->{'creatorType'})."] ".&getValue($_->{'lastName'}).", ".&getValue($_->{'firstName'})."; ";
	};
	$x{"year"} = &getValue($_->{"year"});
	$x{"publicationType"} = &getValue($_->{"publicationType"});
	$x{"containerName"} = &getValue($_->{"containerName"});
	$x{"isbn"} = &getValue($_->{"isbn"});
	$x{"doi"} = &getValue($_->{"doi"});
	$x{"keywords"} = &getValue($_->{"keywords"});
	$x{"abstract"} = &getValue($_->{"abstract"});
	@x = &getArrayFromScalar($_->{'tags'});
	foreach(@x) { 
	    $x{keywords} .= &getValue($_->{'tag'})."; ";
	};       
	push @oitems, \%x;
    };
    return @oitems;
};


### XML-based formats

sub xmlToJSON() {
    use IPC::Open2;
    use Symbol;
    use open IO => ':encoding(UTF-8)', ':std';
    my $WTR = gensym();  # get a reference to a typeglob
    my $RDR = gensym();  # and another one
    my $pid = open2($RDR, $WTR, "node $xmljson");
    print $WTR encode_utf8($_[0]);
    close($WTR);    # finish sending all output to sort(1)    
    my $out = join "",<$RDR>;
    waitpid($pid, 0);
    return $out;
};

sub processWebOfScienceXML() {
    my $json = &xmlToJSON($_[0]);
    return &processWebOfScienceJSON($json);
};

sub getWOStype()  {
    my $ref = $_[0];
    my $key = $_[1];
    my $keywords = "";
    if ($ref->{$key."s"}) {
	my $count = &getValue($ref->{$key."s"}->{"_attributes"}->{"count"});
	# say "KEY/COUNT: $key ($count)" if $verbose;
	my @keywords = &makeArrayFromRefHint($ref->{$key."s"}->{$key},'$ref->{$key."s"}->{$key}');
	# say Dumper($_->{"static_data"}->{"fullrecord_metadata"}->{"keywords"});
	my @kawats =();
	foreach my $kw (@keywords) {
	    if ($key eq "name") {
		push @kawats, &getValue($kw->{"full_name"}->{"_text"});
	    } else {
		push @kawats, &getValue($kw->{"_text"});
	    };
	};
	$keywords = join("; ",@kawats);
    };
    return $keywords;
};

sub processWebOfScienceJSON() {
    my %stat = %protoStat; 
    my @items = ();
    my @oitems = ();
    my $json = decode_json(encode_utf8($_[0]));
    # print Dumper($json);
    if ($json->{'records'}) {
	if ($json->{'records'}->{'REC'}) {
	    if (ref $json->{'records'}->{'REC'} eq "ARRAY") {
		@items = @{$json->{'records'}->{'REC'}};
		$stat{total} = $#items+1;
		$stat{pageSize} = 100;
	    } else {
		@items = ($json->{'records'}->{'REC'});
		$stat{total} = 1;
		$stat{pageSize} = 100;
		#say "ERROR RECORDS->REC";
		#say Dumper($json->{'records'});
	    };
	} else {
	    #$stat{total} = 1;
	    #$json = decode_json("{\"items\": [".encode_utf8($_[0])."]}");
	    #@items = @{$json->{'items'}};
	    die("This will fail");
	    exit;
	};
    } else {
	$stat{total} = 0;
	push @oitems,\%stat;
	return;
    };
    push @oitems,\%stat;
    foreach (@items) {
	# say "--------------------- item -------------------------";
	my %x = %protoFields;
	## add json string of the item
	$x{itemdata} = decode_utf8(encode_json($_));
	$x{itemdatatype} = "json";
	# WOS id
	my $localID = &getValue($_->{"UID"}->{"_text"});
	# say "- $localID";
	$x{identifier} = $localID;
	my $names = &getWOStype($_->{"static_data"}->{"summary"},"name");
	#say "-> ".$names;
	$x{"authors"} = $names;
	my $keywords = &getWOStype($_->{"static_data"}->{"fullrecord_metadata"},"keyword");		
	#say "-> ".$keywords;
	$x{"keywords"} = $keywords;
	my $doctypes = &getWOStype($_->{"static_data"}->{"summary"},"doctype");		
	#say "-> ".$doctypes;
	$x{"publicationType"} = $doctypes;
	my $titles = "";
	if ($_->{"static_data"}->{"summary"}->{"titles"}) {
	    my $count = &getValue($_->{"static_data"}->{"summary"}->{"titles"}->{"_attributes"}->{"count"});
	    # say "titles: $count";
	    my @titles = &makeArrayFromRefHint($_->{"static_data"}->{"summary"}->{"titles"}->{"title"},'$_->{"static_data"}->{"summary"}->{"titles"}->{"title"}');
	    # say Dumper($_->{"static_data"}->{"summary"}->{"titles"});
	    my @kawats =();
	    foreach my $kw (@titles) {
		my $tx = $kw->{"_text"};
		my $ty = $kw->{"_attributes"}->{"type"};
		# say "TITLE: $ty: $tx";
		if ($ty eq "item") {
		    $x{"title"} = $tx;
		} elsif ($ty eq "source") {
		    $x{"containerName"} = $tx;
		} else {
		    $x{"additionalTitles"} .= "[$ty] $tx;";
		}
	    };
	};
	$x{"year"} = &getValue($_->{"static_data"}->{"summary"}->{'pub_info'}->{"_attributes"}->{"pubyear"});
	$x{"containerType"} = &getValue($_->{"static_data"}->{"summary"}->{'pub_info'}->{"_attributes"}->{"pubtype"});
	my $abstract = "";
	if ($_->{"static_data"}->{"fullrecord_metadata"}->{"abstracts"}) {
	    #	    print "abs:";
	    #	    say Dumper($_->{"static_data"}->{"fullrecord_metadata"}->{"abstracts"});
	    my $count = &getValue($_->{"static_data"}->{"fullrecord_metadata"}->{"abstracts"}->{"_attributes"}->{"count"});
	    if ($count > 1) {
		say "_MOREABS - WARNING: UnifiedBibDataAccess.pm cannot deal with mulitple abstracts. Amend code.";
	    };
	    my $countSeg = &getValue($_->{"static_data"}->{"fullrecord_metadata"}->{"abstracts"}->{"abstract"}->{"abstract_text"}->{"_attributes"}->{"count"});
	    my @abstracts = &makeArrayFromRefHint($_->{"static_data"}->{"fullrecord_metadata"}->{"abstracts"}->{"abstract"}->{"abstract_text"}->{"p"},'$_->{"static_data"}->{"fullrecord_metadata"}->{"abstracts"}->{"abstract"}->{"abstract_text"}->{"p"}');
	    say "WARNING: Abstracts count: $count; segment count in first abstract: $countSeg" if $count > 1;
	    foreach my $awbs (@abstracts) {
		$abstract .= &getValue($awbs->{"_text"}) . "\\n";
	    };
	};
	$x{"abstract"} = $abstract;
	#my @identifiers = &getArrayFromScalar($_->{"dynamic_data"}->{"cluster_related"}->{"identifiers"}->{"identifier"});
	if ($_->{"dynamic_data"}->{"cluster_related"} 
	    && $_->{"dynamic_data"}->{"cluster_related"}->{"identifiers"} 
	    && $_->{"dynamic_data"}->{"cluster_related"}->{"identifiers"}->{"identifier"}) {
	    my @identifiers = &makeArrayFromRefHint($_->{"dynamic_data"}->{"cluster_related"}->{"identifiers"}->{"identifier"},'$_->{"dynamic_data"}->{"cluster_related"}->{"identifiers"}->{"identifier"}');
	    foreach (@identifiers) {
		#say Dumper($_);
		if ($_->{"_attributes"}) {
		    $x{$_->{"_attributes"}->{"type"}} = &getValue($_->{"_attributes"}->{"value"});
		};
	    };
	};
	# doi, isn, eissn
	# {
	#   {"_attributes"}: {
	#       {"type"}: "doi",
	#       {"value"}: "10.1080/17400201.2011.589253"
	#   }   	
	push @oitems, \%x;
    };
    return @oitems;
};


sub processProQuestXML() {
    my $json = &xmlToJSON($_[0]);
    return &processProQuestJSON($json);
};

sub processProQuestJSON() {
    #TODO likely this doens't capture all aiuthors correctly. - done?!
    #	print "xxx========== FULL RESULTS ===============\n";
    #	&pretty($_[0]);
    #   if ($repo =~ /^pq/) {
    my %stat = %protoStat; 
    my @items = ();
    my @oitems = ();
    if ($verbose) {
	my $a = $_[0];
	$a =~ s/\},/\},\n/sg;
	print $a;
    };
    my $json = decode_json(encode_utf8($_[0]));
    if ($verbose) {
	# print Dumper(\$json);
    };    
    if ($json->{'zs:searchRetrieveResponse'}) {
	$stat{total} = $json->{'zs:searchRetrieveResponse'}->{'zs:numberOfRecords'}->{'_text'};
	if ($stat{total} > 1) {
	    @items = @{$json->{'zs:searchRetrieveResponse'}->{'zs:records'}->{'zs:record'}};
	} elsif ($stat{total} == 1) {
	    @items = ($json->{'zs:searchRetrieveResponse'}->{'zs:records'}->{'zs:record'});
	};
    } else {
	$stat{total} = 1;
	$json = decode_json("{\"items\": [".encode_utf8($_[0])."]}");
	@items = @{$json->{'items'}};
    };
    push @oitems,\%stat;
    # zs:extraResponseData
    foreach (@items) {
	my %x = %protoFields;
	local $Data::Dumper::Purity = 1;           
	local $Data::Dumper::Indent = 0;
	$x{itemdata} = decode_utf8(encode_json($_));
	$x{itemdatatype} = "perldumper";
	my @datafield = @{$_->{'zs:recordData'}->{'record'}->{'datafield'}};
	#print Dumper(@datafield);
	my %meta = ();
	my %help = ( "100","author100",
		     "245","title",
		     "513","publicationType",
		     "520","abstract",
		     "653","keywords",
		     "700","author700",
		     "786","database",
		     "856","link",
		     "651","location",
		     "773","citation",
		     "260","PublisherDateCopyright",
		     "024","doi",
		     "035","identifier",	    
		     "045","daterange");	    
	foreach my $subf (@datafield) {
	    #print Dumper($subf);
	    #my @sf = @{$subf->{'subfield'}};
	    # ->{'_text'}
	    my $key = $subf->{'_attributes'}->{'tag'};
	    my @sf = (); 
	    my $type = ref($subf->{'subfield'});
	    if ($type eq "HASH") {
		@sf = ($subf->{'subfield'});
	    } else {
		@sf = @{$subf->{'subfield'}};
	    };
	    foreach my $sf (@sf) {
		if ($sf->{'_text'}) {
		    $sf->{'_text'} =~ s/\n/\\n/sg;
		    $meta{$key} .= $sf->{'_text'}."; ";
		} else {
		    say "Non-critical exception: ";
		    say Dumper($sf);
		    say "In item: ";
		    say $x{itemdata};
		};
	    };
	};
	foreach my $key (keys %meta) {      	    
	    if (defined $help{$key}) {
		$x{$help{$key}} .=  "$meta{$key}";
	    } else {
	    };
	    $meta{$key}=~ s/\;$//;
	};
	foreach my $key (keys %x) {      	    
	    $x{$key} =~ s/\;+\s*$//;
	};
	$x{"authors"} = "";
	if ($x{"author100"}) {
	    $x{"authors"} = $x{"author100"} . "; ";
	};
	if ($x{"author700"}) {
	    $x{"authors"} .= $x{"author700"};
	};
	$x{"authors"} =~ s/\;+\s*$//;
	if ($x{identifier}) {
	    $x{identifier} = "PQ-$x{identifier}";
	};
	if ($meta{"045"}) {
	    if ($meta{"045"} =~ m/^d(\d\d\d\d)/) {
		$x{year} = $1;
	    };
	}
	$x{doi} =~ s/\;\s*doi\s*$//;
	$x{doi} =~ s/^\d+\;\s+//;
	$x{doi} = "" if $x{doi} =~ m/No DOI/i;
	push @oitems, \%x;
    }
    return @oitems;
};

sub utify() {
    my $str = $_[0];
    my $flag1 = utf8::is_utf8($str);
    my $flag2 = utf8::valid($str);
    if (!$flag1) {
	# $str = decode_utf8($str);
	$str = &fix_latin($str);
	utf8::upgrade($str);
    } else {
    };
    return $str;
};


sub lutify() {
    my $str = $_[0];
    my $flag1 = utf8::is_utf8($str);
    my $flag2 = utf8::valid($str);
    if (!$flag1) {
	$str = &fix_latin($str);
	utf8::upgrade($str);
    } else {
	if (utf8::downgrade($str,1)) {
	    $str = &fix_latin($str);
	    utf8::upgrade($str);
	} else {
	    # string is utf already, and cannot be downgraded. HAve to assume this is ok
	};
    };
    return $str;
};


#  Routines for utf8
sub utifyItem() {		
    my $item = $_[0];
    #  fix non-latin encodings:
    foreach my $string (keys %{$item}) {
	# the procedure below doesn't work for itemdata
	next if $string eq "itemdata";
	if (${$item}{$string}) {
	    ${$item}{$string} =~ s/\cM//sg;
	    if (!utf8::is_utf8(${$item}{$string})) {
		${$item}{$string} = &utify(${$item}{$string});
	    } else {
		#  some characters have been marked UTF8 erroneously...
		my $str = ${$item}{$string};
		if (utf8::downgrade($str,1)) {
		    $str = &fix_latin($str);
		    utf8::upgrade($str);
		    ${$item}{$string} = $str;
		} else {
		};
	    };
	};
    };
};

#  Routines for utf8 end

sub readRIS() {
    my $indexBy = $_[0];
    my $file = $_[1];
    say "Reading RIS file '$file', will index by $indexBy";
    my %ris ;
    open F,"$file" or die("File $file no found.");
    my $n = -1;
    my %record;
    my $item = "";
    my $key = "";
    my %out;
    while (<F>) {
	s/\cM//sg;
	if (m/^ER  ?\-/) {
	    foreach my $k (keys %record) {
		$record{$k} =~ s/\n$//s;
	    };
	    $n++;
	    if ($indexBy && $record{$indexBy}) {
		$key = $record{$indexBy};
	    } else {
		$key = "N__".$n;
	    };
	    if ($out{$key}) {
		say "WARNING: readRIS - non-unique key $key. Choose a different key!";
	    };
	    #say "output $key";
	    %{$out{$key}} = %record;
	    %record = ();
	    next;
	} elsif (s/^(\w\w)  ?\- //) {
	    $item = $1;
	}
	$record{$item} .= $_;
    };
    close F;
    # say 'Dumper='.Dumper(\%out);
    return \%out;
};


