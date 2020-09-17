

use My::UnifiedBibDataAccess qw(@exportedItems);
&ubaUsage();

# supply data type
print &ubaShow(&uba(\"ESSA\",\$string)); 

# detect type from filename
print &ubaShow(&ubaFile(\"?\",\$filename)); 

# or detect file type from a string, such as the filename: 
print &ubaShow(&uba(\$filename,\$f)); 

The possible types are shown below.

my @items = &ubaFile("GS",$somefile);
          = &uba("GS",$anystring);

print &ubaShow(@items); 


# read the json file
my $file = $ARGV[0];
open my $IN,"<",$file;
my $jsonstring = join "", <$IN>;
close $IN;

my %record;

my @data = &uba("?".$file, $jsonstring);

@data = [
 { ... }.
 { item1 },
 { item2 },
 ...
]


$record{data} = \@data;
%{$record{stat}} = %{shift @{$record{data}}};

%record = 
{
    "stat" : { ... } },
    "data" : [
	{ item1 },
	{ item2 },
	...
    ]
}
