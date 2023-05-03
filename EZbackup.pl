#!/usr/bin/perl

##########################################################################
#
# EZbackup.pl -- version 0.1
#
# Author:	Faisal Chaudhry  (faisal.chaudhry@qatar.tamu.edu)
# Date:		May 4, 2011
#
# This script is a wrapper for the opensource "duplicity" backup tool.
# The wrapper is indended to allow the user to create a backup policy
# by writing simple text files specifying things like what to back up,
# when, how long to keep old backups, and other such parameters.  This
# script is intended to be invoked once per day as a cron job.
# 
# Apart from ease of use, the main benefit of the script is that on 
# systems where it makes sense, it can create multiple backup threads
# each of which can target different directories for backup simultaneously.
#
# EZbackup may be extended to use other backup tools in place of 
# duplicity in the future.
#
##########################################################################

package main;

#--------- used packages section ---------------------

use strict;
#use warnings;				# used in testing and development only

use AppConfig ':argcount';
use DateTime;
use File::Basename;
use POSIX qw(strftime);
use threads;
use threads::shared;
use Thread::Queue;
use Time::Duration;
use Time::Piece;
use Time::Seconds;


#--------- global variables section ------------------

$main::debug 			= 1;		# default debug mode; 1 = on
$main::N 			= 12;		# number of threads in threaded solution
$main::configProfile 		= "main";	# name of default configuration profile
$main::lockDir			= "/tmp/EZbackupv2";	# default location for lock files
$main::cacheDir			= "/netapp/data/duplicity-tempdir";  # location of cache for archive signature files
$main::tToday 			= localtime;	# determine today's date and time
$main::bad_param 		= 0;		# "invalid parameter value" flag


# The %main::sections hash contains the section labels and descriptions for
# all possible file systems that may be configured for backup in this script's
# policy file. These section labels are declared in the "sectiondefs" file.
# Section names (i.e. [home]) used in the policy file must be declared in
# the sectiondefs file.

%main::sections = ();

my $retcode = verify_section_defs ($main::configProfile);
if ($retcode == 0) {
	print "Everything OK with section defs\n" if ($main::debug);
}
elsif ($retcode == 1) {
	print "Section defs file not found\n" if ($main::debug);
	exit (1);
}
elsif ($retcode == 2) {
	print "Section defs file could not be opened\n" if ($main::debug);
	exit (1);
} 
elsif ($retcode == 3) {
	print "The format of the section defs file is invalid\n" if ($main::debug);
	exit (1);
}

$retcode = read_section_defs (\%main::sections, $main::configProfile);

if ($main::debug) {
	print ("\nContents read from sectiondefs file...\n\n");
	foreach my $dummykey (keys %main::sections) {
		print "$dummykey = ", $main::sections{$dummykey}, "\n";
	}
}

# The "./config/<profilename>/*.excludes" files contain the files and/or
# subdirectories to be excluded from the filesystem backups specified in
# the policy file.  The prefix of any such exclude files must match one
# of the section labels defined in the sectiondefs file.  The function
# below verifies that the content of existing excludes files conforms 
# to expected syntax.

$retcode = verify_section_excludes ($main::configProfile);
if ($retcode == 0) {
	print "Everything OK with section exclude files\n" if ($main::debug);
}
elsif ($retcode == 1) {
	print "The profile directory could not be opened\n" if ($main::debug);
	exit (1);
}
elsif ($retcode == 2) {
	print "A section exclude file could not be opened\n" if ($main::debug);
	exit (1);
} 
elsif ($retcode == 3) {
	print "A section exclude file contains a malformed path\n" if ($main::debug);
	exit (1);
}

# Each possible section in the policy file may configure the following parameters.
# Additional parameters may be added to this "allowed" list by adding elements to
# the %main::parameters hash below.

%main::parameters = (		
	
	"source" => "Absolute path of the directory to be backed up in this section",
	"target" => "Absolute path of the location the source backup should be stored",
	"method" => "The string value: 'duplicity' (other backup methods may be added in the future)",
	"savequota" => "The string value 'yes' or 'no' (for quota-enabled filesystems)",
	"precmd" => "Any valid script to be run before this particular backup",
	"postcmd" => "Any valid script to be run after this particular backup",
	"full_interval" => "Single integer denoting weeks between full backups (between 0 and 52)",
	"diff_interval" => "Single integer denoting days between differentials (between 0 and 30)",
	"full_bak_day" => "Day of the week on which full backups are to take place (e.g. Sat, Sun, etc.)",
	"volsize" => "Size (in MB) of backup chunks created by backup utility (between 25 and 2000)",
	"exclude" => "Comma separated list of relative dir paths to exclude from this backup",
	"retention" => "Number of previous full backups to retain for this backup set (from 1 to 12)",
	"multiple_dirs" => "The string value 'yes' or 'no' (each subdir with separate backup?)",

);

# If any of the following parameters are not explicitly set in the policy file, certain
# default values will be assigned to these parameters.

%main::defaults = (

	"full_bak_day" 		=> "Saturday",
	"multiple_dirs" 	=> "no",
	"volsize" 		=> 500,
	"method"		=> "duplicity",
	"savequota"		=> "no",

);

# Each possible parameter to be read from the policy file has to be "defined" 
# before the parsing of the policy file.  This loop builds the definition string
# passed to the define() method for all possible paramters in the policy file.
# For example, the parameter "target" appearing within the [home] section
# of the policy file needs to be pre-defined with the satement:
#
# $cfg->define("home_target=s");

# $cfg is an object (from the AppConfig package) that will be used to parse the
# policy file and assign values to parameters.  The instantiation of this object
# below defines some defaults for the types of values to be assigned to parameters.
# For instance, every parameter defined with the $cfg->define statement is
# initialized with the 'undef' value.  If this parameter is then found in the 
# policy file, it gets assinged the value listed for it in that policy file.
# Also, every parameter is of type string, and every value, before being
# assigned to a parameter, is checked for validity by the subroutine check_val().

my $cfg = AppConfig->new({ 	GLOBAL => 	{	DEFAULT  => undef,
            						ARGCOUNT => ARGCOUNT_ONE,
            						ARGS => '=s',
							VALIDATE => \&check_val,
        					}
			});

printf ("\nChecking validity of parameter values...\n") if ($main::debug);
printf ("----------------------------------------\n\n") if ($main::debug);

# The following code defines the list of all possible parameters that could
# be found in the policy file.

foreach my $prefix (keys %main::sections) {
	foreach my $pname (keys %main::parameters){
		my $suffix = "=s";
		my $varspec = $prefix . "_" . $pname . $suffix;
		$cfg->define($varspec);
	}
}

# The following code parses the relevant policy file, reads in values, and
# assigns them to the parameters "defined" above.

my $policyfile = "./config/$main::configProfile/policy";
if (-e $policyfile) {
	$cfg->file($policyfile);
}
else {
	printf ("The backup policy file \"$policyfile\" not found.  Exiting script...");
	exit (1);
}

# The policy file is an .ini style file (2 level hierarchy -- section names, 
# section parameters). The appconfig package reads this hierarchy but "flattens"
# it out by creating a single level list of parameters and their values. For
# instance, all parameters in the [home] section get put into variables with names
# of the form home_param1, home_param2, etc.  Likewise, paramters in the [projects]
# section would go into projects_param1, projects_param2, etc.

# The code below in effect "un-flattens" that parameter list by creating a separate
# hash for every section in the config file, within which all paramters from that
# section, along with their values, are stored.  So for instance, the list of
# variables home_param1, home_param2... home_paramN results in a new hash by the
# name "home", and its hash elements include parameter-value pairs for param1, 
# param2, through paramN.  Furthermore, a reference to that newly created hash
# gets stored in a 2-element array, where the first element is the string
# "home" and the 2nd element is the hash reference.  A reference to that 
# 2-element array is in turn pushed on to an array called @config.  From
# here on, this script will be able to reference any parameter-value pair
# from any section of the policy file using the @config array. 

my @policy = ();

for my $sname (keys %main::sections) {
	no strict 'refs';
	my $new_hash_name = $sname; 
	my $prefix = "^" . $sname . "_";
	%$new_hash_name = $cfg->varlist($prefix,1);
	my @tmp = ($sname, \%$new_hash_name);
	push @policy, \@tmp;
}

# The followig function could be uncommented for debugging purposes if required

#print_policy_contents (\@policy);

if (check_param_constraints (\@policy) || $main::bad_param) {
	print "\nFATAL ERROR:\n\nThe policy file is missing certain required parameters, ". 
	      "or contains invalid values.\n";
	print "Please run the script with the --debug flag for more information.  " .
	      "A description \nof policy file parameters follows:\n\n" ;
	&print_help ();
	exit (1);	
}

# &print_help();

my $can_use_threads = eval 'use threads; 1';
my @allfiles = ();
my @subdirs = ();
my @command = ();
my $start_time = 0;
my $i;


print "\n\nStarted performing backups  ---  ";
system ("date");
print "\n";

# if the instance of perl installed on the host supports threading, this script
# will run in the if block, doing multi-threaded backups... otherwise, just serial

if ($can_use_threads) {
	
	print "Performing a multi-threaded backup with $main::N threads...\n\n";
	
	my $Q = new Thread::Queue;
	my @threads = ();

	for (my $count = 1; $count <= $main::N; $count++) {
		my $t = threads->new( \&threaded_backup, $Q );
		push (@threads, $t);
	}

	for ($i=0 ; $i <= $#policy ; $i++ ){

	  if (defined ($policy[$i][1]{'source'})) {		# only process sections actually found in policy file

	  	print "Processing section [$policy[$i][0]] of policy\n";

		if (lc($policy[$i][1]{'multiple_dirs'}) eq 'yes') {
			opendir (SOURCEDIR, $policy[$i][1]{'source'}) or die "cannot open directory $policy[$i][1]{'source'}: $!\n";
			@allfiles = grep {$_ ne '.' and $_ ne '..'} readdir SOURCEDIR;
			@subdirs = grep -d,  map {"$policy[$i][1]{'source'}/$_"} @allfiles;		# weed out non-directory files
			closedir (SOURCEDIR);
			for my $sourceDir (@subdirs) {

				# create a lock file before backing up this sub-dir; this lock file will be
				# removed eventually by the rm command generated in $command[3]

				if (! set_subsection_lock ($policy[$i][0], $sourceDir)) { next; }
				generate_duplicity_cmd($policy[$i][1], $policy[$i][0], $sourceDir, \@command);
				$Q->enqueue( join(" && ",@command) );
			}
		}
		else {
			# create a lock file before backing up this dir; this lock file will be
                        # removed eventually by the rm command generated in $command[3]

			if (! set_section_lock ($policy[$i][0])) { next; }
			generate_duplicity_cmd($policy[$i][1], $policy[$i][0], $policy[$i][1]{'source'}, \@command);
			$Q->enqueue( join(" && ",@command) );
		}

	  }			# end of if (defined... block

	  @command = ();	# clear the command array for use in the next iteration

	}			# end of for block

	# indicate to threads that there's no more work left, so they can finish up
	$Q->enqueue( ( undef ) x $main::N );

	# And wait for threads to clean up
	$_->join for @threads;

} else {
	
	print "Performing a serial (non-threaded) backup...\n\n";

	for ($i=0 ; $i <= $#policy ; $i++ ){

	  if (defined ($policy[$i][1]{'source'})) {		# only process sections actually found in policy file

	    	print "Processing section [$policy[$i][0]] of policy\n";

		if (lc($policy[$i][1]{'multiple_dirs'}) eq 'yes') {
			opendir (SOURCEDIR, $policy[$i][1]{'source'}) or die "cannot open directory $policy[$i][1]{'source'}: $!\n";
			@allfiles = grep {$_ ne '.' and $_ ne '..'} readdir SOURCEDIR;
			@subdirs = grep -d,  map {"$policy[$i][1]{'source'}/$_"} @allfiles;		# weed out non-directory files
			closedir (SOURCEDIR);
			for my $sourceDir (@subdirs) {
				
				if (! set_subsection_lock ($policy[$i][0], $sourceDir)) { next; }
				generate_duplicity_cmd($policy[$i][1], $policy[$i][0], $sourceDir, \@command);
				
				if (system ( join(" && ",@command) ) == 0) {
					if ($main::debug) {
						printf ("-----------------------------\n");
        					printf ("SUCCESS:   Runtime on $sourceDir = %s\n", duration(time() - $start_time));
						printf ("%s\n", join("\n",@command) );
						printf ("-----------------------------\n");
					}
        			}  else {
					if ($main::debug) {
						printf ("-----------------------------\n");
                                        	if ($command[2] =~ /false/) {
                                                	printf ("BACKUP NOT DONE\n");
                                                	system ($command[4]);         # command[4], which deletes lock, needs to be run "manually" here
                                        	}
                                        	else {
                                                	printf ("BACKUP FAILED:  exit status = %s \n", $?/256);
                                        	}
						printf ("%s\n", join("\n",@command) );
                                        	printf ("-----------------------------\n");
					}
        			}
			}
		}
		else {
			if (! set_section_lock ($policy[$i][0])) { next; }	
			generate_duplicity_cmd($policy[$i][1], $policy[$i][0], $policy[$i][1]{'source'}, \@command);
			
			if (system ( join(" && ",@command) ) == 0) {
				if ($main::debug) {
					printf ("-----------------------------\n");
        				printf ("SUCCESS:   Runtime on $policy[$i][1]{'source'} = %s\n", duration(time() - $start_time));
					printf ("%s\n", join("\n",@command) );
                			printf ("-----------------------------\n");
				}

        		}
			else {
        			if ($main::debug) {
					printf ("-----------------------------\n");
                			if ($command[3] =~ /false/) {
                        			printf ("BACKUP NOT DONE\n");
                        			system ($command[4]);         # command[4], which deletes the lock, needs to be run "manually" here
                			}
                			else {
                        			printf ("BACKUP FAILED:  exit status = %s \n", $?/256);
                			}
					printf ("%s\n", join("\n",@command) );
					printf ("-----------------------------\n");
				}
        		}
		}


	  }	# end of "if (defined..." block
	}	# end of for block
	
}


print "\nFinished performing backups  ---  ";
system ("date");

#----------------------------------------------------------------------------------
#------------------- subroutines are defined below --------------------------------
#----------------------------------------------------------------------------------

# print_help() prints out the format of the policy file if the program is
# invoked with the -h or --help options.

sub print_help () {
	
	my ($i, $section, $parameter);
	
	print "\nHELP:\n\n";
	print "To configure backup policy, edit the file:  ./config/$main::configProfile/policy\n\n";
	print "Valid section names for the policy file include (with descriptions):\n\n";
	foreach $section (sort keys %main::sections){
		my $header = "[" . $section . "]";
		printf ("%15s   (%s)\n", $header, $main::sections{$section});
	}
	print "\nTo define additional sections, edit the file ./config/$main::configProfile/sectiondefs\n\n";
	
	print "\nPossible parameters and values for any given section include:\n\n";
	foreach $parameter (sort keys %main::parameters){
		printf ("%15s = %s\n", $parameter, $main::parameters{$parameter});
	} 
	
	print "\nThese parameters (for any given section) must be explicitly defined in the policy file:\n\n";
	printf ("%15s\n", 'source');
	printf ("%15s\n", 'target');
	printf ("%15s\n", 'full_interval');
	printf ("%15s\n", 'diff_interval');
	printf ("%15s\n", 'retention');
	
	print "\nIf left undefined, the parameters below get these default values:\n\n";
	foreach $parameter (sort keys %main::defaults){
		printf ("%15s = %s\n", $parameter, $main::defaults{$parameter});
	} 	
	print "\n";
}

# print_policy_contents()  requires a single reference to an array that provides
# access to all information read in from the policy file. This is to be
# an array of references, each of which further points to its own two-element
# array where element 0 contains the name (string) of the respective policy
# file section, and element 1 contains a hash with name-value pairs for all
# parameters within that section.  Any valid parameter not defined under a
# policy file section assumes the 'undefined' value. 

sub print_policy_contents {	
	
	my ($policy, $i, $key, $numvalues);
	
	$policy = shift;		# $policy should now contain a reference to the top-level array
	
	# The loop below simply traverses the structures accessible from the 
	# @policy array and prints out the parameter-value pairs within
	# their respective section headings.

	for ($i=0 ; $i <= $#policy ; $i++ ){
		print "[$policy[$i][0]]\n";
		foreach $key (keys %{$policy[$i][1]}){
			printf "   %-20s =  %s\n", $key, $policy[$i][1]{$key} if (defined ($policy[$i][1]{$key}));
		}    	 
	}
}

# Given the name of the configuration profile (default is "main"), this routine reads
# the file ./config/<profilename>/sectiondefs to validate conformity to expected syntax

sub verify_section_defs {

	my ($profile);

	my ($filename, $name, $description, @fields, $n);
		
	$profile = shift;
	
	# construct the pathname to the sectiondefs file
	
	$filename = "./config/$profile/sectiondefs";
	
	# open the file; return failure code 1 if no such file,
	# or code 2 if file found but cannot be opened
	
	if (-e $filename) {
		open (FILE, $filename) || return (2);
	}
	else {
		return (1);
	}
	
	# read file contents; assume each line has the following format (w/out quotes):
	# "sectionname	# a short descriptive string for this section"	
	# return failure code (3) if sectioname or descriptive string is problematic
	
	while (<FILE>) {
		$n = @fields = split ("#", $_, 2);
		if ($n == 1) { return (3);}		# the input line contains no "#" separator
		($name, $description) = @fields;
		$name =~ tr/\t //d;			# remove all whitespace
		#if ($name !~ /^[a-z-]+$/) { return (3);}	# name contains non-alphanumeric character(s)
	}
	
	# close file
	
	close (FILE);
	
	# return success code (0)
	
	return (0);
	
}

# Given the name of the configuration profile (default is "main"), this routine reads
# the file ./config/<profilename>/sectiondefs to populate a hash.  This routine
# assumes that verify_section_defs() has already been called previously, so it does
# not perform any error checking.

sub read_section_defs {

	my ($hashref, $profile);

	my ($filename, $name, $description);
	
	$hashref = shift;	
	$profile = shift;
	
	# construct the pathname to the sectiondefs file
	
	$filename = "./config/$profile/sectiondefs";
	
	# open the file
	
	open (FILE, $filename);
	
	# read file contents into the hash
	
	while (<FILE>) {
		($name, $description) = split "#";
		$name =~ tr/\t //d;					# remove all whitespace
		$description = join (" ", split " ", $description); 	# canonicalize whitespace
		$$hashref{$name} = $description;
	}
	
	# close file
	
	close (FILE);
	
	# return success code (0)
	
	return (0);
	
}

# Given the name of the configuration profile (default is "main"), this routine reads
# the files ./config/<profilename>/*.excludes to validate conformity to expected syntax.
# It also ensures that exclude file names have prefixes matching valid section names.

sub verify_section_excludes {

	my ($profile);

	my ($dirname, $filename, $pattern, $prefix, $suffix, @allfiles, @excludefiles);
	
	$profile = shift;
	
	# construct the pathname to the dir containing section exclude file(s)
	
	$dirname = "./config/$profile";
	
	# generate list of exclude file names in $dirname
	
	if (! opendir (SOURCEDIR, $dirname)) {
		print "Directory '$dirname' cannot be opened\n" if ($main::debug);
		return (1);
	}
	@allfiles = grep {$_ ne '.' and $_ ne '..'} readdir SOURCEDIR;
	@excludefiles = grep { /.*\.exclude$/ } @allfiles;		# retain only exclude file names
	closedir (SOURCEDIR);
	
	foreach $filename (@excludefiles) {
		($prefix, $suffix) = split /\./, $filename; 
		if (! grep /^$prefix$/, keys %main::sections) {
			print "$dirname/$filename does not refer to any valid section name\n" if ($main::debug);
			return (4);
		}
	}
	
	print "\nExclude files found:\n\n" if ($main::debug);
	print join ("\n", @excludefiles), "\n\n" if ($main::debug);
	
	$pattern = '[a-zA-Z][a-zA-Z0-9_.]*';
	
	foreach $filename (map {"$dirname/$_"} @excludefiles){
		if (! open (FILE, $filename)) {
			print "'$filename' cannot be opened\n" if ($main::debug);
			return (2);
		}
		while (<FILE>) {
			chomp;
			if ($_ =~ /^((?:$pattern){1})+$\/?/x) {
				next;
			}
			else {
				print "$filename: the path '$_' seems to be invalid\n" if ($main::debug);
				return (3);
			}
		}
		close (FILE);
	}
	
	# if we reach this point, everything is dandy!
	
	return (0);
	
}

# Given the name of the configuration profile (default is "main"), this routine reads
# the files ./config/<profilename>/*.excludes to populate an array.  It does no error
# checking, assuming that verify_section_excludes() has already been called previously.

sub read_section_exclude {

	my ($arrayref, $profile, $section);

	my ($dirname, $filename, $pattern, $prefix, $suffix, @allfiles, @excludefiles);
	
	$arrayref = shift;
	$profile = shift;
	$section = shift;
	
	# construct the pathname to the specific section exclude file
	
	$filename = "./config/$profile/$section.exclude";
	
	# open the file
	
	if (! open (FILE, $filename)) {
		print "'$filename' cannot be opened\n" if ($main::debug);
		return (1);
	}

	# read each line of the file into a separate element of array referenced by $arrayref
	
	@{$arrayref} = <FILE>;
	chomp (@{$arrayref});
		
	# close the file
	
	close (FILE);
	
	return (0);
	
}

sub set_section_lock {

        my ($sectionName, $lockFile, $pid);

        $sectionName = shift;

        $lockFile = $main::lockDir . "/duplicity." . $sectionName . ".lock";

        if (-e $lockFile) {     # if a lock file exists, read the pid value stored in it
                open EXISTING, $lockFile;
                $pid = <EXISTING>;
                chomp $pid;
                close EXISTING;

                # $pid should be the pid of the parent EZbackup script; check if this process is actually running
                # if it is, print appropriate error messages and return a 0 value from subroutine (meaning failure)

                if (active_pid ($pid)) {
                        printf ("\n");
			printf ("-----------------------------------------------------------------------------\n");
                        printf ("ERROR: Unable to create lock for the section \"$sectionName\". \n");
                        printf ("PID %d appears to be currently backing up this section.  Skipping section.\n", $pid);
                        printf ("\n");
                        printf ("Output of ps command:\n\n");
                        system ("ps -jlf $pid");
                        #printf ("\nOutput of pstree rooted at pid $pid:\n\n");
                        #system ("pstree -cap $pid");
                        return 0;
                }
                else {          # if pid not active, delete lock file and re-create it
                        unlink $lockFile;

                        open FILE, ">$lockFile";
                        print FILE getppid();
                        close FILE;
			print "Pid in existing lock not valid.  Creating new lock: $lockFile\n" if ($main::debug);
                        return 1;
                }
        }
        else {                  # if no lock file found, create one and write pid of parent in it

                open FILE, ">$lockFile";
                print FILE getppid();
                close FILE;
		print "Created: $lockFile\n" if ($main::debug);
                return 1;
        }


}


sub set_subsection_lock {

        my ($sectionName, $subDirName, $lockFile, $pid);

        $sectionName = shift;
	$subDirName = shift;

        $lockFile = $main::lockDir . "/duplicity." . $sectionName . "." . basename($subDirName) . ".lock";

        if (-e $lockFile) {     # if a lock file exists, read the pid value stored in it
                open EXISTING, $lockFile;
                $pid = <EXISTING>;
                chomp $pid;
                close EXISTING;

                # $pid should be the pid of the parent EZbackup script; check if this process is actually running
                # if it is, print appropriate error messages and return a 0 value from subroutine (meaning failure)

                if (active_pid ($pid)) {
                        printf ("\n");
			printf ("--------------------------------------------------------------------------------------\n");
                        printf ("ERROR: Unable to create lock for sub-section \"%s\" in section \"$sectionName\".\n", basename($subDirName));
                        printf ("PID %d appears to be currently backing up this sub-section.  Skipping sub-section.\n", $pid);
                        printf ("\n");
                        printf ("Output of ps command:\n\n");
                        system ("ps -jlf $pid");
                        #printf ("\nOutput of pstree rooted at pid $pid:\n\n");
                        #system ("pstree -cap $pid");
                        return 0;
                }
                else {          # if pid not active, delete lock file and re-create it
                        unlink $lockFile;

                        open FILE, ">$lockFile";
			print FILE getppid();
                        close FILE;
			print "Pid in existing lock not valid.  Creating new lock: $lockFile\n" if ($main::debug);
                        return 1;
                }
        }
        else {                  # if no lock file found, create one and write pid of parent in it

                open FILE, ">$lockFile";
		print FILE getppid();
                close FILE;
#		print "Created: $lockFile\n" if ($main::debug);
                return 1;
        }


}


sub active_pid {

        my ($pid);

        $pid = shift;
        chomp $pid;

        if (-e "/proc/$pid") {
                return 1;
        }
        else {
                return 0;
        }

}



# check_param_constraints()  requires a single reference to an array that provides
# access to all information read in from the policy file. This is to be
# an array of references, each of which further points to its own two-element
# array where element 0 contains the name (string) of the respective policy
# file section, and element 1 contains a hash with name-value pairs for all
# parameters within that section.  Any valid parameter not defined under a
# policy file section assumes the 'undefined' value.  This routine enforces
# additional checks on the values of parameters.  For instance, certain
# parameters must not be undefined.

sub check_param_constraints {	
	
	my ($policy, $i, $key, $numvalues, $section_defined, $bad_policy_file, $bad_section);
	
	$policy = shift;	# $policy should now contain a reference to the top-level array

	$bad_policy_file = 0;	# start with assumption that policy file is good
	$bad_section = 0;	# temp flag to detect problems within a section
    
	# The loop below simply traverses the structures accessible from the 
	# @policy array and examines the parameter-value pairs within
	# their respective section headings.

	print "\n\nChecking for required parameters, setting default values...\n" .
	      "-----------------------------------------------------------\n"  if ($main::debug);
	      
	for ($i=0 ; $i <= $#policy ; $i++ ){
		print "\nChecking [$policy[$i][0]]\n" if ($main::debug);

        # Determine if the current section actually apeared in the policy file.
        # Assume that if all parameter values in the section are undefined, the 
        # section was not present in the policy file.  In such a case, do nothing
        # and move on to examine the parameters of the next section.
        
        $section_defined = 0;			# initially, assume section is undefined (0 = undefined)
		foreach $key (keys %{$policy[$i][1]}){
			if (defined ($policy[$i][1]{$key})) { 
				$section_defined = 1; 	# if even single parameter defined, section is defined
				# printf ("   %-20s =  %s\n", $key, $policy[$i][1]{$key}) if ($main::debug);
			}
		}
		if ($main::debug) { print "\n"; }
		
		if ($section_defined == 0) {	# if section undefined, do nothing, move to next section
			print "   This section is NOT defined ... moving on ...\n\n" if ($main::debug);
			next;
		}
		else {		
			
			# ...section is defined, now make sure the required fields are not empty
			
			$bad_section = 0;
			
			if (! defined ($policy[$i][1]{"source"})) {
				print "   ERROR: ´source´ is undefined\n" if ($main::debug);
				$bad_policy_file = 1;	# raise flag indicating bad policy file
				$bad_section = 1;
			}
			if (! defined ($policy[$i][1]{"target"})) {
				print "   ERROR: ´target´ is undefined\n" if ($main::debug);
				$bad_policy_file = 1;
				$bad_section = 1;
			}
			if (! defined ($policy[$i][1]{"full_interval"})) {
				print "   ERROR: ´full_interval´ is undefined\n" if ($main::debug);
				$bad_policy_file = 1;
				$bad_section = 1;
			}
			if (! defined ($policy[$i][1]{"diff_interval"})) {
				print "   ERROR: ´diff_interval´ is undefined\n" if ($main::debug);
				$bad_policy_file = 1;
				$bad_section = 1;
			}
			if (! defined ($policy[$i][1]{"retention"})) {
				print "   ERROR: ´retention´ is undefined\n" if ($main::debug);
				$bad_policy_file = 1;
				$bad_section = 1;
			}
			
			# other fields, if empty, should be set to default values
			
			if (! defined ($policy[$i][1]{"multiple_dirs"})) {
				$policy[$i][1]{"multiple_dirs"} = $main::defaults{'multiple_dirs'};
				if ($main::debug) {
					printf ("   ´multiple_dirs´ set to default value of %s\n", $main::defaults{'multiple_dirs'});
				}
			}
			if (! defined ($policy[$i][1]{"full_bak_day"})) {
				$policy[$i][1]{"full_bak_day"} = $main::defaults{'full_bak_day'};
				if ($main::debug) {
					printf ("   ´full_bak_day´ set to default value of %s\n", $main::defaults{'full_bak_day'});
				}
			}
			if (! defined ($policy[$i][1]{"volsize"})) {
				$policy[$i][1]{"volsize"} = $main::defaults{'volsize'};
				if ($main::debug) {
					printf ("   ´volsize´ set to default value of %d\n", $main::defaults{'volsize'});
				}
			}
			
			if ($bad_section == 0) { print "   Everything looks OK in this section\n" if ($main::debug); }
		}
		 	 
	}	# end of for loop
	
	return $bad_policy_file;
}

# This is a subroutine that checks the values of all parameters to be defined 
# with the $cfg->define() method in the main program.

sub check_val () {
	my $var = shift;
	my $val = shift;
	
	my $dir = '[a-zA-Z][a-zA-Z0-9_.-]*';
	
	printf ("%30s  =  %-50s ...  ", $var, $val) if $main::debug;
	
	if ($var =~ /\w+_source/){
		if ($val =~ /^\/((?:$dir){1}\/?)+$/x) {
			print "PASS\n" if $main::debug;
			return 1;
		}
		else {
			print "FAIL\n\t\t\t\t" if $main::debug;
			$main::bad_param = 1;
			return 0;
		}
	}
	elsif ($var =~ /\w+_target/){
		if ($val =~ /^\/((?:$dir){1}\/?)+$/x) {
			print "PASS\n" if $main::debug;
			return 1;
		}
		else {
			print "FAIL\n\t\t\t\t" if $main::debug;
			$main::bad_param = 1;
			return 0;
		}
	}
	elsif ($var =~ /\w+_method/){
		if ($val =~ /^(?:duplicity)$/i) {
			print "PASS\n" if $main::debug;
			return 1;
		}
		else {
			print "FAIL\n\t\t\t\t" if $main::debug;
			$main::bad_param = 1;
			return 0;
		}	
	}
	elsif ($var =~ /\w+_savequota/){
		if ($val =~ /^(?:yes|no)$/i) {
			print "PASS\n" if $main::debug;
			return 1;
		}
		else {
			print "FAIL\n\t\t\t\t" if $main::debug;
			$main::bad_param = 1;
			return 0;
		}	
	}
	elsif ($var =~ /\w+_precmd/){
		if ($val =~ /\w+/) {
			print "PASS\n" if $main::debug;
			return 1;
		}
		else {
			print "FAIL\n\t\t\t\t" if $main::debug;
			$main::bad_param = 1;
			return 0;
		}	
	}
	elsif ($var =~ /\w+_postcmd/){
		if ($val =~ /\w+/) {
			print "PASS\n" if $main::debug;
			return 1;
		}
		else {
			print "FAIL\n\t\t\t\t" if $main::debug;
			$main::bad_param = 1;
			return 0;
		}	
	}
	elsif ($var =~ /\w+_full_interval/){
		if (($val =~ /^(\d+)$/) && ($val >= 0) && ($val <= 52)) {
			print "PASS\n" if $main::debug;
			return 1;
		}
		else {
			print "FAIL\n\t\t\t\t" if $main::debug;
			$main::bad_param = 1;
			return 0;
		}	
	}
	elsif ($var =~ /\w+_diff_interval/){
		if (($val =~ /^\d+$/) && ($val >= 0) && ($val <= 30)) {
			print "PASS\n" if $main::debug;
			return 1;
		}
		else {
			print "FAIL\n\t\t\t\t" if $main::debug;
			$main::bad_param = 1;
			return 0;
		}	
	}
	elsif ($var =~ /\w+_full_bak_day/){
		if (($val =~ /^(?:sat|sun|mon|tue|wed|thu|fri)$/i) || 
		    ($val =~ /^(?:saturday|sunday|monday|tuesday|wednesday|thursday|friday)$/i)) {
			print "PASS\n" if $main::debug;
			return 1;
		}
		else {
			print "FAIL\n\t\t\t\t" if $main::debug;
			$main::bad_param = 1;
			return 0;
		}	
	}
	elsif ($var =~ /\w+_volsize/){
		if (($val =~ /^\d+$/) && ($val >= 25) && ($val <= 2000)) {
			print "PASS\n" if $main::debug;
			return 1;
		}
		else {
			print "FAIL\n\t\t\t\t" if $main::debug;
			$main::bad_param = 1;
			return 0;
		}	
	}
	elsif ($var =~ /\w+_exclude/){
	    if ($val =~ /^((?:$dir){1}\/?)+(?:\s*,\s*((?:$dir){1}\/?)+)*$/x) {
			print "PASS\n" if $main::debug;
			return 1;
		}
		else {
			print "FAIL\n\t\t\t\t" if $main::debug;
			$main::bad_param = 1;
			return 0;
		}	
	}
	elsif ($var =~ /\w+_retention/){
		if (($val =~ /^\d+$/) && ($val <= 12) && ($val >= 1)) {
			print "PASS\n" if $main::debug;
			return 1;
		}
		else {
			print "FAIL\n\t\t\t\t" if $main::debug;
			$main::bad_param = 1;
			return 0;
		}	
	}
	elsif ($var =~ /\w+_multiple_dirs/){
		if ($val =~ /^(?:yes|no)$/i) {
			print "PASS\n" if $main::debug;
			return 1;
		}
		else {
			print "FAIL\n\t\t\t\t" if $main::debug;
			$main::bad_param = 1;
			return 0;
		}	
	}
	else { print "Unkown variable\n"; return 0; }
}

sub threaded_backup {

    my $Q = shift;
    my $mytid = threads->tid();
    my $x;
    my ($cmd0, $cmd1, $cmd2, $cmd3, $cmd4);

    while( my $nextcmd = $Q->dequeue ) {
    	my $start_time = time();
        if (system ($nextcmd) == 0) {
		if ($main::debug) {
			printf ("%s\nThread %d: SUCCESS:  Runtime = %s\n%s\n%s\n",
				"-----------------------------",
				$mytid, duration(time() - $start_time),
				join ("\n", split ("&& ", $nextcmd)),
				"-----------------------------");
		}
        }  else {
		($cmd0, $cmd1, $cmd2, $cmd3, $cmd4) = split ("&& ", $nextcmd);
		if ($cmd3 =~ /false/) {
			printf ("%s\nThread %d: BACKUP NOT DONE\n%s\n%s\n",
			"-----------------------------",
			$mytid, join ("\n", split ("&& ", $nextcmd)),
			"-----------------------------") if ($main::debug);
			system ($cmd4);		# cmd4, which deletes the lock file, needs to be run "manually" here
		}
		else {
			printf ("%s\nThread %d: BACKUP FAILED: exit status = %s\n%s\n%s\n",
                        "-----------------------------",
                        $mytid, $?/256, join ("\n", split ("&& ", $nextcmd)),
                        "-----------------------------") if ($main::debug);
		}
        }  
        
    }
}


sub generate_duplicity_cmd {
	
	my ($hashref, $sectionName, $sourceDir, $cmdArrayRef);	# these are the input parameters to this subroutine

	my ($source, $target, $method, $savequota);		# these vars hold values read from $hashref
	my ($precmd, $postcmd, $full_interval, $diff_interval);
	my ($full_bak_day, $volsize, $exclude, $retention, $multiple_dirs);

	my ($tStampFull, $tStampDiff, $tFullDue, $tDiffDue);	# these vars are used within this subroutine
	my ($tFull, $tDiff, $fullduedate, $cmdString, $baktype);
	my ($diffduedate, $today, $sourceURL, $targetURL);
	my ($excludeString, @excludeList, $val, @moreExcludes);
	my ($today_name_lc, $full_bak_day_name_lc);
	
	$hashref = shift;			# 1st param is a ref to a hash containting characteristics of this backup
	$sectionName = shift;			# 2nd param is name of the section referenced by $hashref
	$sourceDir = shift;			# 3rd param is the specific directory to be backed up
	$cmdArrayRef = shift;			# 4th param is reference to array of backup commands
	
	
	# note that we will choose to ignore $source in this subroutine and use $sourceDir instead;
	# e.g. while $source maybe "/rdrive", $sourceDir may be "/rdrive/fachaud74" or "/rdrive/otbouha20" etc.
	
	$source = $$hashref{'source'};
	$target = $$hashref{'target'};
	$method = $$hashref{'method'};
	$savequota = $$hashref{'savequota'};
	$precmd = $$hashref{'precmd'};
	$postcmd = $$hashref{'postcmd'};
	$full_interval = $$hashref{'full_interval'};
	$diff_interval = $$hashref{'diff_interval'};
	$full_bak_day = $$hashref{'full_bak_day'};
	$volsize = $$hashref{'volsize'};
	$exclude = $$hashref{'exclude'};
	$retention = $$hashref{'retention'};
	$multiple_dirs = $$hashref{'multiple_dirs'};
	

	# So, if we're backing up /rdrive to the target /backups/suqoor/rdrive, and 
	# we'd like to split up that backup per user directory, then the target URL
	# for every user backup needs to have the relevant user dir name appended.
	# The target URL needs to become something like: /backups/suqoor/rdrive/fachaud74.	

	$targetURL = "file://" . $target;
	if (lc($multiple_dirs) eq 'yes') { $targetURL .= "/" . basename($sourceDir)};

	# determine the date/time of the most recent full and diff backups of $dir

	#debug
	#print "******************************************\n";
	#print "calling prev_backup_tstamps ($targetURL)\n";
	#print "******************************************\n";

	($tStampFull, $tStampDiff) = prev_backup_tstamps ($targetURL);

	#debug
	#print "tStampFull = ", $tStampFull, "\n";
	#print "tStampDiff = ", $tStampDiff, "\n";

	if (! defined ($tStampFull)){	# if no previous full was found, pretend there was one on Jan 1, 1950
		$tFull = Time::Piece->strptime('Sun Jan 1 00:00:01 1950', "%a %b %d %T %Y");
	}
	else {
		$tFull = Time::Piece->strptime($tStampFull, "%a %b %d %T %Y");
	}
	
	if (! defined ($tStampDiff)){	# if no previous diff was found, pretend there was one on Jan 1, 1950
		$tDiff = Time::Piece->strptime('Sun Jan 1 00:00:01 1950', "%a %b %d %T %Y");
	}
	else {
		$tDiff = Time::Piece->strptime($tStampDiff, "%a %b %d %T %Y");
	}
	
	$tFullDue = $tFull + ($full_interval * 7 * ONE_DAY);	# calculate due date for next full as per policy
	$tDiffDue = $tDiff + ($diff_interval * ONE_DAY);	# calculate due date for next diff as per policy

	#debug
	#print "tFullDue->date = ", 	$tFullDue->date, "\n";
	#print "tDiffDue->date = ", 	$tDiffDue->date, "\n";

	# convert $tFullDue->date from the form "2011-01-26" to "20110126" so numerical comparisons can be done
	
	$fullduedate = $tFullDue->date;
	$fullduedate =~ tr/-//d;
	
	# convert $tDiffDue->date  as above
	
	$diffduedate = $tDiffDue->date;
	$diffduedate =~ tr/-//d;
	
	# convert $tToday->date  as above
	
	$today = $main::tToday->date;
	$today =~ tr/-//d;

	#debug
	#print "today = ", $main::tToday->date, "\n";
	

	#debug 
	#print "today num       = $today\n";
	#print "fullduedate num = $fullduedate\n";
	#print "diffduedate num = $diffduedate\n";	
	#print "lc(\$main::tToday->fullday) = ", lc($main::tToday->fullday), "\n";
	#print "lc(\$full_bak_day) = ", lc($full_bak_day), "\n";

	@excludeList = split (",", $exclude) if (defined ($exclude));
	
	# check to see if an exclude file exists for this section; if so, use it
	
	my $filename = "./config/$main::configProfile/$sectionName" . ".exclude";
	if (-e $filename) {
		if (read_section_exclude (\@moreExcludes, $main::configProfile, $sectionName) == 0) {
			@excludeList = (@excludeList, @moreExcludes)
		}
		else {
			printf ("ERROR: Could not process section excludes from $filename\n") if ($main::debug);
		}
	}
	else {	# this else block need to be retained; exclude files should be optional
		printf ("ERROR: Could not find $filename\n") if ($main::debug);
		return (1);
	}
	
	# using the values in @excludeList, create a string with multiple --exclude options
	
	$excludeString = "";
	foreach $val (@excludeList) {
		$val =~ tr/ //d;
		$val = "$source/$val";
		if ($val =~ /^$sourceDir/) {
			$excludeString .= "--exclude $val ";
		}	
	}
	
	# debug statement:
    	# print "today = $today    /    fullduedate = $fullduedate    /    diffduedate = $diffduedate \n";

	$baktype = "";								# initialize to empty string    

	if ($today >= $fullduedate) {						# if a full is due now...

		$today_name_lc = lc($main::tToday->fullday);
		$full_bak_day_name_lc = lc($full_bak_day);

		if ($today_name_lc =~ /^$full_bak_day_name_lc/) {		# ...and if today is a full backup day...
										# generate a full backup command string
			$baktype = "full";
		}
		elsif ($full_interval == 0) {					# if the full_interval param in the policy
										# file is 0, this script will run full
			$baktype = "full";					# backups EVERY time it is invoked, even
										# if invoked multiple times in a single day
		}
		else {								# full is due, but full backup day not here yet...
			if (($today >= $diffduedate) && (defined ($tStampFull))) {					
				# a full is due now, but since its not a full backup day today, a diff will be generated
				$baktype = "incremental";
			}
		}
	}
	else {									# else if a full is not due yet...

		if ($today >= $diffduedate) {					# ...but a diff is due...
			# generate a diff backup command string
			$baktype = "incremental";
		}
	}


	if (length($baktype) == 0) {	

		# If $baktype remains empty, it means no backup command is to be run today. This can be
		# accomplished with the unix "false" command that does nothing and returns exit status 1

		$cmdString = "false";
	}
	else {

		# if $baktype is either "full" or "incremental", generate the appropriate command string...
		# Faisal, 5/30/18: temporarily added --allow-source-mismatch to command string below
	
		$cmdString = "duplicity $baktype --encrypt-key 86F6BC1D --sign-key EC639BF5 --archive-dir $main::cacheDir --no-print-statistics --volsize $volsize " .
			     "--allow-source-mismatch $excludeString $sourceDir $targetURL";
	}

	# regardless of whether a backup is run today or not, implement retention policy with...

	# Retention of n means that INCLUDING the full backup we're just about to make, we should
	# end up with n full backups at $targetURL at any given time.  This in turn means that from
	# the existing (old) fulls we already have lying around, we need to keep only n-1 of them.

	if ($retention >= 2) {
	   if  ($baktype eq "full") {

		$retention -= 1;
		$$cmdArrayRef[0] = "duplicity remove-all-but-n-full $retention --archive-dir $main::cacheDir --force $targetURL > /dev/null";
		$$cmdArrayRef[1] = "duplicity remove-all-inc-of-but-n-full $retention --archive-dir $main::cacheDir --force $targetURL > /dev/null";
		$$cmdArrayRef[2] = "duplicity cleanup --encrypt-key 86F6BC1D --sign-key EC639BF5 --extra-clean --archive-dir $main::cacheDir --force $targetURL > /dev/null";
	   }
	   else {

		$$cmdArrayRef[0] = "true";
		$$cmdArrayRef[1] = "true";
		$$cmdArrayRef[2] = "true";
	   }
	}

	# If the retention policy calls for only 1 full backup to be retained at any given time, duplicity
	# will not be able to remove the only existing previous full with its sub-commands, and we will
	# simply have to delete the backup reposity at $targetURL "manually".  Note that we could order 
	# these command so that the current backup command runs first, followed by the remove and cleanup
	# commands seen above.  However, this will mean that if $retention is 1, we will still need room
	# at $targetURL sufficient for two full backups, because then the current backup would complete
	# before the older one would be deleted.  We have opted instead to delete the older one first,
	# even when it it the only remaining backup, before starting the new one.

	else {		# $retention must be 1 to enter this else block; <= 0 no valid values

	   if ($baktype eq "full") {

		$$cmdArrayRef[0] = "true";
		$$cmdArrayRef[1] = "true";
		$$cmdArrayRef[2] = "rm -rf " . substr ($targetURL, 7);
	   }
	   else {

		$$cmdArrayRef[0] = "true";
                $$cmdArrayRef[1] = "true";
                $$cmdArrayRef[2] = "true";
	   }
	}

	# the actual duplicity backup command...

	$$cmdArrayRef[3] = $cmdString;

	# final command in sequence will delete the lock file for this specific backup

	if (lc($multiple_dirs) eq 'yes') {
		$$cmdArrayRef[4] = "rm " . $main::lockDir . "/duplicity." . $sectionName . "." . basename ($sourceDir) . ".lock";
	}
	else {
		$$cmdArrayRef[4] = "rm " . $main::lockDir . "/duplicity." . $sectionName . ".lock";
	}

	return;
}

sub prev_backup_tstamps {
	
	my ($url, $dup_cmd, $fullTstamp, $diffTstamp, $done);
	
	$url = shift;
	
	$dup_cmd = "duplicity collection-status --archive-dir $main::cacheDir $url";
	$fullTstamp = undef;
	$diffTstamp = undef;
	
	$done = 0;
	open (CMD, "$dup_cmd |");
	while (<CMD>) {
		if ($_ =~ /^No backup chains with active signatures found$/) {
			last;
		}
		elsif ($_ =~ /^Found primary backup chain with matching signature chain:$/) {
			while (<CMD>) {
				if ($_ =~ /^Chain start time:/) {
					$fullTstamp = substr ($_, 18);
					chomp $fullTstamp;
					$_ = <CMD>;
					$diffTstamp = substr ($_, 16);
					chomp $diffTstamp;
					$done = 1;	# raise flag indicating we're done parsing ouput from <CMD>
					last;
				}
			}
			if ($done) {	# if chain start and end times have been obtained, exit search loop
				last;
			}
		}
	}
	
	return ($fullTstamp, $diffTstamp);
}

