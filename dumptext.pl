#!/usr/bin/perl

use strict;
use warnings;
no  warnings 'utf8';

use threads;
use threads::shared;
use Thread::Queue;

use Encode;
use HTML::Entities;

# First parameter is XML file, second parameter are categories
my $xml_name = shift(@ARGV) or die "Expected a MediaWiki dump file as parameter";

# Transclusions of interest:
#   {{good article}}, {{featured article}}
# Categories of interest:
#   {music, albums, songs}, {television, films}, {books, novels}, video games, {websites, internet meme}
#   sports, {ancient, history, chronology}
my @categories = map { qr/\Q$_\E/i; } @ARGV;
if ( scalar(@categories) == 0 ) { die "Expected at least one article category as a parameter"; }
printf(STDERR "Using categories: %s\n", join(', ', @ARGV));


# Number of worker threads to create and a dummy variable to act as a print lock
my $thread_count = 2;
my $stdout_lock :shared;
my $stderr_lock :shared;

# Work queue and maximum number of queue entries before stalling the producer thread
my $thread_queue = Thread::Queue->new();
my $thread_queue_max = 1000;
my $thread_results = Thread::Queue->new();

# Our array of threads and thread creation
my @threads = ();
for ( 1 .. $thread_count ) { push(@threads, threads->create(\&worker_thread, $_)); }


# The XML file contains UTF-8, but specifying :utf8 encoding causes almost all
#   string functions (including regex, read, etc) to be almost 20x slower
# Instead, we lie to perl here, then later decode() the article to UTF-8 for processing
open(my $xml, '<', $xml_name) || die("Failed to open MediaWiki XML Dump ${xml_name}");
my $buffer = "";

my $total  = 0;
my $parsed = 0;
my $start_time  = time;
my $last_status = time;

my $xml_bytes   = -s $xml_name;
my $total_bytes = 0;


# Read XML data and append it to our buffer, storing the number of read bytes
while( (my $read_bytes = read($xml, $buffer, 64 * 1024, length($buffer))) > 0 ) {
    $total_bytes += $read_bytes;

    # We have some data in our buffer, iterate each full <page> it contains, removing it from the buffer
    while ( $buffer =~ s{^.*?<page>(.*?)</page>}{}is ) {
        my $page = $1;

        # Yes, I'm parsing XML with Regular Expressions
        # MediaWiki dumps are very clean and real XML parsing is way slower (20x)
        # Skip this page unless it has a title, namespace, and text body
        next unless $page =~ m{
            <title>(?'title'.*?)</title> .*?
            <ns>(?'namespace'.*?)</ns>   .*?
            (?'redirect'<redirect[^>]*>  .*?)?
            <text[^>]*>(?'text'.*?)</text>
        }xis;

        my $title     = $+{'title'};
        my $namespace = int( $+{'namespace'} );
        my $redirect  = defined($+{'redirect'});
        my $text      = $+{'text'};
        $total++;


        if ( !defined($namespace) || !defined($text) ) {
            print STDERR "Got strange article:", $/, $page, $/, $/;
            exit;
        }


        # Skip special articles (lists, categories, help pages)
        if ( $namespace != 0 ) { next; }
        if ( $redirect       ) { next; }

        ############ Keep Criteria #############
        # Only articles in our target categories
        my $keep = 0;
        while ( $text =~ /\[\[ Category: ([^\]]+) \]\]/xig ) {
            my $cur_category = $1;
            for my $target_category (@categories) {
                if ( $cur_category =~ $target_category ) { $keep = 1; last; }
            }
        }
        if ( $keep == 0 ) { next; }
        ########################################
        $parsed++;


        # Queue the work and stall if threads are overloaded
        $thread_queue->enqueue($text);
        while ($thread_queue->pending() >= $thread_queue_max) { sleep(1); }

        # Print any pending results
        print_queue();


        # Print a status update every X seconds
        if ( time() - $last_status >= 30 ) {
            printf(STDERR "[%s] Kept %d out of %d articles (%.2f%%)\n",
                scalar(localtime()),
                $parsed,
                $total,
                100 * $parsed / $total
            );


            my $progress_pct = $total_bytes / $xml_bytes;
            my $bytes_per_sec = $total_bytes / ( time() - $start_time );
            my $seconds_to_finish = ($xml_bytes - $total_bytes) / $bytes_per_sec;

            printf(STDERR "    Overall Progress: %.2f%% (ETA %d minutes @ %d MB/s); Current Article: %s\n",
                100 * $progress_pct,
                $seconds_to_finish / 60,
                $bytes_per_sec / 1024 / 1024,
                $title
            );

            printf(STDERR "\n");
            $last_status = time();
        }
    }
}

close($xml);

# Signal end-of-work and wait for queue to finish
$thread_queue->end();
for my $thread (@threads) { $thread->join(); }
print_queue();


printf(STDERR "[%s] Kept %d out of %d articles (%.2f%%)\n",
    scalar(localtime()),
    $parsed,
    $total,
    100 * $parsed / $total
);



#################### Utility Functions #####################

sub print_queue {
    while ( $thread_results->pending() ) {
        print STDOUT $thread_results->dequeue(), $/;
    }
}

# Parses an article body and prints the result to STDOUT
sub worker_thread {
    my $worker_id = shift(@_);

    {
        lock($stderr_lock);
        printf(STDERR "Worker %d started\n", $worker_id);
        select(STDERR)->flush();
    }

    while ( defined(my $text = $thread_queue->dequeue()) ) {
        # Strip any footers or reference sections
        # The shorter we make the article early on, the faster the processing
        $text =~ s/
            ={2,} \s* (?:See[ ]Also|References|Further[ ]Reading|External[ ]Links|Notes) \s* ={2,}
            .*$
        //xsi;

        # We've lied to perl so far about this article body being ASCII, not UTF-8
        # At this point we need to convert to UTF-8 so decode_entities() doesn't barf
        # Because we're handling UTF-8 on our own, setting binmode(STDOUT, ':utf8')
        #   results in double-encoded UTF-8 values, so we opt to never set it
        $text =  decode("UTF-8", $text);

        # Miscellaneous markdown and HTML cleanup
        $text =  decode_entities($text);        # Decode because of embedding in XML (e.g. &lt; &gt; &amp;)
        $text =~ s/&(?:nb|thin)sp;/ /g;         # iconv doesn't transliterate non-breaking spaces very well
        $text =~ s/&[mn]dash;/ - /g;            # mdash and ndash need to work like word breaks
        $text =~ s/&hellip;/... /g;             # Ellipsis need to act like word breaks as well
        $text =  decode_entities($text);        # Decode again because of actual entities (e.g. &omega;)
        $text =~ s/&[A-Za-z0-9]+;//g;           # Unconverted HTML entities (?)
        $text =~ s/<br(?:\s+\/)>/ /g;           # Breaks should at least act as word separators
        $text =~ s/<!--.*?-->//sg;              # HTML <!-- comments -->

        # Remove any HTML tags (nested)
        while ( $text =~ s/
            <(\w+) (?:\s [^>]*)? >
            (?:(?!<\1).)*?
            <\/\1>
        //xsg ) { ; }

        $text =~ s/<\/?\w+[^>]*>//sg;           # Remove any single-element HTML tags

        # Wiki text markup
        $text =~ s/^#.*$//mg;                   # Headings
        $text =~ s/'{2,}//g;                    # ''italic'' and '''bold'''
        $text =~ s/={2,}[^=]+={2,}//g;          # Anything in ==Tag== or ===Tags===

        # Remove any transclusions (nested)
        while ( $text =~ s/
            \{\{
            (?:(?!\{\{).)*?
            \}\}
        //xsg ) { ; }

        # Remove any tables (nested)
        while ( $text =~ s/
            \{\|
            (?:(?!\{\|).)*?
            \|\}
        //xsg ) { ; }


        # Links and embeds
        # Parse all "long" links [[text]] (nested)
        while ( $text =~ s/
            \[\[
            ( (?:(?!\[\[).)*? )
            \]\]
        /parse_long_link($1)/xesg ) { ; }

        $text =~ s/\[ http[^\s\]]+ \s ([^\]]*) \]/$1/xig;   # Any offsite [http text] links
        $text =~ s/https?:\/\/\S+//ig;                      # Any bare http(s) links
        $text =~ s/^[*#;: ]+//mg;                           # Remove list bullets

        $text =~ s/^\s+//; $text =~ s/\s+$//;               # Trim leading and trailing newlines/spaces
        $text =~ s/[\r\n]+/\n/g;                            # Condense blank lines, remove carriage returns


        $thread_results->enqueue($text);
    }
}


# Handles [[ ... ]] links
sub parse_long_link {
    # Handles [[Article]], [[Article|Text]], and [[File:]] type links
    my $link_body = shift(@_);
    if ( length($link_body) == 0 ) { return ""; }

    # Handle "File:", "Category:", "Image:", "Namespace:", by blanking them
    # They always start with an uppercase letter and won't have a space after the colon
    if ( $link_body =~ /^[A-Z]\w+:\S/ ) { return ""; }

    # Links to translated versions of articles always start with lang:Article Name
    # Keep everything after the first colon
    if ( $link_body =~ /^[a-z]+:\S/ ) {
        my @link_parts = split(/\Q:\E/, $link_body, 2);
        return pop(@link_parts);
    }

    # If the link has multiple parts delimited by |s, the last part is the display text
    my @link_parts = split(/\Q|\E/, $link_body, -1);
    return pop(@link_parts);
}


# Handles [url display text] by keeping everything after the first space
sub parse_short_link {
    my $link_body = shift(@_);
    my @link_parts = split(/\s+/, $link_body, 2);
    return pop(@link_parts);
}