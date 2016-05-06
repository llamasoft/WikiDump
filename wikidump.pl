#!/usr/bin/perl

use strict;
use warnings;
no  warnings 'utf8';

use Getopt::Long;
use Pod::Usage;
use Data::Dumper;

use threads;
use threads::shared;
use Thread::Queue;
use Time::HiRes qw{ usleep };

use Encode;
use HTML::Entities;



# First parameter is XML file, second parameter are categories
my $xml_name;
my @categories;
my @transclusions;
my $keep_all = 0;
my $worker_count = 2;


GetOptions(
    "xml=s"                 => \$xml_name,
    "workers=i"             => \$worker_count,
    "categories=s{1,}"      => \@categories,
    "transclusions=s{1,}"   => \@transclusions,
    "help|?"                => sub { pod2usage({ -verbose => 3 }); }
);


# Make sure --xml was included
if ( !defined($xml_name) || length($xml_name) == 0 ) {
    pod2usage("Input file must be specified with --xml option\n");
}


# Should we apply filtering?
if ( scalar(@categories) == 0 && scalar(@transclusions) == 0 ) {
    # If no filtering options were supplied, keep all articles
    printf(STDERR "No filtering applied, all articles will be kept\n");
    $keep_all = 1;

} else {
    # Otherwise, convert transclusions and categories to static regular expressions
    if ( scalar(@categories) > 0 ) {
        printf(STDERR "Including categories: \"%s\"\n", join('", "', @categories));
        @categories = map { qr/\Q$_\E/i; } @categories;
    }

    if ( scalar(@transclusions) > 0 ) {
        printf(STDERR "Including transclusions: \"%s\"\n", join('", "', @transclusions));
        @transclusions = map { qr/\Q$_\E/i; } @transclusions;
    }
}


# Number of worker threads to create and a dummy variable to act as a print lock
my $stdout_lock :shared;
my $stderr_lock :shared;

# Work queue and maximum number of queue entries before stalling the producer thread
my $worker_queue   = Thread::Queue->new();
my $worker_results = Thread::Queue->new();
my $worker_queue_max = 4 * 1024;

# Our array of workers and worker thread creation
my @workers = ();
printf(STDERR "Spawning %d worker threads\n", $worker_count);
for ( 1 .. $worker_count ) { push(@workers, threads->create(\&worker_thread, $_)); }


# The XML file contains UTF-8, but specifying :utf8 encoding causes almost all
#   string functions (including regex, read, etc) to be almost 20x slower
# Instead, we lie to perl here, then later decode() the article to UTF-8 for processing
open(my $xml, '<', $xml_name) || die("Failed to open MediaWiki XML dump ${xml_name}");
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
        # MediaWiki dumps are very clean and even XML::Simple parsing is way slower (20x)
        # Skip this page unless it has a title, namespace, and text body (with content)
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

        # Skip special articles (lists, categories, help pages)
        if ( $namespace != 0 ) { next; }
        if ( $redirect       ) { next; }
        $total++;


        ########## Article Filtering ###########
        my $keep = $keep_all;

        # First check for categories
        if ( scalar(@categories) > 0 ) {
            pos($text) = 0;

            while ( !$keep && $text =~ /\[\[ Category: ([^\[\]\n]+) \]\]/xig ) {
                my $cur_category = $1;

                for my $target_category (@categories) {
                    if ( $cur_category =~ $target_category ) { $keep = 1; last; }
                }
            }
        }

        # Then check transclusions (but not citations)
        if ( scalar(@transclusions) > 0 ) {
            pos($text) = 0;

            while ( !$keep && $text =~ /\{\{ (?!cite) ([^\{\}\n]+) \}\}/xig ) {
                my $cur_transclusion = $1;

                for my $target_transclusion (@transclusions) {
                    if ( $cur_transclusion =~ $target_transclusion ) { $keep = 1; last; }
                }
            }
        }

        # If we have no reason to keep, skip this article
        if ( !$keep ) { next; }
        ########################################


        # Queue the work and stall if threads are overloaded
        $parsed++;
        $worker_queue->enqueue($text);
        while ($worker_queue->pending() >= $worker_queue_max) { usleep(0.1 * 1000000); }

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

            # We start to slow down after the first few minutes
            # Inflate the ETA by a percentage based on our current run time
            my $error_fudge = 1 + ( 1 / sqrt(time() - $start_time) );

            printf(STDERR "    Overall Progress: %.2f%% (ETA %d minutes @ %d MB/s); Current Article: %s\n",
                100 * $progress_pct,
                ($seconds_to_finish / 60) * $error_fudge,
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
$worker_queue->end();
for my $worker (@workers) { $worker->join(); }
print_queue();


printf(STDERR "[%s] Kept %d out of %d articles (%.2f%%)\n",
    scalar(localtime()),
    $parsed,
    $total,
    100 * $parsed / $total
);



#################### Utility Functions #####################

sub print_queue {
    while ( $worker_results->pending() ) {
        print STDOUT $worker_results->dequeue(), $/;
    }
}

# Parses an article body and prints the result to STDOUT
sub worker_thread {
    my $worker_id = shift(@_);

    # These are the HTML tags whose content we wish to keep
    # All other HTML tags and their content will be removed
    # This list is derived from the following help page:
    #   https://en.wikipedia.org/wiki/Help:HTML_in_wikitext
    my $html_keeps = join('|', (
        # Quotes and blocks
        'blockquote', 'div', 'span', 'center', 'p',

        # Headings
        'h1', 'h2', 'h3', 'h4', 'h5', 'h6',

        # Code and code-like blocks
        'pre', 'code', 'syntaxhighlight', 'poem', 'tt',

        # Font size
        'small', 'big', 'sub', 'sup',

        # Font style
        'abbr', 'b', 'bdi', 'bdo', 'cite', 'data',
        'del', 'dfn', 'em', 'font', 'i', 'ins', 'kbd',
        'mark', 'q', 's', 'samp', 'strike', 'strong',
        'time', 'u', 'var', 'wbr',

        # List elements
        'dl', 'dt', 'dd', 'ol', 'ul', 'li',

        # Table elements
        'table', 'td', 'tr', 'th',
        'thead', 'tfoot', 'tbody', 'caption'
    ));
    $html_keeps = qr{$html_keeps}i;


    while ( defined(my $text = $worker_queue->dequeue()) ) {
        # Strip any footers or reference sections
        # The shorter we make the article early on, the faster the processing
        $text =~ s/
            ={2,} \s* (?:See[ ]Also|References|Further[ ]Reading|External[ ]Links|Notes) \s* ={2,}
            .*$
        //xsi;

        # We've lied to perl so far about this article body being ASCII, because it's actually UTF-8
        # We convert to UTF-8 so decode_entities() doesn't mix encodings if it decodes a UTF-8 entitiy like &egrave;
        # After the decode, the $text will use perl's internal UTF-8 representation (each Unicode character
        #   will be a single character with an ord() value allowed to go over 256)
        # We will re-encode this before placing it in the result queue
        $text = decode("UTF-8", $text);

        # Miscellaneous markdown and HTML cleanup
        $text =  decode_entities($text);        # Decode because of embedding in XML (e.g. &lt; &gt; &amp;)
        $text =~ s/&(?:nb|thin)sp;/ /g;         # iconv doesn't transliterate non-breaking spaces very well
        $text =~ s/&shy;//g;                    # Soft hyphens are invisible and break things
        $text =~ s/&[mn]dash;/ - /g;            # mdash and ndash need to work like word breaks
        $text =~ s/&hellip;/... /g;             # Ellipsis need to act like word breaks as well
        $text =~ s/(\.{2,})\b(?!\s)/$1 /g;      # Any series of ellipsis should have a space afterwards
        $text =  decode_entities($text);        # Decode again because of actual entities (e.g. &omega;)
        $text =~ s/&[A-Za-z0-9]+;//g;           # Unconverted HTML entities (?)
        $text =~ s/<[bh]r(?:\s+\/)>/ /g;        # Breaks should at least act as word separators
        $text =~ s/<!--.*?-->//sg;              # HTML <!-- comments -->

        # Convert back to multibyte UTF-8 instead of perl's internal representation
        # The regex engine is a good bit faster when each character's ord() is under 256
        $text = encode("UTF-8", $text);


        # There are a few HTML tags whose content we want to keep (recursive, inside to out)
        # Note: ( (?: (?!X) . )*? ) is "everything as long as it doesn't contain X"
        #   You will see this used to make sure we don't match nested items
        while ( $text =~ s/
            <($html_keeps) (?:\s [^>]*)? >
            ( (?:(?!<\1).)*? )
            <\/\1>
        /$2/xsig ) { ; }

        # Remove any remaining HTML tags
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

        # Remove any transclusions (recursive, inside to out)
        while ( $text =~ s/
            \{\{
            (?:(?!\{\{).)*?
            \}\}
        //xsg ) { ; }

        # Remove any tables (recursive, inside to out)
        while ( $text =~ s/
            \{\|
            (?:(?!\{\|).)*?
            \|\}
        //xsg ) { ; }


        # Links and embeds
        # Parse all "long" links [[text]] (recursive, inside to out)
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


        $worker_results->enqueue($text);
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

    # Intra-article header links are blanked
    if ( $link_body =~ /^#/ ) { return ""; }

    # Links to translated versions of articles always start with lang:Article Name
    # The exception to this is "w:" which is a dummy entry for pipe tricks
    # Keep everything after the first colon
    if ( $link_body =~ /^[a-z]+:\S/ && $link_body !~ /^w:/ ) {
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



__END__
=head1 NAME

B<WikiDump> - Processes a WikiMedia XML dump file and prints the stripped markup text.

=head1 SYNOPSIS

B<wikidump.pl> B<--xml> F<path/to/dump.xml> [B<--workers> N] [B<--categories> ...] [B<--transclusions> ...]

Parsed articles are written to STDOUT, status updates are written to STDERR.
Filtering can be applied to include only certain articles.  If no filtering options are specified, all articles are kept.


B<Options>

=over 4

=item B<--xml>, B<-x>

The MediaWiki XML dump to process, this option is required

=item B<--workers>, B<-w>

The number of worker threads to spawn (default 2)

=item B<--categories>, B<-c>

Articles whose categores contain any of these entries will be kept

=item B<--transclusions>, B<-t>

Articles whose transclusions contain any of these entires will be kept

=item B<--help>

Displays a full help guide including additional parameter details

=back


B<Examples>

=over 4

B<wikidump.pl> B<--xml> F<dump.xml> B<--transclusions> "featured article" "good article" > enwiki_quality.txt

B<wikidump.pl> B<--xml> F<dump.xml> B<--categories> "novels" "books" > enwiki_books.txt

B<wikidump.pl> B<--xml> F<dump.xml> > enwiki_everything.txt

=back


=head1 OPTIONS

=over 4

=item B<--xml> F<path>

The path to the MediaWiki XML dump file to be processed.


=item B<--workers N>

The number of worker threads spawned to parse MediaWiki markdown.  By default, B<2> workers are used.

Workers are I<only> responsible for parsing the MediaWiki markdown into plain text, not filtering articles.
Filters that result in low keep rates may result in underutilized workers.


=item B<--categories  "string1"  "string2"  ...>

Articles whose categories contain I<any> of these case-insensitive strings will be kept.

An article's categories are determined by the C<[[Category:]]> tags that its markdown contains.
Multiple values may be defined by separating them with spaces (e.g. I<"space travel"> I<"NASA">).

Information about markdown categories can be found here: L<https://en.wikipedia.org/wiki/Help:Category>


=item B<--transclusions  "string1"  "string2"  ...>

Articles whose transclusions contain I<any> of these case-insensitive strings will be kept.

An article's transclusions are determined by the C<{{ ... }}> tags that its markdown contains.
Multiple values may be defined by separating them with spaces (e.g. I<"featured article"> I<"good article">).

Information about markdown transclusions can be found here: L<https://en.wikipedia.org/wiki/Wikipedia:Transclusion>

=back



=head1 DESCRIPTION

This program processes a MediaWiki XML dump file and prints the stripped markup text to STDOUT and status messages to STDERR.

Filtering with B<--categories> or B<--transclusions> can narrow down the articles printed, useful for generating topical word frequencies, wordlists, or N-grams.
Both B<--categories> and B<--transclusions> use inclusive filtering.  If any string from either option results in a match, the article will be kept.
If no filtering criteria is specified, all articles will be kept.

The latest English Wikipedia XML dumps can be found at L<https://dumps.wikimedia.org/enwiki/latest/>.

This program was created to parse F<enwiki-latest-pages-articles.xml>, but may parse other files as well.  Your mileage may vary.

=cut
