CSC8101 web log analytics coursework, academic year 2014/15
=========

This coursework introduces you to real-time analytics and basic stream processing.

Some analytics applications require that reports or visualizations are available on-demand, usually within sub-second response times i.e. an experience comparable to browsing most modern dynamically rendered web sites.

A typical application domain for such analytics is system log or metric processing, such as for monitoring activity on a group of servers.

Meeting such response time targets, particularly under high load, usually requires storing the data in such as way as to minimise the disk read activity necessary for query execution. This in turn can require denormalising of data and pre-calculation of summary statistics.

In this exercise you are provided with web server activity data, comprising a time ordered list of the site access activity. Client IP addresses have been replaced with unique identifiers for anonymity.

For part one, the required query is:

- for a given set of URLs, start hour and end hour, show the total number of accesses for each url during each hour the period.

You should write code to process the log data and write a database table, such that each application query can be satisfied efficiently with minimal disk reads.

Your log processing code must not use an amount of memory proportional to the amount data being processed, the number of distinct URLs or the data time span.

You may assume entries in the origin file are in time order, although this is not a realistic real world assumption.

You are not required to deal with fault tolerance concerns beyond basic exception handling.

You are not required to provide command line argument parsing for your query client, but the query parameters should be easily editable in your source file.

The log line format is: client_id timestamp "method url version" status size

Hints:

- URLs are encoded and thus don't contain spaces. Timestamps contain exactly one space.
- [dd/MMM/yyyy:HH:mm:ss z]
- The database server is capable of handling multiple requests concurrently.

For part two, you are required to identify user sessions and the activity in them. A session is a group of accesses by the same client, separated by a defined interval of inactivity.

Unlike part one, this requires maintaining some state in the stream processor, since you can't immediately identify the end of a session when you see it - it's only apparent after the inactivity period (30 minutes).

The required query is:
- For a given client id, show all sessions with their start time, end time, number of accesses and approximate number of distinct URLs accessed.
- Provide an approximate number of distinct URLs over all the sessions for the given client id.

Your log processing code may use an amount of memory approximately proportional to the number of concurrent sessions, but not the total number of sessions, total clients or number of accesses per session.

Hints:

- LinkedHashMap#removeEldestEntry
- HyperLogLog(0.05)

Deliverables
---
Java code to populate and query the database. The processing should be MessageHandler plus any additional code files you create to call from it. The processing code should populate the tables for both parts 1 and 2 in a single pass over the log data. The query code may be individual classes each having either a main method or unit tests (or both).

Code will be judged on correctness, maintainability and performance. Note the last two are not mutually exclusive, just hard to achieve at the same time.  Your code may use existing 3rd party libraries provided they are open source and you comply with the licence terms.

References
---
- http://kafka.apache.org/
- http://cassandra.apache.org/
- http://www.datastax.com/documentation/cassandra/2.1/cassandra/gettingStartedCassandraIntro.html
- https://github.com/datastax/java-driver
- https://github.com/clearspring/stream-lib