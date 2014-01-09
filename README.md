CSC8101 web log analytics coursework, academic year 2013/14
=========

This coursework introduces you to real-time analytics and basic stream processing.

Some analytics applications require that reports or visualizations are available on-demand, usually within sub-second response times i.e. an experience comparable to browsing most modern dynamically rendered web sites.

A typical application domain for such analytics is system log or metric processing, such as for monitoring activity on a group of servers.

Meeting such response time targets, particularly under high load, usually requires storing the data in such as way as to minimise the disk read activity necessary for query execution. This in turn can require denormalising of data and pre-calculation of summary statistics.

In this exercise you are provided with a web server activity log, comprising a time ordered list of the site access activity. Client IP addresses have been replaced with unique identifiers for anonymity.

For part one, the queries you are required to be able to satisfy are:

- for a given client id, start time and end time, show all activity by the client between the times.

- for a given set of URLs, start hour and end hour, show the total number of accesses for each url during the period.

You should write code to process the log file and write database tables, such that each application query can be satisfied efficiently with minimal disk reads.

Your log processing code must not use an amount of memory proportional to the amount data being processed.  You may assume entries in the file are in time order, although this is not a realistic real world assumption.  You are not required to deal with concurrent log streams, or fault tolerance concerns beyond basic exception handling.

The log line format is: client_id timestamp "method url version" status size

Hints:

- Test your code on a subset of the input first - processing the entire file may take hours
- URLs are encoded and thus don't contain spaces. Timestamps contain exactly one space.
- [dd/MMM/yyyy:HH:mm:ss z]
- The database server is capable of handling multiple requests concurrently.

For part two, you are required to identify user sessions and the activity in them. A session is a group of accesses by the same client, separated by a defined interval of inactivity.

Unlike part one, this requires maintaining some state in the stream processor, since you can't immediately identify the end of a session when you see it - it's only apparent after the inactivity period (30 minutes).

The required query is:
- For a given client id, show all sessions with their start time, end time, number of accesses and approximate number of distinct URLs accessed. Provide an approximate number of distinct URLs over all the sessions for the user.

Your log processing code may use an amount of memory proportional to the number of concurrent sessions, but not the total number of sessions, total clients or number of accesses per session.

Hints:

- LinkedHashMap#removeEldestEntry
- HyperLogLog(0.05)

Deliverables
---
Java code to populate and query the database.  Code will be judged on correctness, maintainability and performance. Note the last two are not mutually exclusive, just hard to achieve at the same time.  Your code may use existing 3rd party libraries provided they are open source and you comply with the licence terms.

References
---
- http://cassandra.apache.org/
- http://www.datastax.com/documentation/cassandra/2.0/webhelp/index.html
- https://github.com/datastax/java-driver
- https://github.com/clearspring/stream-lib