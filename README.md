To run the four featurs
fansite-analytics-challenge$ sbt run

Feature 1:
Lists the top 10 most active host/IP addresses that have accessed the site.

Feature 12 (extra)
Lists the top 10 host/IP addresses that consume the most bandwidth on the site.

Feature 1 with scalaz:
This a trial to stream from the log file
It lists the top 10 most active host/IP addresses that have accessed the site, by streaming the logs from the log file using scalaz stream library.

Feature 2:
Lists the 10 resources that consume the most bandwidth on the site.

Feature 3:
Lists the top 10 busiest (or most frequently visited) 60-minute periods.

Feature 4:
Lists all blocked attempts for 5 minutes after detecting patterns of three failed login attempts from the same IP address over 20 seconds.