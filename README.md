Masschusetts Bay Transit Authority Performance Tracker Scraper
-------------------------------------------------------------

This script crawls the [MBTA Back On Track Website](www.mbtabackontrack.com)
and pulls key performance metrics from it.

Specifically, all Green, Red, Orange, and Blue line reliability metrics are
pulled straight from the website, in addiition to all the bus lines that run
through the cities that the MBTA serves.

This script is originally adapted from one I used for work while at the City
of Boston. That version of this script is runs on a schedule where it pulls 
MBTA reliability metrics daily. The script originally writes this data to a
PostgreSQL database for use by other analysts. The odbc driver, connection string,
and write code can be easily adapted for other platforms (e.g., replace psycopg2 
with pymssql to write to MS SQL Server).


Originally, data was pulled from PostgreSQL, into a Tableau dashboard,
which appears in Mayor Marty Walsh's office for his personal viewing.
