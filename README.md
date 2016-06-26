**NOTE: This project is deprecated.  At the time was in use, there were three different versions of the
"fitnessjiffy" application, working with three different databases having somewhat different data models.
Today, only the [Java/Spring-based version](https://github.com/steve-perkins/fitnessjiffy-spring) is still
in active development, and I do not anticipate major changes to the database or data model anytime soon.**

# FitnessJiffy ETL

Utilities for importing, exporting, and migrating data between different versions of the
[FitnessJiffy](https://github.com/steve-perkins/fitnessjiffy-spring) diet and exercise tracking application.  
SQLite, H2, and PostgreSQL databases are currently supported.

The codebase includes "reader" classes for all supported databases, which load the entire
database into a neutral JSON backup format.  There are also "writer" classes for all
supported databases, which import that JSON data into an empty database, creating all of
the necessary tables if they don't already exist.

By default, this project builds a monolithic uber-JAR with all dependencies bundled.  The main 
class of this executable JAR, `net.steveperkins.fitnessjiffy.etl.Migrate`, allows you to backup/restore 
a database or migrate between different database versions.

WARNING: To prevent dates from being one day off due to time zone mangling, it is best for the 
system clocks on all machines involved to be set to UTC time.
