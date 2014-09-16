[![Build Status](https://drone.io/github.com/steve-perkins/fitnessjiffy-etl/status.png)](https://drone.io/github.com/steve-perkins/fitnessjiffy-etl/latest)

fitnessjiffy-etl
================

Utilities for importing, exporting, and migrating data between different versions of the
FitnessJiffy demo application.  SQLite, H2, and PostgreSQL databases are currently supported.

The codebase includes "reader" classes for all supported databases, which load the entire
database into a neutral JSON backup format.  There are also "writer" classes for all
supported databases, which import that JSON data into an empty database, creating all of
the necessary tables if they don't already exist.

By default, this project builds a monolithic uber-JAR with all dependencies bundled.  The main 
class of this executable JAR, `net.steveperkins.fitnessjiffy.etl.Migrate`, allows you to backup/restore 
a database or migrate between different database versions.
