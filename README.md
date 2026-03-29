# Scheduler

Quartz starter project backed by an H2 file database.

## What is included
- Quartz JDBC job store configuration
- H2 file database under `./data`
- Official Quartz H2 schema script copied into project resources
- A sample `LoggingJob`, `JobDetail`, `Trigger`, and bootstrap class

## Run
```powershell
C:\Tools\apache-maven-3.9.11\bin\mvn.cmd clean package
C:\Tools\apache-maven-3.9.11\bin\mvn.cmd exec:java
```

The bootstrap creates the Quartz schema if it is missing, registers a sample job, and starts the scheduler.

## H2 data
The local database file is created at `./data/quartz-db`.

You do not need a separate DB editor to run scripts. H2 can be inspected with its own shell or any JDBC-capable SQL client if you want a UI.
