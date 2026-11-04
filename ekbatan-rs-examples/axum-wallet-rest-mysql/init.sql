-- Runs as root when the container is first created. The named user (`wallet`) only has rights
-- on its own database, and the first migration creates the `eventlog` database and writes
-- tables into it, so it gets rights on every database. Java's examples grant the same.
GRANT ALL PRIVILEGES ON *.* TO 'wallet'@'%';
FLUSH PRIVILEGES;
