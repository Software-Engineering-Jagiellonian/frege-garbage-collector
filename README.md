# Frege Garbage Collector

Garbage collector for a Frege project.

Docker image available as `jagiellonian/frege-garbage-collector`

## Docker environment variables

* `RMQ_HOST` - RabbitMQ host
* `RMQ_PORT` - RabbitMQ port (*optional - default 5672*)


* `DB_HOST` - PostgreSQL server host
* `DB_PORT` - PostgreSQL server host (*optional - default 5432*)
* `DB_DATABASE` - Database name
* `DB_USERNAME` - Database username
* `DB_PASSWORD` - Database password

## Bind path

You must bind a `/repositories` path to folder where repositories are stored
