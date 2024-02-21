# Date and time representation

## Timestamp

A single moment in time with millisecond precision, is represented as `DateTimeOffset`.

## Date

Introduced in Cassandra 2.2, a date portion without a time-zone, is represented as a [LocalDate][localdate-api].

## Time

Introduced in Cassandra 2.2, a time portion without a time-zone, is represented as a [LocalTime][localtime-api].

[localdate-api]: https://docs.datastax.com/en/drivers/csharp/latest/api/Cassandra.LocalDate.html
[localtime-api]: https://docs.datastax.com/en/drivers/csharp/latest/api/Cassandra.LocalTime.html
