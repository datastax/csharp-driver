# Date and time representation

## Timestamp

Cassandra timestamp, a single moment in time with millisecond precision, is represented as `DateTimeOffset`.

## Date

Introduced in Cassandra 2.2, a date portion without a time-zone, is represented as a [`LocalDate`][localdate-api].

## Time

Introduced in Cassandra 2.2, a time portion without a time-zone, is represented as a [`LocalTime`][localtime-api].

[localdate-api]: http://docs.datastax.com/en/latest-csharp-driver-api/html/T_Cassandra_LocalDate.htm
[localtime-api]: http://docs.datastax.com/en/latest-csharp-driver-api/html/T_Cassandra_LocalTime.htm