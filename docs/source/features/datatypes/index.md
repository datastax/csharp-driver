# CQL data types to C# types

When retrieving the value of a column from a `Row` object, you use a getter based on the type of the column.

CQL data type|C# type
---|---
ascii|string
bigint|long
blob|byte[]
boolean|bool
counter|long
custom|byte[]
date|[LocalDate](datetime/index)
decimal|decimal
double|double
duration|Duration
float|float
inet|IPAddress
int|int
list|IEnumerable&lt;T&gt;
map|IDictionary&lt;K, V&gt;
set|IEnumerable&lt;T&gt;
smallint|short
text|string
time|[LocalTime](datetime/index)
timestamp|[DateTimeOffset](datetime/index)
timeuuid|TimeUuid
tinyint|sbyte
uuid|Guid
varchar|string
varint|BigInteger
vector|[CqlVector](../vectors/index)


:::{toctree}
:hidden:
:maxdepth: 1

datetime/index
nulls-unset/index
:::
