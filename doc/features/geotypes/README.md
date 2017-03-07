# Geospatial types

[DataStax Enterprise][dse] 5 comes with a set of additional types to represent geospatial data:

- `PointType`
- `LineStringType`
- `PolygonType`

```
cqlsh> CREATE TABLE points_of_interest(name text PRIMARY KEY, coords 'PointType');
cqlsh> INSERT INTO points_of_interest (name, coords) VALUES ('Eiffel Tower', 'POINT(48.8582 2.2945)');
```

The DSE driver includes C# representations of these types in the `Dse.Geometry` namespace that can be used directly
as parameters in queries. All C# geospatial types implement `ToString()`, that returns the string representation
in [Well-known text][wkt] format.

```csharp
using Dse.Geometry;
```

```csharp
Row row = session.Execute("SELECT coords FROM points_of_interest WHERE name = 'Eiffel Tower'").First();
Point coords = row.GetValue<Point>("coords");

var statement = new SimpleStatement("INSERT INTO points_of_interest (name, coords) VALUES (?, ?)",
    "Washington Monument", 
    new Point(38.8895, 77.0352));
session.Execute(statement);
```

[dse]: http://www.datastax.com/products/datastax-enterprise
[wkt]: https://en.wikipedia.org/wiki/Well-known_text
[geojson]: https://en.wikipedia.org/wiki/GeoJSON