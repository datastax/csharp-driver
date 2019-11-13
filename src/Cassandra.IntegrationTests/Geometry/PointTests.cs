//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using Cassandra.Geometry;

namespace Cassandra.IntegrationTests.Geometry
{
    public class PointTests : GeometryTests<Point>
    {
        protected override Point[] Values
        {
            get
            {
                return new[]
                {
                    new Point(1.2, 3.9),
                    new Point(-1.2, 1.9),
                    new Point(0.21222, 3122.9)
                };
            }
        }

        protected override string TypeName
        {
            get { return "PointType"; }
        }
    }
}
