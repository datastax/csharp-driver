using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.IntegrationTests.TestBase;
using Cassandra.Serialization;
using Dse.Geometry;

namespace Dse.Test.Integration.Geometry
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
