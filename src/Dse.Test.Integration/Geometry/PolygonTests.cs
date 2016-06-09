using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.IntegrationTests.TestBase;
using Dse.Geometry;

namespace Dse.Test.Integration.Geometry
{
    public class PolygonTests : GeometryTests<Polygon>
    {
        protected override Polygon[] Values
        {
            get
            {
                return new[]
                {
                    new Polygon(new Point(1, 3), new Point(3, -11.2), new Point(3, 6.2), new Point(1, 3)),
                    new Polygon(
                        new[] {new Point(-10, 10), new Point(10, 0), new Point(10, 10), new Point(-10, 10)},
                        new[] {new Point(6, 7), new Point(3, 9), new Point(9, 9), new Point(6, 7)}),
                    new Polygon()
                };
            }
        }

        protected override string TypeName
        {
            get { return "PolygonType"; }
        }
    }
}
