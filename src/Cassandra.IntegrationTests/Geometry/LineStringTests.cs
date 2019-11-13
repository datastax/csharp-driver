//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cassandra.IntegrationTests.TestClusterManagement;
using Cassandra.Geometry;

namespace Cassandra.IntegrationTests.Geometry
{
    public class LineStringTests : GeometryTests<LineString>
    {
        protected override LineString[] Values
        {
            get
            {
                return new[]
                {
                    new LineString(new Point(1.2, 3.9), new Point(6.2, 18.9)),
                    new LineString(new Point(-1.2, 1.9), new Point(111, 22)),
                    new LineString(new Point(0.21222, 32.9), new Point(10.21222, 312.9111), new Point(4.21222, 6122.9))
                };
            }
        }

        protected override string TypeName
        {
            get { return "LineStringType"; }
        }
    }
}
