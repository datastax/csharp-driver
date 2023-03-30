//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//
using Cassandra.Geometry;

namespace Cassandra.IntegrationTests.Geometry
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
