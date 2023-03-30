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
