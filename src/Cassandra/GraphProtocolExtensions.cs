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

using System.Collections.Generic;

namespace Cassandra
{
    /// <summary>
    /// Defines extension methods for graph protocol versions
    /// </summary>
    internal static class GraphProtocolExtensions
    {
        private static readonly IDictionary<GraphProtocol, string> EnumToNameMap =
            new Dictionary<GraphProtocol, string>
            {
                { GraphProtocol.GraphSON1, "graphson-1.0" },
                { GraphProtocol.GraphSON2, "graphson-2.0" },
                { GraphProtocol.GraphSON3, "graphson-3.0" },
            };

        public static string GetInternalRepresentation(this GraphProtocol? version)
        {
            return version == null ? "null" : version.Value.GetInternalRepresentation();
        }

        public static string GetInternalRepresentation(this GraphProtocol version)
        {
            return GraphProtocolExtensions.EnumToNameMap[version];
        }
    }
}