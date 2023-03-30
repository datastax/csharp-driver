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

namespace Cassandra.DataStax.Insights.Schema.Converters
{
    internal class ConsistencyInsightsConverter : InsightsEnumConverter<ConsistencyLevel, string>
    {
        private static readonly IReadOnlyDictionary<ConsistencyLevel, string> ConsistencyLevelStringMap =
            new Dictionary<ConsistencyLevel, string>
            {
                { ConsistencyLevel.All, "ALL" },
                { ConsistencyLevel.Any, "ANY" },
                { ConsistencyLevel.EachQuorum, "EACH_QUORUM" },
                { ConsistencyLevel.LocalOne, "LOCAL_ONE" },
                { ConsistencyLevel.LocalQuorum, "LOCAL_QUORUM" },
                { ConsistencyLevel.LocalSerial, "LOCAL_SERIAL" },
                { ConsistencyLevel.One, "ONE" },
                { ConsistencyLevel.Quorum, "QUORUM" },
                { ConsistencyLevel.Serial, "SERIAL" },
                { ConsistencyLevel.Three, "THREE" },
                { ConsistencyLevel.Two, "TWO" }
            };

        protected override IReadOnlyDictionary<ConsistencyLevel, string> EnumToJsonValueMap => 
            ConsistencyInsightsConverter.ConsistencyLevelStringMap;
    }
}