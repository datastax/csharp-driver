//
//       Copyright (C) DataStax, Inc.
//
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Collections.Generic;

namespace Cassandra.Insights.Schema.Converters
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