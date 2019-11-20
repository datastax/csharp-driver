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
using Cassandra.DataStax.Insights.Schema;

namespace Cassandra.DataStax.Insights.MessageFactories
{
    internal class InsightsMetadataFactory : IInsightsMetadataFactory
    {
        private readonly IInsightsMetadataTimestampGenerator _unixTimestampGenerator;

        public InsightsMetadataFactory(IInsightsMetadataTimestampGenerator unixTimestampGenerator)
        {
            _unixTimestampGenerator = unixTimestampGenerator;
        }

        public InsightsMetadata CreateInsightsMetadata(string messageName, string mappingId, InsightType insightType)
        {
            var millisecondsSinceEpoch = _unixTimestampGenerator.GenerateTimestamp();

            return new InsightsMetadata
            {
                Name = messageName,
                InsightMappingId = mappingId,
                InsightType = insightType,
                Tags = new Dictionary<string, string>
                {
                    { "language" , "csharp" }
                },
                Timestamp = millisecondsSinceEpoch
            };
        }
    }
}