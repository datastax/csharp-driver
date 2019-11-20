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
    internal class InsightTypeInsightsConverter : InsightsEnumConverter<InsightType, string>
    {
        private static readonly IReadOnlyDictionary<InsightType, string> InsightTypeStringMap =
            new Dictionary<InsightType, string>
            {
                { InsightType.Event, "EVENT" }
            };

        protected override IReadOnlyDictionary<InsightType, string> EnumToJsonValueMap => 
            InsightTypeInsightsConverter.InsightTypeStringMap;
    }
}