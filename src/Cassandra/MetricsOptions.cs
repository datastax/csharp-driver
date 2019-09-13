// 
//       Copyright (C) DataStax Inc.
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//       http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.

using System.Collections.Generic;

namespace Cassandra
{
    public class MetricsOptions
    {
        public IEnumerable<string> DisabledNodeMetrics { get; private set; } = new List<string>();

        public IEnumerable<string> DisabledSessionMetrics { get; private set; } = new List<string>();

        public IEnumerable<string> Context { get; private set; } = new List<string>();
        
        public MetricsOptions SetDisabledNodeMetrics(IEnumerable<string> disabledNodeMetrics)
        {
            DisabledNodeMetrics = disabledNodeMetrics;
            return this;
        }

        public MetricsOptions SetDisabledSessionMetrics(IEnumerable<string> disabledSessionMetrics)
        {
            DisabledSessionMetrics = disabledSessionMetrics;
            return this;
        }

        public MetricsOptions SetContext(params string[] context)
        {
            Context = context;
            return this;
        }

        internal MetricsOptions Clone()
        {
            return new MetricsOptions
            {
                DisabledNodeMetrics = DisabledNodeMetrics,
                DisabledSessionMetrics = DisabledSessionMetrics,
                Context = Context
            };
        }
    }
}