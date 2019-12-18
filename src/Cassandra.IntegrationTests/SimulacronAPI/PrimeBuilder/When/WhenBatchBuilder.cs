// 
//       Copyright (C) 2019 DataStax Inc.
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
// 

using System;
using System.Collections.Generic;
using System.Linq;
using Cassandra.IntegrationTests.SimulacronAPI.Models.Converters;

namespace Cassandra.IntegrationTests.SimulacronAPI.PrimeBuilder.When
{
    public class WhenBatchBuilder : IWhen, IWhenBatchBuilder
    {
        private readonly List<BatchQuery> _queriesOrIds = new List<BatchQuery>();
        
        private readonly List<ConsistencyLevel> _allowedCls = new List<ConsistencyLevel>();
        
        public object Render()
        {
            var dictionary = new Dictionary<string, object>
            {
                { "request", "batch" },
                { "queries", _queriesOrIds.Select(q => q.ToDictionary()) },
                { "consistency_level", _allowedCls.Select(ConsistencyLevelEnumConverter.ConvertConsistencyLevelToString) }
            };

            return dictionary;
        }

        public IWhenBatchBuilder WithQueries(params string[] queries)
        {
            _queriesOrIds.AddRange(queries.Select(q => new BatchQuery { QueryOrId = q }));
            return this;
        }

        public IWhenBatchBuilder WithQueries(params BatchQuery[] queries)
        {
            _queriesOrIds.AddRange(queries);
            return this;
        }

        public IWhenBatchBuilder WithAllowedConsistencyLevels(params ConsistencyLevel[] consistencyLevels)
        {
            _allowedCls.AddRange(consistencyLevels);
            return this;
        }
    }
}