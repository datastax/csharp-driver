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
using System.Linq;
using Cassandra.IntegrationTests.SimulacronAPI.Models.Converters;

namespace Cassandra.IntegrationTests.SimulacronAPI.PrimeBuilder.When
{
    public class WhenQueryBuilder : IWhen, IWhenQueryBuilder
    {
        private readonly string _query;
        private readonly List<(string, string)> _namesToTypes = new List<(string, string)>();
        private readonly List<object> _values = new List<object>();
        private string[] _consistency;

        public WhenQueryBuilder(string query)
        {
            this._query = query;
        }

        public IWhenQueryBuilder WithNamedParam(string name, DataType type, object value)
        {
            _namesToTypes.Add((name, type.Value));
            _values.Add(value);
            return this;
        }


        public IWhenQueryBuilder WithParam(DataType type, object value)
        {
            _namesToTypes.Add(($"column{_namesToTypes.Count}", type.Value));
            _values.Add(value);
            return this;
        }

        public IWhenQueryBuilder WithParam(object value)
        {
            return WithParam(DataType.GetDataType(value), value);
        }

        public IWhenQueryBuilder WithParams(params object[] values)
        {
            IWhenQueryBuilder @this = this;
            foreach (var v in values)
            {
                @this = this.WithParam(v);
            }

            return @this;
        }

        public IWhenQueryBuilder WithParams(params (DataType, object)[] values)
        {
            IWhenQueryBuilder @this = this;
            foreach (var v in values)
            {
                @this = this.WithParam(v.Item1, v.Item2);
            }

            return @this;
        }

        public IWhenQueryBuilder WithConsistency(params ConsistencyLevel[] consistencyLevels)
        {
            _consistency = consistencyLevels.Select(ConsistencyLevelEnumConverter.ConvertConsistencyLevelToString).ToArray();
            return this;
        }

        public object Render()
        {
            var dictionary = new Dictionary<string, object>()
            {
                { "request", "query" },
                { "query", _query }
            };

            if (_namesToTypes != null && _values != null)
            {
                var parameters =
                    _namesToTypes
                        .Zip(_values, (tuple, value) => (tuple.Item1, value))
                        .ToDictionary(kvp => kvp.Item1, kvp => kvp.value);

                dictionary.Add("params", parameters);
                dictionary.Add("param_types", _namesToTypes.ToDictionary(tuple => tuple.Item1, tuple => tuple.Item2));
            }

            if (_consistency != null)
            {
                dictionary.Add("consistency_level", _consistency);
            }

            return dictionary;
        }
    }
}