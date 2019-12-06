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

namespace Cassandra.IntegrationTests.SimulacronAPI.When
{
    public class WhenQueryFluent : IWhen, IWhenQueryFluent
    {
        private readonly string _query;
        private readonly List<(string, string)> _namesToTypes = new List<(string, string)>();
        private readonly List<object> _values = new List<object>();

        public WhenQueryFluent(string query)
        {
            _query = query;
        }
        
        public IWhenQueryFluent WithParam(string name, string type, object value)
        {
            _namesToTypes.Add((name, type));
            _values.Add(value);
            return this;
        }

        public object Render()
        {
            if (_namesToTypes != null && _values != null)
            {
                var parameters = 
                    _namesToTypes
                        .Zip(_values, (tuple, value) => (tuple.Item1, value))
                        .ToDictionary(kvp => kvp.Item1, kvp => kvp.value);

                return new
                {
                    query = _query,
                    @params = parameters,
                    param_types = _namesToTypes.ToDictionary(tuple => tuple.Item1, tuple => tuple.Item2)
                };
            }

            return new { query = _query };
        }
    }
}