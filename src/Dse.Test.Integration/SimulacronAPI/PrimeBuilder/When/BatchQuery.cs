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

namespace Dse.Test.Integration.SimulacronAPI.PrimeBuilder.When
{
    public class BatchQuery
    {
        public string QueryOrId { get; set; }

        public Dictionary<string, string> ParamTypes { get; set; }

        public Dictionary<string, object> Params { get; set; }

        public IDictionary<string, object> ToDictionary()
        {
            var dict = new Dictionary<string, object>
            {
                {"query", QueryOrId}
            };

            if (Params == null)
            {
                return dict;
            }

            if (Params.Count != ParamTypes.Count)
            {
                throw new InvalidOperationException("Types don't match the number of parameters.");
            }

            dict.Add("params", Params);
            dict.Add("param_types", ParamTypes);

            return dict;
        }
    }
}