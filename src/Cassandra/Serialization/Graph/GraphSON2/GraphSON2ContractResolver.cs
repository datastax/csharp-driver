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

using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace Cassandra.Serialization.Graph.GraphSON2
{
    //internal class GraphSON2ContractResolver : DefaultContractResolver
    //{
    //    /// <summary>
    //    /// A single instance of a JsonSerializerSettings that uses this ContractResolver.
    //    /// </summary>
    //    internal static readonly JsonSerializerSettings Settings = new JsonSerializerSettings
    //    {
    //        ContractResolver = new GraphSON2ContractResolver()
    //    };

    //    protected GraphSON2ContractResolver()
    //    {

    //    }

    //    protected override JsonContract CreateContract(Type objectType)
    //    {
    //        var contract = base.CreateContract(objectType);
    //        if (GraphSON2Converter.Instance.CanConvert(objectType))
    //        {
    //            contract.Converter = GraphSON2Converter.Instance;
    //        }
    //        return contract;
    //    }
    //}
}
