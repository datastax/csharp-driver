//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace Dse.Serialization.Graph.GraphSON2
{
    internal class GraphSON2ContractResolver : DefaultContractResolver
    {
        /// <summary>
        /// A single instance of a JsonSerializerSettings that uses this ContractResolver.
        /// </summary>
        internal static readonly JsonSerializerSettings Settings = new JsonSerializerSettings
        {
            ContractResolver = new GraphSON2ContractResolver()
        };

        protected GraphSON2ContractResolver()
        {

        }

        protected override JsonContract CreateContract(Type objectType)
        {
            var contract = base.CreateContract(objectType);
            if (GraphSON2Converter.Instance.CanConvert(objectType))
            {
                contract.Converter = GraphSON2Converter.Instance;
            }
            return contract;
        }
    }
}
