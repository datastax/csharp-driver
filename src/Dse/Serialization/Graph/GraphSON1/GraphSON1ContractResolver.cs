//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;

namespace Dse.Serialization.Graph.GraphSON1
{
    internal class GraphSON1ContractResolver : DefaultContractResolver
    {
        /// <summary>
        /// A single instance of a JsonSerializerSettings that uses this ContractResolver.
        /// </summary>
        internal static readonly JsonSerializerSettings Settings = new JsonSerializerSettings
        {
            ContractResolver = new GraphSON1ContractResolver()
        };

        protected GraphSON1ContractResolver()
        {

        }

        protected override JsonContract CreateContract(Type objectType)
        {
            var contract = base.CreateContract(objectType);
            if (GraphSON1Converter.Instance.CanConvert(objectType))
            {
                contract.Converter = GraphSON1Converter.Instance;
            }
            return contract;
        }
    }
}
