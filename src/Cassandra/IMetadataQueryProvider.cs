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
// 

using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Cassandra.Responses;
using Cassandra.Serialization;

namespace Cassandra
{
    /// <summary>
    /// Represents an object that can execute metadata queries
    /// </summary>
    internal interface IMetadataQueryProvider
    {
        ProtocolVersion ProtocolVersion { get; }

        /// <summary>
        /// The address of the endpoint used by the ControlConnection
        /// </summary>
        IPEndPoint Address { get; }
        
        /// <summary>
        /// The local address of the socket used by the ControlConnection
        /// </summary>
        IPEndPoint LocalAddress { get; }

        Serializer Serializer { get; }

        Task<IEnumerable<Row>> QueryAsync(string cqlQuery, bool retry = false);

        Task<Response> SendQueryRequestAsync(string cqlQuery, bool retry, QueryProtocolOptions queryProtocolOptions);

        IEnumerable<Row> Query(string cqlQuery, bool retry = false);
    }
}