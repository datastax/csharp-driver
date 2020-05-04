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

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cassandra.Responses;
using Cassandra.Serialization;

namespace Cassandra.Connections.Control
{
    /// <summary>
    /// Handles metadata queries (usually sent by the control connection).
    /// </summary>
    internal interface IMetadataRequestHandler
    {
        Task<Response> SendMetadataRequestAsync(
            IConnection connection, ISerializer serializer, string cqlQuery, QueryProtocolOptions queryProtocolOptions);

        Task<Response> UnsafeSendQueryRequestAsync(
            IConnection connection, ISerializer serializer, string cqlQuery, QueryProtocolOptions queryProtocolOptions);

        /// <summary>
        /// Validates that the result contains a RowSet and returns it.
        /// </summary>
        /// <exception cref="NullReferenceException" />
        /// <exception cref="DriverInternalError" />
        IEnumerable<IRow> GetRowSet(Response response);
    }
}