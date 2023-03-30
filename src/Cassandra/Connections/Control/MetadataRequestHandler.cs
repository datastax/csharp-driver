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
using System.Net.Sockets;
using System.Threading.Tasks;
using Cassandra.Requests;
using Cassandra.Responses;
using Cassandra.Serialization;

namespace Cassandra.Connections.Control
{
    /// <inheritdoc />
    internal class MetadataRequestHandler : IMetadataRequestHandler
    {
        public async Task<Response> SendMetadataRequestAsync(
            IConnection connection, ISerializer serializer, string cqlQuery, QueryProtocolOptions queryProtocolOptions)
        {
            var request = new QueryRequest(serializer, cqlQuery, queryProtocolOptions, false, null);
            Response response;
            try
            {
                response = await connection.Send(request).ConfigureAwait(false);
            }
            catch (SocketException ex)
            {
                ControlConnection.Logger.Error(
                    $"There was an error while executing on the host {cqlQuery} the query '{connection.EndPoint.EndpointFriendlyName}'", ex);
                throw;
            }
            return response;
        }

        public Task<Response> UnsafeSendQueryRequestAsync(
            IConnection connection, ISerializer serializer, string cqlQuery, QueryProtocolOptions queryProtocolOptions)
        {
            return connection.Send(new QueryRequest(serializer, cqlQuery, queryProtocolOptions, false, null));
        }

        /// <inheritdoc />
        public IEnumerable<IRow> GetRowSet(Response response)
        {
            if (response == null)
            {
                throw new NullReferenceException("Response can not be null");
            }
            if (!(response is ResultResponse))
            {
                throw new DriverInternalError("Expected rows, obtained " + response.GetType().FullName);
            }
            var result = (ResultResponse)response;
            if (!(result.Output is OutputRows))
            {
                throw new DriverInternalError("Expected rows output, obtained " + result.Output.GetType().FullName);
            }
            return ((OutputRows)result.Output).RowSet;
        }
    }
}