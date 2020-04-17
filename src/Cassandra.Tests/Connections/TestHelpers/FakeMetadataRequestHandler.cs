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

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cassandra.Connections;
using Cassandra.Connections.Control;
using Cassandra.Responses;
using Cassandra.Tests.DataStax.Insights;

namespace Cassandra.Tests.Connections.TestHelpers
{
    internal class FakeMetadataRequestHandler : IMetadataRequestHandler
    {
        private readonly IDictionary<string, IEnumerable<IRow>> _rows;
        private readonly ConcurrentDictionary<Response, string> _responsesByCql = new ConcurrentDictionary<Response, string>();

        internal ConcurrentQueue<MetadataRequest> Requests { get; } =
            new ConcurrentQueue<MetadataRequest>();
        
        public FakeMetadataRequestHandler(IDictionary<string, IEnumerable<IRow>> rows)
        {
            _rows = rows;
        }

        private async Task<Response> Send(IConnection connection, ProtocolVersion version, string cqlQuery, QueryProtocolOptions queryProtocolOptions)
        {
            Requests.Enqueue(new MetadataRequest { Version = version, CqlQuery = cqlQuery, QueryProtocolOptions = queryProtocolOptions });
            await Task.Yield();
            ThrowErrorIfNullRows(cqlQuery);
            var response = new FakeResultResponse(ResultResponse.ResultResponseKind.Rows);
            _responsesByCql.AddOrUpdate(response, _ => cqlQuery, (_,__) => cqlQuery);
            return (Response) response;
        }

        private void ThrowErrorIfNullRows(string cqlQuery)
        {
            if (_rows.ContainsKey(cqlQuery) && _rows[cqlQuery] == null)
            {
                throw new InvalidQueryException("error");
            }
        }

        public Task<Response> SendMetadataRequestAsync(IConnection connection, ProtocolVersion version, string cqlQuery, QueryProtocolOptions queryProtocolOptions)
        {
            return Send(connection, version, cqlQuery, queryProtocolOptions);
        }

        public Task<Response> UnsafeSendQueryRequestAsync(IConnection connection, ProtocolVersion version, string cqlQuery, QueryProtocolOptions queryProtocolOptions)
        {
            return Send(connection, version, cqlQuery, queryProtocolOptions);
        }

        public IEnumerable<IRow> GetRowSet(Response response)
        {
            return _rows[_responsesByCql[response]];
        }

        internal struct MetadataRequest
        {
            public ProtocolVersion Version { get; set; }

            public string CqlQuery { get; set; }

            public QueryProtocolOptions QueryProtocolOptions { get; set; }
        }
    }
}