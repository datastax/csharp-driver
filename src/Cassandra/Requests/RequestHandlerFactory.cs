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

using System.Threading.Tasks;

using Cassandra.ExecutionProfiles;
using Cassandra.Serialization;
using Cassandra.SessionManagement;

namespace Cassandra.Requests
{
    internal class RequestHandlerFactory : IRequestHandlerFactory
    {
        public async Task<IRequestHandler> CreateAsync(IInternalSession session, ISerializer serializer, IRequest request, IStatement statement, IRequestOptions options)
        {
            var infoAndObserver = await RequestHandler.CreateRequestObserver(session, statement).ConfigureAwait(false);
            return new RequestHandler(session, serializer, request, infoAndObserver.Item1, options, infoAndObserver.Item2);
        }

        public async Task<IRequestHandler> CreateAsync(
            IInternalSession session, ISerializer serializer, IStatement statement, IRequestOptions options)
        {
            var infoAndObserver = await RequestHandler.CreateRequestObserver(session, statement).ConfigureAwait(false);
            return new RequestHandler(session, serializer, infoAndObserver.Item1, options, infoAndObserver.Item2);
        }

        public async Task<IRequestHandler> CreateAsync(IInternalSession session, ISerializer serializer)
        {
            var infoAndObserver = await RequestHandler.CreateRequestObserver(session, null).ConfigureAwait(false);
            return new RequestHandler(session, serializer, infoAndObserver.Item1, infoAndObserver.Item2);
        }

        public IGraphRequestHandler CreateGraphRequestHandler(IInternalSession session, IGraphTypeSerializerFactory graphTypeSerializerFactory)
        {
            return new GraphRequestHandler(session, graphTypeSerializerFactory);
        }
    }
}