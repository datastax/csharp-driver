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

using Cassandra.ExecutionProfiles;
using Cassandra.Serialization;
using Cassandra.SessionManagement;

namespace Cassandra.Requests
{
    internal interface IRequestHandlerFactory
    {
        IRequestHandler Create(IInternalSession session, ISerializer serializer, IRequest request, IStatement statement, IRequestOptions options);

        IRequestHandler Create(IInternalSession session, ISerializer serializer, IStatement statement, IRequestOptions options);

        IRequestHandler Create(IInternalSession session, ISerializer serializer);
        
        IGraphRequestHandler CreateGraphRequestHandler(IInternalSession session, IGraphProtocolResolver graphProtocolResolver);
    }
}