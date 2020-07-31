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
using System.Threading.Tasks;

using Cassandra.ExecutionProfiles;
using Cassandra.Requests;
using Cassandra.Serialization;
using Cassandra.SessionManagement;

using Moq;

namespace Cassandra.Tests.Requests
{
    internal class FakeRequestHandlerFactory : IRequestHandlerFactory
    {
        private readonly Action<IStatement> _executeCallback;
        private readonly Func<IStatement, RowSet> _rs;

        public FakeRequestHandlerFactory(Action<IStatement> executeCallback, Func<IStatement, RowSet> rs = null)
        {
            _executeCallback = executeCallback;
            _rs = rs ?? (stmt => new RowSet());
        }

        public IRequestHandler Create(IInternalSession session, ISerializer serializer, IRequest request, IStatement statement, IRequestOptions options)
        {
            return CreateMockHandler(statement);
        }

        public IRequestHandler Create(IInternalSession session, ISerializer serializer, IStatement statement, IRequestOptions options)
        {
            return CreateMockHandler(statement);
        }

        public IRequestHandler Create(IInternalSession session, ISerializer serializer)
        {
            return CreateMockHandler();
        }

        public IGraphRequestHandler CreateGraphRequestHandler(IInternalSession session, IGraphTypeSerializerFactory resolver)
        {
            return new GraphRequestHandler(session, resolver);
        }

        private IRequestHandler CreateMockHandler(IStatement statement = null)
        {
            var handler = Mock.Of<IRequestHandler>();
            var mock = Mock.Get(handler)
                .Setup(h => h.SendAsync())
                .Returns(FakeRequestHandlerFactory.TaskOf(_rs(statement)));

            if (statement != null)
            {
                mock.Callback(() => _executeCallback(statement)).Verifiable();
            }
            else
            {
                mock.Verifiable();
            }

            return handler;
        }

        private static Task<T> TaskOf<T>(T value)
        {
            var tcs = new TaskCompletionSource<T>();
            tcs.SetResult(value);
            return tcs.Task;
        }
    }
}