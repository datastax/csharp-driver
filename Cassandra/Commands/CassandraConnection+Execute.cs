//
//      Copyright (C) 2012 DataStax Inc.
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
﻿using System;
using System.Collections.Generic;

namespace Cassandra
{
    internal partial class CassandraConnection : IDisposable
    {
        Dictionary<byte[], string> preparedQueries = new Dictionary<byte[], string>();

        public Action<int> SetupPreparedQuery(byte[] Id, string cql, Action<int> dx)
        {
            return new Action<int>((streamId) =>
            {
                if (!preparedQueries.ContainsKey(Id))
                {
                    Evaluate(new PrepareRequest(streamId, cql), streamId, new Action<ResponseFrame>((frame2) =>
                    {
                        var response = FrameParser.Parse(frame2);
                        if (response is ResultResponse)
                        {
                            preparedQueries[Id] = cql;
                            dx(streamId);
                        }
                        else
                            _protocolErrorHandlerAction(new ErrorActionParam() { AbstractResponse = response, StreamId = streamId });

                    }));
                }
                else
                    dx(streamId);
            });
        }

        public IAsyncResult BeginExecuteQuery(int _streamId, byte[] Id, string cql, RowSetMetadata Metadata, object[] values,
                                              AsyncCallback callback, object state, object owner,
                                              ConsistencyLevel consistency, bool isTracing)
        {
            var jar = SetupJob(_streamId, callback, state, owner, "EXECUTE");

            BeginJob(jar, SetupKeyspace(SetupPreparedQuery(Id, cql, (streamId) =>
               {
                   Evaluate(new ExecuteRequest(streamId, Id, Metadata, values, consistency, isTracing), streamId,
                            new Action<ResponseFrame>((frame2) =>
                                {
                                    var response = FrameParser.Parse(frame2);
                                    if (response is ResultResponse)
                                        JobFinished(streamId, (response as ResultResponse).Output);
                                    else
                                        _protocolErrorHandlerAction(new ErrorActionParam()
                                            {
                                                AbstractResponse = response,
                                                StreamId = streamId
                                            });

                                }));
               })));

            return jar;
        }

        public IOutput EndExecuteQuery(IAsyncResult result, object owner)
        {
            return AsyncResult<IOutput>.End(result, owner, "EXECUTE");
        }

        public IOutput ExecuteQuery(int streamId, byte[] Id, string cql, RowSetMetadata Metadata, object[] values, ConsistencyLevel consistency,
                                    bool isTracing)
        {
            return EndExecuteQuery(BeginExecuteQuery(streamId, Id, cql, Metadata, values, null, null, this, consistency, isTracing),
                                   this);
        }
    }
}
