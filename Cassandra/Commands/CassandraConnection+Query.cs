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

namespace Cassandra
{
    internal partial class CassandraConnection : IDisposable
    {
        string currentKs;
        string selectedKs;

        public void SetKeyspace(string ks)
        {
            selectedKs = ks;
        }

        public Action<int> SetupKeyspace(Action<int> dx)
        {
            return new Action<int>((streamId) =>
            {
                if (currentKs != selectedKs)
                {
                    Evaluate(new QueryRequest(streamId, CqlQueryTools.GetUseKeyspaceCQL(selectedKs), ConsistencyLevel.Default, false), streamId, new Action<ResponseFrame>((frame3) =>
                    {
                        var response = FrameParser.Parse(frame3);
                        if (response is ResultResponse)
                        {
                            currentKs = selectedKs;
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

        public IAsyncResult BeginQuery(string cqlQuery, AsyncCallback callback, object state, object owner, ConsistencyLevel consistency, bool tracingEnabled)
        {
            return BeginJob(callback, state, owner, "QUERY", SetupKeyspace((streamId) =>
            {
                Evaluate(new QueryRequest(streamId, cqlQuery, consistency, tracingEnabled), streamId, (frame2) =>
                {
                    var response = FrameParser.Parse(frame2);
                    if (response is ResultResponse)
                        JobFinished(streamId, (response as ResultResponse).Output);
                    else
                        _protocolErrorHandlerAction(new ErrorActionParam() { AbstractResponse = response, StreamId = streamId });

                });
            }));
        }

        public IOutput EndQuery(IAsyncResult result, object owner)
        {
            return AsyncResult<IOutput>.End(result, owner, "QUERY");
        }

        public IOutput Query(string cqlQuery, ConsistencyLevel consistency, bool tracingEnabled)
        {
            return EndQuery(BeginQuery(cqlQuery, null, null, this, consistency,tracingEnabled), this);
        }
    }
}
