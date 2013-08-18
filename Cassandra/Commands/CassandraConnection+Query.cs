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
        AtomicValue<string> currentKs = new AtomicValue<string>("");
        AtomicValue<string> selectedKs = new AtomicValue<string>("");

        public void SetKeyspace(string ks)
        {
            selectedKs.Value = ks;
        }

        public Action SetupKeyspace(AsyncResult<IOutput> jar, Action dx)
        {
            return new Action(() =>
            {
                if (!currentKs.Value.Equals(selectedKs.Value))
                {
                    Evaluate(new QueryRequest(jar.StreamId, CqlQueryTools.GetUseKeyspaceCQL(selectedKs.Value), ConsistencyLevel.Default, false), jar.StreamId, new Action<ResponseFrame>((frame3) =>
                    {
                        var response = FrameParser.Parse(frame3);
                        if (response is ResultResponse)
                        {
                            currentKs.Value = selectedKs.Value;
                            dx();
                        }
                        else
                            _protocolErrorHandlerAction(new ErrorActionParam() { AbstractResponse = response, Jar = jar });
                    }));
                }
                else
                    dx();
            });
        }

        public IAsyncResult BeginQuery(int _streamId, string cqlQuery, AsyncCallback callback, object state, object owner, ConsistencyLevel consistency, bool tracingEnabled)
        {
            var jar = SetupJob(_streamId, callback, state, owner, "QUERY");
            BeginJob(jar, SetupKeyspace(jar, () =>
           {
               Evaluate(new QueryRequest(jar.StreamId, cqlQuery, consistency, tracingEnabled), jar.StreamId, (frame2) =>
               {
                   var response = FrameParser.Parse(frame2);
                   if (response is ResultResponse)
                       JobFinished(jar, (response as ResultResponse).Output);
                   else
                       _protocolErrorHandlerAction(new ErrorActionParam() { AbstractResponse = response, Jar = jar });

               });
           }));
            return jar;
        }

        public IOutput EndQuery(IAsyncResult result, object owner)
        {
            return AsyncResult<IOutput>.End(result, owner, "QUERY");
        }

        public IOutput Query(int streamId, string cqlQuery, ConsistencyLevel consistency, bool tracingEnabled)
        {
            return EndQuery(BeginQuery(streamId, cqlQuery, null, null, this, consistency, tracingEnabled), this);
        }
    }
}
