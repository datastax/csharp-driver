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
using System.Collections.Concurrent;

namespace Cassandra
{
    internal partial class CassandraConnection : IDisposable
    {
        public Action SetupPreparedQueries(AsyncResult<IOutput> jar, List<Tuple<byte[], string>> Ids, int idx, Action dx)
        {
            return new Action(() =>
            {
                if (idx < Ids.Count && !preparedQueries.ContainsKey(Ids[idx].Item1))
                {
                    Evaluate(new PrepareRequest(jar.StreamId, Ids[idx].Item2), jar.StreamId, new Action<ResponseFrame>((frame2) =>
                    {
                        var response = FrameParser.Parse(frame2);
                        if (response is ResultResponse)
                        {
                            preparedQueries.TryAdd(Ids[idx].Item1, Ids[idx].Item2);
                            if (idx == Ids.Count - 1)
                                dx();
                            else
                                BeginJob(jar, SetupPreparedQueries(jar, Ids, idx + 1, dx));
                        }
                        else
                            _protocolErrorHandlerAction(new ErrorActionParam() { AbstractResponse = response, Jar = jar });

                    }));
                }
                else
                    dx();
            });
        }

        private List<Tuple<byte[], string>> GetIdsFromListOfQueries(List<Query> queries)
        {
            var ret = new List<Tuple<byte[], string>>();
            foreach (var q in queries)
            {
                if (q is BoundStatement)
                {
                    var bs = (q as BoundStatement);
                    ret.Add(Tuple.Create(bs.PreparedStatement.Id, bs.PreparedStatement.Cql));
                }
            }
            return ret;
        }

        private List<IBatchableRequest> GetRequestsFromListOfQueries(List<Query> queries)
        {
            var ret = new List<IBatchableRequest>();
            foreach (var q in queries)
            {
                if (q is BoundStatement)
                {
                    var bs = (q as BoundStatement);
                    ret.Add( new ExecuteRequest(-1,bs.PreparedStatement.Id, bs.PreparedStatement.Metadata, bs.Values,ConsistencyLevel.Any, false ));
                }
                else if(q is SimpleStatement)
                {
                    var ss = (q as SimpleStatement);
                    ret.Add(new QueryRequest(-1,ss.QueryString,ConsistencyLevel.Any, false));
                }
            }
            return ret;
        }

        public IAsyncResult BeginBatch(int _streamId, List<Query> queries,
                                              AsyncCallback callback, object state, object owner,
                                              ConsistencyLevel consistency, bool isTracing)
        {
            var jar = SetupJob(_streamId, callback, state, owner, "BATCH");


            BeginJob(jar, SetupKeyspace(jar, SetupPreparedQueries(jar, GetIdsFromListOfQueries(queries), 0, () =>
               {
                   Evaluate(new BatchRequest(jar.StreamId, BatchRequest.BatchType.Logged, GetRequestsFromListOfQueries(queries), consistency, isTracing), jar.StreamId,
                            new Action<ResponseFrame>((frame2) =>
                                {
                                    var response = FrameParser.Parse(frame2);
                                    if (response is ResultResponse)
                                        JobFinished(jar, (response as ResultResponse).Output);
                                    else
                                        _protocolErrorHandlerAction(new ErrorActionParam()
                                            {
                                                AbstractResponse = response,
                                                Jar = jar
                                            });

                                }));
               })));

            return jar;
        }

        public IOutput EndBatch(IAsyncResult result, object owner)
        {
            return AsyncResult<IOutput>.End(result, owner, "BATCH");
        }
    }
}
