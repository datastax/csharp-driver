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
        public IAsyncResult BeginPrepareQuery(string cqlQuery, AsyncCallback callback, object state, object owner)
        {
            return BeginJob(callback, state, owner, "PREPARE", SetupKeyspace((streamId) =>
            {
                Evaluate(new PrepareRequest(streamId, cqlQuery), streamId, new Action<ResponseFrame>((frame2) =>
                {
                    var response = FrameParser.Parse(frame2);
                    if (response is ResultResponse)
                    {
                        var outp = (response as ResultResponse).Output ;
                        if (outp is OutputPrepared)
                            preparedQueries[(outp as OutputPrepared).QueryID] = cqlQuery;
                        JobFinished(streamId, outp);
                    }
                    else
                        _protocolErrorHandlerAction(new ErrorActionParam() { AbstractResponse = response, StreamId = streamId });

                }));
            }));
        }

        public IOutput EndPrepareQuery(IAsyncResult result, object owner)
        {
            return AsyncResult<IOutput>.End(result, owner, "PREPARE");
        }

        public IOutput PrepareQuery(string cqlQuery)
        {
            return EndPrepareQuery(BeginPrepareQuery(cqlQuery, null, null, this), this);
        }
    }
}
