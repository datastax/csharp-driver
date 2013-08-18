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
        public IAsyncResult BeginExecuteQueryOptions(int _streamId, AsyncCallback callback, object state, object owner)
        {
            var jar = SetupJob(_streamId, callback, state, owner, "OPTIONS");

            BeginJob(jar, new Action(() =>
            {
                Evaluate(new OptionsRequest(jar.StreamId), jar.StreamId, new Action<ResponseFrame>((frame2) =>
                {
                    var response = FrameParser.Parse(frame2);
                    if (response is SupportedResponse)
                        JobFinished(jar, (response as SupportedResponse).Output);
                    else
                        _protocolErrorHandlerAction(new ErrorActionParam() { AbstractResponse = response, Jar = jar });

                }));
            }), true);

            return jar;
        }

        public IOutput EndExecuteQueryOptions(IAsyncResult result, object owner)
        {
            return AsyncResult<IOutput>.End(result, owner, "OPTIONS");
        }

        public IOutput ExecuteOptions(int streamId)
        {
            return EndExecuteQueryOptions(BeginExecuteQueryOptions(streamId, null, null, this), this);
        }
    }
}
