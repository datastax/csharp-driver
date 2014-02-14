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
        public IAsyncResult BeginExecuteQueryCredentials(int _streamId, IDictionary<string, string> credentials, AsyncCallback callback, object state, object owner)
        {
            var jar = SetupJob(_streamId, callback, state, owner, "CREDENTIALS");
            BeginJob(jar, new Action(() =>
            {
                Evaluate(new CredentialsRequest(jar.StreamId, credentials), jar.StreamId, new Action<ResponseFrame>((frame2) =>
                {
                    var response = FrameParser.Parse(frame2);
                    if (response is ReadyResponse)
                        JobFinished(jar, new OutputVoid(null));
                    else
                        _protocolErrorHandlerAction(new ErrorActionParam() { AbstractResponse = response, Jar = jar });

                }));
            }));
            return jar;
        }

        public IOutput EndExecuteQueryCredentials(IAsyncResult result, object owner)
        {
            return AsyncResult<IOutput>.End(result, owner, "CREDENTIALS");
        }

        public IOutput ExecuteCredentials(int streamId, IDictionary<string, string> credentials)
        {
            return EndExecuteQueryCredentials(BeginExecuteQueryCredentials(streamId, credentials, null, null, this), this);
        }
    }
}
