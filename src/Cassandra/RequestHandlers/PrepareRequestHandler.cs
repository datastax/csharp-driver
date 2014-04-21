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

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra.RequestHandlers
{
    internal class PrepareRequestHandler : RequestHandler
    {
        public string CqlQuery;
        override public void Begin(Session owner, int streamId)
        {
            Connection.BeginPrepareQuery(streamId, CqlQuery, owner.RequestCallback, this, owner);
        }

        override public void Process(Session owner, IAsyncResult ar, out object value)
        {
            byte[] id;
            RowSetMetadata metadata;
            RowSetMetadata resultMetadata;
            ProcessPrepareQuery(Connection.EndPrepareQuery(ar, owner), out metadata, out id, out resultMetadata);
            value = new KeyValuePair<RowSetMetadata, Tuple<byte[], string, RowSetMetadata>>(metadata, Tuple.Create(id, CqlQuery, resultMetadata));
        }

        override public void Complete(Session owner, object value, Exception exc = null)
        {
            var ar = LongActionAc as AsyncResult<KeyValuePair<RowSetMetadata, Tuple<byte[], string, RowSetMetadata>>>;
            if (exc != null)
                ar.Complete(exc);
            else
            {
                var kv = (KeyValuePair<RowSetMetadata, Tuple<byte[], string, RowSetMetadata>>)value;
                ar.SetResult(kv);
                owner.AddPreparedQuery(kv.Value.Item1, kv.Value.Item2);
                ar.Complete();
            }
        }

        internal void ProcessPrepareQuery(IOutput outp, out RowSetMetadata metadata, out byte[] queryId, out RowSetMetadata resultMetadata)
        {
            using (outp)
            {
                if (outp is OutputPrepared)
                {
                    queryId = (outp as OutputPrepared).QueryId;
                    metadata = (outp as OutputPrepared).Metadata;
                    resultMetadata = (outp as OutputPrepared).ResultMetadata;
                    return;
                }
                if (outp is OutputError)
                {
                    var ex = (outp as OutputError).CreateException();
                    throw ex;
                }
                throw new DriverInternalError("Unexpected output kind");
            }
        }
    }
}
