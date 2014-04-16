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
            Connection.BeginPrepareQuery(streamId, CqlQuery, owner.ClbNoQuery, this, owner);
        }
        override public void Process(Session owner, IAsyncResult ar, out object value)
        {
            byte[] id;
            RowSetMetadata metadata;
            RowSetMetadata resultMetadata;
            owner.ProcessPrepareQuery(Connection.EndPrepareQuery(ar, owner), out metadata, out id, out resultMetadata);
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
    }
}
