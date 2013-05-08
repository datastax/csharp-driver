using System;
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

        public IAsyncResult BeginExecuteQuery(byte[] Id, string cql, RowSetMetadata Metadata, object[] values,
                                              AsyncCallback callback, object state, object owner,
                                              ConsistencyLevel consistency, bool isTracing)
        {
            return BeginJob(callback, state, owner, "EXECUTE", SetupKeyspace(SetupPreparedQuery(Id, cql, (streamId) =>
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
        }

        public IOutput EndExecuteQuery(IAsyncResult result, object owner)
        {
            return AsyncResult<IOutput>.End(result, owner, "EXECUTE");
        }

        public IOutput ExecuteQuery(byte[] Id, string cql, RowSetMetadata Metadata, object[] values, ConsistencyLevel consistency,
                                    bool isTracing)
        {
            return EndExecuteQuery(BeginExecuteQuery(Id, cql, Metadata, values, null, null, this, consistency, isTracing),
                                   this);
        }
    }
}
