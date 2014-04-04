using System;

namespace Cassandra
{
    public class QueryProtocolOptions
    {
        public enum QueryFlags
        {
            Values = 0x01,
            SkipMetadata = 0x02,
            PageSize = 0x04,
            WithPagingState = 0x08,
            WithSerialConsistency = 0x10
        }

        public static QueryProtocolOptions DEFAULT = new QueryProtocolOptions(ConsistencyLevel.One,
                                                                              null,
                                                                              false,
                                                                              QueryOptions.DefaultPageSize,
                                                                              null,
                                                                              ConsistencyLevel.Any);

        public readonly int PageSize;
        public readonly byte[] PagingState;
        public readonly ConsistencyLevel SerialConsistency;
        public readonly bool SkipMetadata;
        public readonly object[] Values;
        public ConsistencyLevel Consistency;
        public QueryFlags Flags;

        internal QueryProtocolOptions(ConsistencyLevel consistency,
                                      object[] values,
                                      bool skipMetadata,
                                      int pageSize,
                                      byte[] pagingState,
                                      ConsistencyLevel serialConsistency)
        {
            Consistency = consistency;
            Values = values;
            SkipMetadata = skipMetadata;
            if (pageSize <= 0)
                PageSize = QueryOptions.DefaultPageSize;
            else if (pageSize == int.MaxValue)
                PageSize = -1;
            else
                PageSize = pageSize;
            PagingState = pagingState;
            SerialConsistency = serialConsistency;
            AddFlags();
        }

        internal static QueryProtocolOptions CreateFromQuery(Query query, ConsistencyLevel defaultCL)
        {
            if (query == null)
                return DEFAULT;
            return new QueryProtocolOptions(query.ConsistencyLevel.HasValue ? query.ConsistencyLevel.Value : defaultCL, query.QueryValues,
                                            query.SkipMetadata, query.PageSize, query.PagingState, query.SerialConsistencyLevel);
        }

        private void AddFlags()
        {
            if (Values != null && Values.Length > 0)
                Flags |= QueryFlags.Values;
            if (SkipMetadata)
                Flags |= QueryFlags.SkipMetadata;
            if (PageSize != int.MaxValue && PageSize >= 0)
                Flags |= QueryFlags.PageSize;
            if (PagingState != null)
                Flags |= QueryFlags.WithPagingState;
            if (SerialConsistency != ConsistencyLevel.Any)
                Flags |= QueryFlags.WithSerialConsistency;
        }

        internal void Write(BEBinaryWriter wb, ConsistencyLevel? extConsistency)
        {
            if ((ushort) (extConsistency ?? Consistency) >= (ushort) ConsistencyLevel.Serial)
                throw new InvalidOperationException("Serial consistency specified as a non-serial one.");

            wb.WriteUInt16((ushort) (extConsistency ?? Consistency));
            wb.WriteByte((byte) Flags);

            if ((Flags & QueryFlags.Values) == QueryFlags.Values)
            {
                wb.WriteUInt16((ushort) Values.Length);
                for (int i = 0; i < Values.Length; i++)
                {
                    byte[] bytes = TypeInterpreter.InvCqlConvert(Values[i]);
                    wb.WriteBytes(bytes);
                }
            }

            if ((Flags & QueryFlags.PageSize) == QueryFlags.PageSize)
                wb.WriteInt32(PageSize);
            if ((Flags & QueryFlags.WithPagingState) == QueryFlags.WithPagingState)
                wb.WriteBytes(PagingState);
            if ((Flags & QueryFlags.WithSerialConsistency) == QueryFlags.WithSerialConsistency)
            {
                if ((ushort) (SerialConsistency) < (ushort) ConsistencyLevel.Serial)
                    throw new InvalidOperationException("Non-serial consistency specified as a serial one.");
                wb.WriteUInt16((ushort) SerialConsistency);
            }
        }
    }
}