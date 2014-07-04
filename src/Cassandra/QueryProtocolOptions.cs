using System;

namespace Cassandra
{
    public class QueryProtocolOptions
    {
        [Flags]
        public enum QueryFlags : byte
        {
            Values = 0x01,
            SkipMetadata = 0x02,
            PageSize = 0x04,
            WithPagingState = 0x08,
            WithSerialConsistency = 0x10,
            WithDefaultTimestamp = 0x20
        }

        public static readonly QueryProtocolOptions Default = 
            new QueryProtocolOptions(ConsistencyLevel.One, null, false, QueryOptions.DefaultPageSize, null, ConsistencyLevel.Any);

        public readonly int PageSize;
        public readonly byte[] PagingState;
        public readonly ConsistencyLevel SerialConsistency;
        public readonly bool SkipMetadata;
        public readonly object[] Values;
        public ConsistencyLevel Consistency { get; set; }
        public QueryFlags Flags { get; set; }
        public DateTimeOffset? Timestamp { get; set; }

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
            {
                PageSize = QueryOptions.DefaultPageSize;
            }
            else if (pageSize == int.MaxValue)
            {
                PageSize = -1;
            }
            else
            {
                PageSize = pageSize;
            }
            PagingState = pagingState;
            SerialConsistency = serialConsistency;
            AddFlags();
        }

        internal static QueryProtocolOptions CreateFromQuery(Statement query, ConsistencyLevel defaultConsistencyLevel)
        {
            if (query == null)
            {
                return Default;
            }
            var options = new QueryProtocolOptions(
                query.ConsistencyLevel.HasValue ? query.ConsistencyLevel.Value : defaultConsistencyLevel,
                query.QueryValues,
                query.SkipMetadata, query.PageSize, query.PagingState, query.SerialConsistencyLevel)
            {
                Timestamp = query.Timestamp
            };
            return options;
        }

        private void AddFlags()
        {
            if (Values != null && Values.Length > 0)
            {
                Flags |= QueryFlags.Values;
            }
            if (SkipMetadata)
            {
                Flags |= QueryFlags.SkipMetadata;
            }
            if (PageSize != int.MaxValue && PageSize >= 0)
            {
                Flags |= QueryFlags.PageSize;
            }
            if (PagingState != null)
            {
                Flags |= QueryFlags.WithPagingState;
            }
            if (SerialConsistency != ConsistencyLevel.Any)
            {
                Flags |= QueryFlags.WithSerialConsistency;
            }
            if (Timestamp != null)
            {
                Flags |= QueryFlags.WithDefaultTimestamp;
            }
        }

        //TODO: Move to ExecuteRequest and QueryRequest
        internal void Write(BEBinaryWriter wb, byte protocolVersion)
        {
            //protocol v1: <query><n><value_1>....<value_n><consistency>
            //protocol v2: <query><consistency><flags>[<n><value_1>...<value_n>][<result_page_size>][<paging_state>][<serial_consistency>]
            //protocol v3: <query><consistency><flags>[<n>[name_1]<value_1>...[name_n]<value_n>][<result_page_size>][<paging_state>][<serial_consistency>][<timestamp>]

            if (protocolVersion > 1)
            {
                wb.WriteUInt16((ushort)Consistency);
                wb.WriteByte((byte)Flags);
            }

            if ((Flags & QueryFlags.Values) == QueryFlags.Values)
            {
                wb.WriteUInt16((ushort)Values.Length);
                for (var i = 0; i < Values.Length; i++)
                {
                    var bytes = TypeCodec.Encode(protocolVersion, Values[i]);
                    wb.WriteBytes(bytes);
                }
            }

            if (protocolVersion == 1)
            {
                wb.WriteUInt16((ushort)Consistency);
            }
            else
            {
                if ((Flags & QueryFlags.PageSize) == QueryFlags.PageSize)
                {
                    wb.WriteInt32(PageSize);
                }
                if ((Flags & QueryFlags.WithPagingState) == QueryFlags.WithPagingState)
                {
                    wb.WriteBytes(PagingState);
                }
                if ((Flags & QueryFlags.WithSerialConsistency) == QueryFlags.WithSerialConsistency)
                {
                    wb.WriteUInt16((ushort)SerialConsistency);
                }
                if (Timestamp != null)
                {
                    //Expressed in microseconds
                    wb.WriteLong(TypeCodec.ToUnixTime(Timestamp.Value).Ticks / 10);
                }
            }
        }
    }
}