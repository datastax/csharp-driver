//
//      Copyright (C) DataStax Inc.
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
using Cassandra.ExecutionProfiles;
using Cassandra.Serialization;

namespace Cassandra
{
    public class QueryProtocolOptions
    {
        //This class was leaked to the API, making it internal would be a breaking change
        [Flags]
        public enum QueryFlags
        {
            Values = 0x01,
            SkipMetadata = 0x02,
            PageSize = 0x04,
            WithPagingState = 0x08,
            WithSerialConsistency = 0x10,
            WithDefaultTimestamp = 0x20,
            WithNameForValues = 0x40,
            WithKeyspace = 0x80
        }

        public static readonly QueryProtocolOptions Default = 
            new QueryProtocolOptions(ConsistencyLevel.One, null, false, QueryOptions.DefaultPageSize, null, ConsistencyLevel.Any);

        public readonly int PageSize;
        public readonly ConsistencyLevel SerialConsistency;
        
        private readonly string _keyspace;

        public bool SkipMetadata { get; }

        public byte[] PagingState { get; set; }

        public object[] Values { get; private set; }

        public ConsistencyLevel Consistency { get; set; }

        public DateTimeOffset? Timestamp
        {
            get
            {
                return RawTimestamp == null ? (DateTimeOffset?) null :
                    TypeSerializer.UnixStart.AddTicks(RawTimestamp.Value * 10);
            }
        }

        internal long? RawTimestamp { get; }

        /// <summary>
        /// Names of the query parameters
        /// </summary>
        public IList<string> ValueNames { get; set; }

        internal QueryProtocolOptions(ConsistencyLevel consistency,
                                      object[] values,
                                      bool skipMetadata,
                                      int pageSize,
                                      byte[] pagingState,
                                      ConsistencyLevel serialConsistency,
                                      long? timestamp = null,
                                      string keyspace = null)
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
            RawTimestamp = timestamp;
            _keyspace = keyspace;
        }

        internal static QueryProtocolOptions CreateFromQuery(
            ProtocolVersion protocolVersion, Statement query, IRequestOptions requestOptions, bool? forceSkipMetadata)
        {
            if (query == null)
            {
                return Default;
            }
            var consistency = query.ConsistencyLevel ?? requestOptions.ConsistencyLevel;
            var pageSize = query.PageSize != 0 ? query.PageSize : requestOptions.PageSize;
            long? timestamp = null;
            if (query.Timestamp != null)
            {
                timestamp = TypeSerializer.SinceUnixEpoch(query.Timestamp.Value).Ticks / 10;
            }
            else if (protocolVersion.SupportsTimestamp())
            {
                timestamp = requestOptions.TimestampGenerator.Next();
                if (timestamp == long.MinValue)
                {
                    timestamp = null;
                }
            }

            return new QueryProtocolOptions(
                consistency,
                query.QueryValues,
                forceSkipMetadata ?? query.SkipMetadata,
                pageSize,
                query.PagingState,
                requestOptions.GetSerialConsistencyLevelOrDefault(query),
                timestamp,
                query.Keyspace);
        }

        /// <summary>
        /// Returns a new instance with the minimum amount of values, valid to generate a batch request item.
        /// </summary>
        internal static QueryProtocolOptions CreateForBatchItem(Statement statement)
        {
            return new QueryProtocolOptions(
                ConsistencyLevel.One, statement.QueryValues, false, 0, null, ConsistencyLevel.Serial);
        }

        private QueryFlags GetFlags(ProtocolVersion protocolVersion, bool isPrepared)
        {
            QueryFlags flags = 0;
            if (Values != null && Values.Length > 0)
            {
                flags |= QueryFlags.Values;
            }
            if (SkipMetadata)
            {
                flags |= QueryFlags.SkipMetadata;
            }
            if (PageSize != int.MaxValue && PageSize >= 0)
            {
                flags |= QueryFlags.PageSize;
            }
            if (PagingState != null)
            {
                flags |= QueryFlags.WithPagingState;
            }
            if (SerialConsistency != ConsistencyLevel.Any)
            {
                flags |= QueryFlags.WithSerialConsistency;
            }
            if (protocolVersion.SupportsTimestamp() && RawTimestamp != null)
            {
                flags |= QueryFlags.WithDefaultTimestamp;
            }
            if (protocolVersion.SupportsNamedValuesInQueries() && ValueNames != null && ValueNames.Count > 0)
            {
                flags |= QueryFlags.WithNameForValues;
            }

            if (!isPrepared && protocolVersion.SupportsKeyspaceInRequest() && _keyspace != null)
            {
                // Providing keyspace is only useful for QUERY requests.
                // For EXECUTE requests, the keyspace will be the one from the prepared statement.
                flags |= QueryFlags.WithKeyspace;
            }

            return flags;
        }

        internal void Write(FrameWriter wb, bool isPrepared)
        {
            //protocol v1: <query><n><value_1>....<value_n><consistency>
            //protocol v2: <query><consistency><flags>[<n><value_1>...<value_n>][<result_page_size>][<paging_state>][<serial_consistency>]
            //protocol v3: <query><consistency><flags>[<n>[name_1]<value_1>...[name_n]<value_n>][<result_page_size>][<paging_state>][<serial_consistency>][<timestamp>]
            var protocolVersion = wb.Serializer.ProtocolVersion;
            var flags = GetFlags(protocolVersion, isPrepared);

            if (protocolVersion != ProtocolVersion.V1)
            {
                wb.WriteUInt16((ushort)Consistency);
                if (protocolVersion.Uses4BytesQueryFlags())
                {
                    wb.WriteInt32((int) flags);
                }
                else
                {
                    wb.WriteByte((byte) flags);
                }
            }

            if (flags.HasFlag(QueryFlags.Values))
            {
                wb.WriteUInt16((ushort)Values.Length);
                for (var i = 0; i < Values.Length; i++)
                {
                    if (flags.HasFlag(QueryFlags.WithNameForValues))
                    {
                        var name = ValueNames[i];
                        wb.WriteString(name);
                    }
                    wb.WriteAsBytes(Values[i]);
                }
            }
            else if (protocolVersion == ProtocolVersion.V1 && isPrepared)
            {
                //n values is not optional on protocol v1
                //Write 0 values
                wb.WriteUInt16(0);
            }

            if (protocolVersion == ProtocolVersion.V1)
            {
                //Protocol v1 ends here
                wb.WriteUInt16((ushort)Consistency);
                return;
            }
            if ((flags & QueryFlags.PageSize) == QueryFlags.PageSize)
            {
                wb.WriteInt32(PageSize);
            }
            if ((flags & QueryFlags.WithPagingState) == QueryFlags.WithPagingState)
            {
                wb.WriteBytes(PagingState);
            }
            if ((flags & QueryFlags.WithSerialConsistency) == QueryFlags.WithSerialConsistency)
            {
                wb.WriteUInt16((ushort)SerialConsistency);
            }
            if (flags.HasFlag(QueryFlags.WithDefaultTimestamp))
            {
                // ReSharper disable once PossibleInvalidOperationException
                // Null check has been done when setting the flag
                wb.WriteLong(RawTimestamp.Value);
            }

            if (flags.HasFlag(QueryFlags.WithKeyspace))
            {
                wb.WriteString(_keyspace);
            }
        }
    }
}