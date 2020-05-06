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

namespace Cassandra.Responses
{
    internal class EventResponse : Response
    {
        public const byte OpCode = 0x0C;
        private readonly Logger _logger = new Logger(typeof(EventResponse));

        /// <summary>
        /// Information on the actual event
        /// </summary>
        public CassandraEventArgs CassandraEventArgs { get; set; }

        internal EventResponse(Frame frame)
            : base(frame)
        {
            string eventTypeString = Reader.ReadString();
            if (eventTypeString == "TOPOLOGY_CHANGE")
            {
                var ce = new TopologyChangeEventArgs();
                ce.What = Reader.ReadString() == "NEW_NODE"
                              ? TopologyChangeEventArgs.Reason.NewNode
                              : TopologyChangeEventArgs.Reason.RemovedNode;
                ce.Address = Reader.ReadInet();
                CassandraEventArgs = ce;
                return;
            }
            if (eventTypeString == "STATUS_CHANGE")
            {
                var ce = new StatusChangeEventArgs();
                ce.What = Reader.ReadString() == "UP"
                              ? StatusChangeEventArgs.Reason.Up
                              : StatusChangeEventArgs.Reason.Down;
                ce.Address = Reader.ReadInet();
                CassandraEventArgs = ce;
                return;
            }
            if (eventTypeString == "SCHEMA_CHANGE")
            {
                CassandraEventArgs = EventResponse.ParseSchemaChangeBody(frame.Header.Version, Reader);
                return;
            }

            var ex = new DriverInternalError("Unknown Event Type");
            _logger.Error(ex);
            throw ex;
        }

        public static SchemaChangeEventArgs ParseSchemaChangeBody(ProtocolVersion protocolVersion, FrameReader reader)
        {
            var ce = new SchemaChangeEventArgs();
            var changeTypeText = reader.ReadString();
            SchemaChangeEventArgs.Reason changeType;
            switch (changeTypeText)
            {
                case "UPDATED":
                    changeType = SchemaChangeEventArgs.Reason.Updated;
                    break;

                case "DROPPED":
                    changeType = SchemaChangeEventArgs.Reason.Dropped;
                    break;

                default:
                    changeType = SchemaChangeEventArgs.Reason.Created;
                    break;
            }
            ce.What = changeType;
            if (!protocolVersion.SupportsSchemaChangeFullMetadata())
            {
                //protocol v1 and v2: <change_type><keyspace><table>
                ce.Keyspace = reader.ReadString();
                ce.Table = reader.ReadString();
                return ce;
            }
            //protocol v3+: <change_type><target><options>
            var target = reader.ReadString();
            ce.Keyspace = reader.ReadString();
            switch (target)
            {
                case "TABLE":
                    ce.Table = reader.ReadString();
                    break;

                case "TYPE":
                    ce.Type = reader.ReadString();
                    break;

                case "FUNCTION":
                    ce.FunctionName = reader.ReadString();
                    ce.Signature = reader.ReadStringList();
                    break;

                case "AGGREGATE":
                    ce.AggregateName = reader.ReadString();
                    ce.Signature = reader.ReadStringList();
                    break;
            }

            return ce;
        }

        internal static EventResponse Create(Frame frame)
        {
            return new EventResponse(frame);
        }
    }
}