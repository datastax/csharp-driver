//
//      Copyright (C) 2012-2014 DataStax Inc.
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

namespace Cassandra
{
    internal class EventResponse : AbstractResponse
    {
        public const byte OpCode = 0x0C;
        private readonly Logger _logger = new Logger(typeof (EventResponse));
        /// <summary>
        /// Information on the actual event
        /// </summary>
        public CassandraEventArgs CassandraEventArgs { get; set; }

        internal EventResponse(ResponseFrame frame)
            : base(frame)
        {
            string eventTypeString = BeBinaryReader.ReadString();
            if (eventTypeString == "TOPOLOGY_CHANGE")
            {
                var ce = new TopologyChangeEventArgs();
                ce.What = BeBinaryReader.ReadString() == "NEW_NODE"
                              ? TopologyChangeEventArgs.Reason.NewNode
                              : TopologyChangeEventArgs.Reason.RemovedNode;
                ce.Address = BeBinaryReader.ReadInet();
                CassandraEventArgs = ce;
                return;
            }
            if (eventTypeString == "STATUS_CHANGE")
            {
                var ce = new StatusChangeEventArgs();
                ce.What = BeBinaryReader.ReadString() == "UP"
                              ? StatusChangeEventArgs.Reason.Up
                              : StatusChangeEventArgs.Reason.Down;
                ce.Address = BeBinaryReader.ReadInet();
                CassandraEventArgs = ce;
                return;
            }
            if (eventTypeString == "SCHEMA_CHANGE")
            {
                HandleSchemaChange(frame);
                return;
            }

            var ex = new DriverInternalError("Unknown Event Type");
            _logger.Error(ex);
            throw ex;
        }

        public void HandleSchemaChange(ResponseFrame frame)
        {
            var ce = new SchemaChangeEventArgs();
            CassandraEventArgs = ce;
            var changeTypeText = BeBinaryReader.ReadString();
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
            if (frame.Header.Version < 3)
            {
                //protocol v1 and v2: <change_type><keyspace><table>
                ce.Keyspace = BeBinaryReader.ReadString();
                ce.Table = BeBinaryReader.ReadString();
                return;
            }
            //protocol v3+: <change_type><target><options>
            var target = BeBinaryReader.ReadString();
            ce.Keyspace = BeBinaryReader.ReadString();
            switch (target)
            {
                case "TABLE":
                    ce.Table = BeBinaryReader.ReadString();
                    break;
                case "TYPE":
                    ce.Type = BeBinaryReader.ReadString();
                    break;
                case "FUNCTION":
                    ce.FunctionName = BeBinaryReader.ReadString();
                    ce.Signature = BeBinaryReader.ReadStringList();
                    break;
                case "AGGREGATE":
                    ce.AggregateName = BeBinaryReader.ReadString();
                    ce.Signature = BeBinaryReader.ReadStringList();
                    break;
            }
        }

        internal static EventResponse Create(ResponseFrame frame)
        {
            return new EventResponse(frame);
        }
    }
}
