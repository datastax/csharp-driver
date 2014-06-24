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
                ce.Address = BeBinaryReader.ReadInet().Address;
                CassandraEventArgs = ce;
                return;
            }
            if (eventTypeString == "STATUS_CHANGE")
            {
                var ce = new StatusChangeEventArgs();
                ce.What = BeBinaryReader.ReadString() == "UP"
                              ? StatusChangeEventArgs.Reason.Up
                              : StatusChangeEventArgs.Reason.Down;
                ce.Address = BeBinaryReader.ReadInet().Address;
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
            string m = BeBinaryReader.ReadString();
            ce.What = m == "CREATED"
                          ? SchemaChangeEventArgs.Reason.Created
                          : (m == "UPDATED"
                                 ? SchemaChangeEventArgs.Reason.Updated
                                 : SchemaChangeEventArgs.Reason.Dropped);
            if (frame.Header.Version < 3)
            {
                //protocol v1 and v2: <change_type><keyspace><table>
                ce.Keyspace = BeBinaryReader.ReadString();
                ce.Table = BeBinaryReader.ReadString();
            }
            else
            {
                //protocol v3: <change_type><target><options>
                var target = BeBinaryReader.ReadString();
                switch (target)
                {
                    case "KEYSPACE":
                        ce.Keyspace = BeBinaryReader.ReadString();
                        break;
                    case "TABLE":
                        ce.Keyspace = BeBinaryReader.ReadString();
                        ce.Table = BeBinaryReader.ReadString();
                        break;
                    case "TYPE":
                        ce.Keyspace = BeBinaryReader.ReadString();
                        ce.Type = BeBinaryReader.ReadString();
                        break;
                }
            }
            CassandraEventArgs = ce;
        }

        internal static EventResponse Create(ResponseFrame frame)
        {
            return new EventResponse(frame);
        }
    }
}