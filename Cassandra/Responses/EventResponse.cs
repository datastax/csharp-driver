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
﻿namespace Cassandra
{
    internal class EventResponse : AbstractResponse
    {
        private readonly Logger _logger = new Logger(typeof(EventResponse));
        public const byte OpCode = 0x0C;

        public CassandraEventArgs CassandraEventArgs;

        internal EventResponse(ResponseFrame frame)
            : base(frame)
        {
            var eventTypeString = BEBinaryReader.ReadString();
            if (eventTypeString == "TOPOLOGY_CHANGE")
            {
                var ce = new TopologyChangeEventArgs();
                ce.What = BEBinaryReader.ReadString() == "NEW_NODE"
                              ? TopologyChangeEventArgs.Reason.NewNode
                              : TopologyChangeEventArgs.Reason.RemovedNode;
                ce.Address = BEBinaryReader.ReadInet().Address;
                CassandraEventArgs = ce;
                return;
            }
            else if (eventTypeString == "STATUS_CHANGE")
            {
                var ce = new StatusChangeEventArgs();
                ce.What = BEBinaryReader.ReadString() == "UP"
                              ? StatusChangeEventArgs.Reason.Up
                              : StatusChangeEventArgs.Reason.Down;
                ce.Address = BEBinaryReader.ReadInet().Address;
                CassandraEventArgs = ce;
                return;
            }
            else if (eventTypeString == "SCHEMA_CHANGE")
            {
                var ce = new SchemaChangeEventArgs();
                var m = BEBinaryReader.ReadString();
                ce.What = m == "CREATED"
                              ? SchemaChangeEventArgs.Reason.Created
                              : (m == "UPDATED"
                                     ? SchemaChangeEventArgs.Reason.Updated
                                     : SchemaChangeEventArgs.Reason.Dropped);
                ce.Keyspace = BEBinaryReader.ReadString();
                ce.Table = BEBinaryReader.ReadString();
                CassandraEventArgs = ce;
                return;
            }

            var ex = new DriverInternalError("Unknown Event Type");
            _logger.Error(ex);
            throw ex;
        }

        internal static EventResponse Create(ResponseFrame frame)
        {
            return new EventResponse(frame);
        }
    }
}
