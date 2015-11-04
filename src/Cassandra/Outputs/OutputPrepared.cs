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

// ReSharper disable once CheckNamespace
namespace Cassandra
{
    internal class OutputPrepared : IOutput
    {
        public RowSetMetadata Metadata { get; private set; }
        public byte[] QueryId { get; private set; }
        public System.Guid? TraceId { get; internal set; }

        internal OutputPrepared(byte protocolVersion, FrameReader reader)
        {
            var length = reader.ReadInt16();
            QueryId = new byte[length];
            reader.Read(QueryId, 0, length);
            Metadata = new RowSetMetadata(reader, protocolVersion >= 4);
        }

        public void Dispose()
        {
        }
    }
}
