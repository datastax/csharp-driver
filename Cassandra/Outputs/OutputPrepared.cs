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
    internal class OutputPrepared : IOutput, IWaitableForDispose
    {
        public byte[] QueryID;
        public RowSetMetadata Metadata;
        internal OutputPrepared(BEBinaryReader reader)
        {
            var len = reader.ReadInt16();
            QueryID = new byte[len];
            reader.Read(QueryID, 0, len);
            Metadata = new RowSetMetadata(reader);
        }

        public void Dispose()
        {
        }
        public void WaitForDispose()
        {
        }
    }
}
