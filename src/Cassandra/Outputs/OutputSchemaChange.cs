﻿//
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

using System;

namespace Cassandra
{
    internal class OutputSchemaChange : IOutput, IWaitableForDispose
    {
        private readonly Guid? _traceID;
        public string Change;
        public string Keyspace;
        public string Table;

        public Guid? TraceID
        {
            get { return _traceID; }
        }

        internal OutputSchemaChange(BEBinaryReader reader, Guid? traceID)
        {
            _traceID = traceID;
            Change = reader.ReadString();
            Keyspace = reader.ReadString();
            Table = reader.ReadString();
        }

        public void Dispose()
        {
        }

        public void WaitForDispose()
        {
        }
    }
}