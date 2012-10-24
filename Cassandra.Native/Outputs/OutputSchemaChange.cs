using System;
using System.Collections.Generic;
using System.Text;

namespace Cassandra.Native
{
    internal class OutputSchemaChange : IOutput, IWaitableForDispose
    {
        public string change;
        public string keyspace;
        public string table;

        internal OutputSchemaChange(BEBinaryReader reader)
        {
            this.change = reader.ReadString();
            this.keyspace= reader.ReadString();
            this.table = reader.ReadString();
        }

        public void Dispose()
        {
        }
        public void WaitForDispose()
        {
        }
    }
}
