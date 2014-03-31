using System;

namespace Cassandra
{
    public class SchemaChangedEventArgs : EventArgs
    {
        public enum Kind { Created, Dropped, Updated }
        public Kind What;
        public string Keyspace;
        public string Table;
    }
}