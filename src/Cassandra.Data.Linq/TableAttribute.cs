using System;

namespace Cassandra.Data.Linq
{
    [AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, Inherited = false, AllowMultiple = false)]
    public sealed class TableAttribute : Attribute
    {
        public TableAttribute() {}
        public TableAttribute(string Name) { this.Name = Name; }
        public string Name = null;
    }
}