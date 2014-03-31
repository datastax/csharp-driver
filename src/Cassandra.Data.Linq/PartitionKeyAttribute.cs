using System;

namespace Cassandra.Data.Linq
{
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, Inherited = true, AllowMultiple = true)]
    public class PartitionKeyAttribute : Attribute
    {
        public int Index = -1;

        public PartitionKeyAttribute(int index = 0)
        {
            Index = index;
        }
    }
}