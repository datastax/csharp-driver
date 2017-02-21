using System;

namespace Cassandra.Mapping.Attributes
{
    /// <summary>
    /// Indicates that the property or field is Frozen.
    /// Only valid for collections, tuples, and user-defined types.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
    public class FrozenAttribute : Attribute
    {

    }
}
