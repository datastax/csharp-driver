using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Cassandra.Mapping.Attributes
{
    /// <summary>
    /// Indicates that the property or field represents a column which value is frozen.
    /// Only valid for maps and lists.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
    public class FrozenValueAttribute : Attribute
    {

    }
}
