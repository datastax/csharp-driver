using System.Collections.Generic;
using System.Reflection;

namespace Cassandra.Mapping.TypeConversion
{
    /// <summary>
    /// Represents the components to build a expression to create a new instance.
    /// </summary>
    internal class NewTypeProjection
    {
        public ConstructorInfo ConstructorInfo { get; private set; }

        public ICollection<MemberInfo> Members { get; private set; }

        public NewTypeProjection(ConstructorInfo constructorInfo)
        {
            ConstructorInfo = constructorInfo;
            Members = new LinkedList<MemberInfo>();
        }
    }
}
