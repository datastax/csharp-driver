using System;

namespace CqlPoco
{
    /// <summary>
    /// Used on a POCO to tell the mapper to only map properties/fields on the POCO that have a <see cref="ColumnAttribute"/>.
    /// </summary>
    [AttributeUsage(AttributeTargets.Class)]
    public class ExplicitColumnsAttribute : Attribute
    {
    }
}