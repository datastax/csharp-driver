using System;

namespace CqlPoco
{
    /// <summary>
    /// Used on a POCO property of field.  Can be used to override the column name in the database that property or field maps to.
    /// When the <see cref="ExplicitColumnsAttribute"/> is used, this attribute also indicates that a property or field should be
    /// mapped.
    /// </summary>
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field)]
    public class ColumnAttribute : Attribute
    {
        private readonly string _name;

        /// <summary>
        /// The column name in the database this property or field maps to.
        /// </summary>
        public string Name
        {
            get { return _name; }
        }

        /// <summary>
        /// Specifies the name of the column in the database to use for this property/field.  If the <see cref="ExplicitColumnsAttribute"/>
        /// is used on the POCO, also tells the mapper that this column should be included when mapping.
        /// </summary>
        /// <param name="name">The name of the column in the database to map this property or field to.</param>
        public ColumnAttribute(string name)
        {
            _name = name;
        }

        /// <summary>
        /// Used with the <see cref="ExplicitColumnsAttribute"/>, indicates this property should be mapped and that the column name
        /// is the same as the property or field name.
        /// </summary>
        public ColumnAttribute()
        {
        }
    }
}