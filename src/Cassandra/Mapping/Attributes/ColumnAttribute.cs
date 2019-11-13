//
//      Copyright (C) DataStax Inc.
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

namespace Cassandra.Mapping.Attributes
{
    /// <summary>
    /// Used on a POCO property of field.  Can be used to override the column name in the database that property or field maps to.
    /// When the <see cref="TableAttribute.ExplicitColumns"/> is set, this attribute also indicates that a property or field should be
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
        /// Used to override the Type of the column in the database for INSERTs/UPDATEs.  The data in the property/field this attribute is 
        /// applied to will be converted to this Type for INSERTs/UPDATEs.  If null, the same Type of the property/field will be used
        /// instead.  (NOTE: This does NOT affect the Type when fetching/SELECTing data from the database.)
        /// </summary>
        public Type Type { get; set; }

        /// <summary>
        /// Specifies the name of the column in the database to use for this property/field.  If the <see cref="TableAttribute.ExplicitColumns"/>
        /// is used on the POCO, also tells the mapper that this column should be included when mapping.
        /// </summary>
        /// <param name="name">The name of the column in the database to map this property or field to.</param>
        public ColumnAttribute(string name)
        {
            _name = name;
        }

        /// <summary>
        /// Used with the <see cref="TableAttribute.ExplicitColumns"/>, indicates this property should be mapped and that the column name
        /// is the same as the property or field name.
        /// </summary>
        public ColumnAttribute()
        {
        }
    }
}