//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;

namespace Dse.Mapping.Attributes
{
    /// <summary>
    /// Indicates that the property or field is part of the Partition Key
    /// </summary>
    [AttributeUsage(AttributeTargets.Property | AttributeTargets.Field, Inherited = true)]
    public class PartitionKeyAttribute : Attribute
    {
        /// <summary>
        /// The index of the key, relative to the other partition keys.
        /// </summary>
        public int Index { get; set; }
        /// <summary>
        /// Specify the primary key column names (in order) for the table.
        /// </summary>
        /// <param name="index">The index of the key, relative to the other partition keys.</param>
        public PartitionKeyAttribute(int index = 0)
        {
            Index = index;
        }
    }
}
