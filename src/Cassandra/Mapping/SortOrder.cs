//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//


namespace Cassandra.Mapping
{
    /// <summary>
    /// Specifies sort order
    /// </summary>
    public enum SortOrder: sbyte
    {
        Unspecified = 0,
        Ascending = 1,
        Descending = -1,
    }
}
