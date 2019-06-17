//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;

namespace Dse
{
    public class CqlColumn : ColumnDesc
    {
        /// <summary>
        /// Index of the column in the rowset
        /// </summary>
        public int Index { get; set; }
        /// <summary>
        /// CLR Type of the column
        /// </summary>
        public Type Type { get; set; }
    }
}