//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse.Data.Linq
{
    internal enum ParsePhase
    {
        None,

        /// <summary>
        /// Select() method calls.
        /// </summary>
        Select,

        /// <summary>
        /// Where() method calls or LWT conditions.
        /// </summary>
        Condition,

        /// <summary>
        /// Lambda evaluation after Select()
        /// </summary>
        SelectBinding,

        /// <summary>
        /// Take() method calls.
        /// </summary>
        Take,
        
        /// <summary>
        /// OrderBy() method calls.
        /// </summary>
        OrderBy,

        /// <summary>
        /// OrderByDescending() method calls.
        /// </summary>
        OrderByDescending,
        
        /// <summary>
        /// GroupBy() method calls.
        /// </summary>
        GroupBy
    };
}