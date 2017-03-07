//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

﻿namespace Dse
{
    /// <summary>
    /// The type of batch to use
    /// </summary>
    public enum BatchType
    {
        /// <summary>
        /// A logged batch: Cassandra will first write the batch to its distributed batch log to ensure the atomicity of the batch.
        /// </summary>
        Logged = 0,
        /// <summary>
        /// An unlogged batch: The batch will not be written to the batch log and atomicity of the batch is NOT guaranteed.
        /// </summary>
        Unlogged = 1,
        /// <summary>
        /// A counter batch
        /// </summary>
        Counter = 2
    }
}
