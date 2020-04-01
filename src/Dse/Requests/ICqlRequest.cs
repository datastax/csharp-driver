//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System.Collections.Generic;

namespace Dse.Requests
{
    /// <summary>
    /// Represents an CQL Request (BATCH, EXECUTE or QUERY)
    /// </summary>
    internal interface ICqlRequest : IRequest
    {
        /// <summary>
        /// Gets or sets the Consistency for the Request.
        /// It defaults to the one provided by the Statement but it can be changed by the retry policy.
        /// </summary>
        ConsistencyLevel Consistency { get; set; }

        /// <summary>
        /// Gets or sets the custom payload to be set with this request
        /// </summary>
        IDictionary<string, byte[]> Payload { get; set; }
    }
}
