// 
//       Copyright (C) 2019 DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

using System.Collections.Generic;

namespace Dse.ExecutionProfiles
{
    /// <summary>
    /// Component that builds <see cref="IRequestOptions"/> instances from the provided <see cref="IExecutionProfile"/> instances.
    /// </summary>
    internal interface IRequestOptionsMapper
    {
        /// <summary>
        /// Converts execution profile instances to RequestOptions which handle the fallback logic
        /// therefore guaranteeing that the settings are non null.
        /// </summary>
        IReadOnlyDictionary<string, IRequestOptions> BuildRequestOptionsDictionary(
            IReadOnlyDictionary<string, IExecutionProfile> executionProfiles,
            Policies policies,
            SocketOptions socketOptions,
            ClientOptions clientOptions,
            QueryOptions queryOptions);
    }
}