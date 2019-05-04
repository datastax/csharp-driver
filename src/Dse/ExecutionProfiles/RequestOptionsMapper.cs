// 
//       Copyright (C) 2019 DataStax, Inc.
// 
//     Please see the license for details:
//     http://www.datastax.com/terms/datastax-dse-driver-license-terms
// 

using System.Collections.Generic;
using System.Linq;
using Dse.Graph;

namespace Dse.ExecutionProfiles
{
    /// <inheritdoc />
    internal class RequestOptionsMapper : IRequestOptionsMapper
    {
        private readonly GraphOptions _graphOptions = new GraphOptions();
        
        public RequestOptionsMapper()
        {
        }

        public RequestOptionsMapper(GraphOptions graphOptions)
        {
            _graphOptions = graphOptions;
        }

        /// <inheritdoc />
        public IReadOnlyDictionary<string, IRequestOptions> BuildRequestOptionsDictionary(
            IReadOnlyDictionary<string, IExecutionProfile> executionProfiles,
            Policies policies,
            SocketOptions socketOptions,
            ClientOptions clientOptions,
            QueryOptions queryOptions)
        {
            executionProfiles.TryGetValue(Configuration.DefaultExecutionProfileName, out var defaultProfile);
            var requestOptions =
                executionProfiles
                    .Where(kvp => kvp.Key != Configuration.DefaultExecutionProfileName)
                    .ToDictionary<KeyValuePair<string, IExecutionProfile>, string, IRequestOptions>(
                        kvp => kvp.Key,
                        kvp => new RequestOptions(kvp.Value, defaultProfile, policies, socketOptions, queryOptions, clientOptions, _graphOptions));

            requestOptions.Add(
                Configuration.DefaultExecutionProfileName, 
                new RequestOptions(null, defaultProfile, policies, socketOptions, queryOptions, clientOptions, _graphOptions));
            return requestOptions;
        }
    }
}