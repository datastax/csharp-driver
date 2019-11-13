//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using Cassandra.ExecutionProfiles;

namespace Cassandra.Tests
{
    internal static class ExecutionProfileBuilderExtensions
    {
        public static ExecutionProfileBuilder CastToClass(this IExecutionProfileBuilder builder)
        {
            return (ExecutionProfileBuilder) builder;
        }
    }
}