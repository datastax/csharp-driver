//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using Dse.ExecutionProfiles;

namespace Dse.Test.Unit
{
    internal static class ExecutionProfileBuilderExtensions
    {
        public static ExecutionProfileBuilder CastToClass(this IExecutionProfileBuilder builder)
        {
            return (ExecutionProfileBuilder) builder;
        }
    }
}