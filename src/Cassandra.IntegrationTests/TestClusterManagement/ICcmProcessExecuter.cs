//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Cassandra.IntegrationTests.TestClusterManagement
{
    public interface ICcmProcessExecuter
    {
        ProcessOutput ExecuteCcm(string args, int timeout = 90 * 1000, bool throwOnProcessError = true);
    }
}
