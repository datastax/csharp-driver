//
//  Copyright (C) 2016 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse.Test.Integration.TestClusterManagement
{
    public interface ICcmProcessExecuter
    {
        ProcessOutput ExecuteCcm(string args, int timeout = 90 * 1000, bool throwOnProcessError = true);
    }
}
