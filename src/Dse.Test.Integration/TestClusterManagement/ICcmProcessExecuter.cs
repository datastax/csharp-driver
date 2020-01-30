//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse.Test.Integration.TestClusterManagement
{
    public interface ICcmProcessExecuter
    {
        ProcessOutput ExecuteCcm(string args, bool throwOnProcessError = true);

        int GetDefaultTimeout();
    }
}
