//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse.Test.Integration.TestClusterManagement
{
    public class LocalCcmProcessExecuter : CcmProcessExecuter
    {
        public const string CcmCommandPath = "/usr/local/bin/ccm";
        public static readonly LocalCcmProcessExecuter Instance = new LocalCcmProcessExecuter();

        private LocalCcmProcessExecuter()
        {
            
        }

        protected override string GetExecutable(ref string args)
        {
            var executable = CcmCommandPath;

            if (!TestUtils.IsWin)
            {
                return executable;
            }
            executable = "cmd.exe";
            args = "/c ccm " + args;
            return executable;
        }
    }
}
