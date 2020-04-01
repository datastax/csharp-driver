//
//      Copyright (C) DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//
using System;

namespace Dse.Test.Integration.TestClusterManagement
{
    public class WslCcmProcessExecuter : CcmProcessExecuter
    {
        public static readonly WslCcmProcessExecuter Instance = new WslCcmProcessExecuter();

        private WslCcmProcessExecuter()
        {
        }

        protected override string GetExecutable(ref string args)
        {
            if (!TestUtils.IsWin)
            {
                throw new InvalidOperationException("Can only use WSL CCM process executor in windows.");
            }
            var executable = "wsl.exe";
            args = $"bash --login -c 'nohup ccm {args} > ~/ccmnohup.out 2>&1'";
            return executable;
        }

        public override int GetDefaultTimeout()
        {
            return 20 * 60 * 1000;
        }
    }
}