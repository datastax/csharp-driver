namespace Dse.Test.Integration.TestBase
{
    public class LocalCcmProcessExecuter : CcmProcessExecuter
    {
        public const string CcmCommandPath = "/usr/local/bin/ccm";

        protected override string GetExecutable(ref string args)
        {
            var executable = CcmCommandPath;

            if (!TestUtils.IsWin) return executable;

            executable = "cmd.exe";
            args = "/c ccm " + args;
            return executable;
        }
    }
}
