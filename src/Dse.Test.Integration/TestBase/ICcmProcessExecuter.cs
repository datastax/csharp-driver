
namespace Dse.Test.Integration.TestBase
{
    public interface ICcmProcessExecuter
    {
        ProcessOutput ExecuteCcm(string args, int timeout = 90 * 1000, bool throwOnProcessError = true);
    }
}
