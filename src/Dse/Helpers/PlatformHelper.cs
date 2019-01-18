using System.Runtime.InteropServices;

namespace Dse.Helpers
{
    internal static class PlatformHelper
    {
        public static bool IsKerberosSupported()
        {
#if NET45
            return true;
#elif NETCORE
            return false;
#else
            return RuntimeInformation.IsOSPlatform(OSPlatform.Windows);
#endif
        }
    }
}