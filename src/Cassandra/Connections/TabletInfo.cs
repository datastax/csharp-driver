using System.Collections.Generic;

namespace Cassandra.Connections
{
    public class TabletInfo
    {
        private const string SCYLLA_TABLETS_STARTUP_OPTION_KEY = "TABLETS_ROUTING_V1";
        private const string SCYLLA_TABLETS_STARTUP_OPTION_VALUE = "";
        public const string TABLETS_ROUTING_V1_CUSTOM_PAYLOAD_KEY = "tablets-routing-v1";

        private bool enabled;

        private TabletInfo(bool enabled)
        {
            this.enabled = enabled;
        }

        // Currently pertains only to TABLETS_ROUTING_V1
        public bool IsEnabled()
        {
            return enabled;
        }

        public static TabletInfo ParseTabletInfo(IDictionary<string, string[]> supported)
        {
            if (supported.TryGetValue(SCYLLA_TABLETS_STARTUP_OPTION_KEY, out var values))
            {
                return new TabletInfo(
                    values != null &&
                    values.Length == 1 &&
                    values[0] == SCYLLA_TABLETS_STARTUP_OPTION_VALUE
                );
            }

            return new TabletInfo(false);
        }
    }
}
