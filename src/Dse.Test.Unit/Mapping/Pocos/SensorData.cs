using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Dse.Test.Unit.Mapping.Pocos
{
    public class SensorData
    {
        public string Id { get; set; }

        public string Bucket { get; set; }

        public TimeUuid Timestamp { get; set; }

        public double Value { get; set; }
    }
}
