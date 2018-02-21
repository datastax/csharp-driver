// 
//       Copyright DataStax Inc.
// 
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
// 
//       http://www.apache.org/licenses/LICENSE-2.0
// 
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
// 

using System.Numerics;

namespace Cassandra.Tests.Mapping.Pocos
{
    public class PocoWithNumericTypes
    {
        public int IntValue { get; set; }

        public short ShortValue { get; set; }
        
        public long LongValue { get; set; }

        public sbyte SbyteValue { get; set; }

        public BigInteger BigIntegerValue { get; set; }

        public decimal DecimalValue { get; set; }

        public double DoubleValue { get; set; }

        public float FloatValue { get; set; }
    }
}