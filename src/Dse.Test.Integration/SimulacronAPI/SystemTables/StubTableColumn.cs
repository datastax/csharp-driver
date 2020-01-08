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

namespace Dse.Test.Integration.SimulacronAPI.SystemTables
{
    public class StubTableColumn
    {
        public StubTableColumn(string name, StubColumnKind kind, DataType type, StubClusteringOrder order = null)
        {
            Type = type;
            Name = name;
            Kind = kind;
            ClusteringOrder = order ?? StubClusteringOrder.None;
        }

        public DataType Type { get; set; }
        
        public string Name { get; set; }
        
        public StubColumnKind Kind { get; set; }

        public StubClusteringOrder ClusteringOrder { get; set; }
    }
    
    public class StubUdtField
    {
        public StubUdtField(string name, DataType type)
        {
            Type = type;
            Name = name;
        }

        public DataType Type { get; set; }
        
        public string Name { get; set; }
    }

    public class StubClusteringOrder
    {
        private StubClusteringOrder(string value)
        {
            Value = value;
        }

        public string Value { get; }

        public override string ToString()
        {
            return Value;
        }

        public static readonly StubClusteringOrder Asc = new StubClusteringOrder("asc");

        public static readonly StubClusteringOrder Desc = new StubClusteringOrder("desc");

        public static readonly StubClusteringOrder None = new StubClusteringOrder("none");
    }

    public class StubColumnKind
    {
        private StubColumnKind(string value)
        {
            Value = value;
        }

        public string Value { get; }

        public override string ToString()
        {
            return Value;
        }

        public static readonly StubColumnKind PartitionKey = new StubColumnKind("partition_key");

        public static readonly StubColumnKind ClusteringKey = new StubColumnKind("clustering");

        public static readonly StubColumnKind Regular = new StubColumnKind("regular");
    }
}