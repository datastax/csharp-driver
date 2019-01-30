// 
//       Copyright DataStax, Inc.
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

using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Cassandra.Tests.MetadataHelpers.TestHelpers
{
    internal class FakeSchemaParser : SchemaParser
    {
        private readonly ConcurrentDictionary<string, KeyspaceMetadata> _keyspaces;

        public FakeSchemaParser(ConcurrentDictionary<string, KeyspaceMetadata> keyspaces) : base(new Metadata(null))
        {
            _keyspaces = keyspaces;
        }

        protected override string SelectAggregates => throw new System.NotImplementedException();

        protected override string SelectFunctions => throw new System.NotImplementedException();

        protected override string SelectTables => throw new System.NotImplementedException();

        protected override string SelectUdts => throw new System.NotImplementedException();

        public override Task<AggregateMetadata> GetAggregate(string keyspaceName, string aggregateName, string signatureString)
        {
            throw new System.NotImplementedException();
        }

        public override Task<FunctionMetadata> GetFunction(string keyspaceName, string functionName, string signatureString)
        {
            throw new System.NotImplementedException();
        }

        public override Task<KeyspaceMetadata> GetKeyspace(string name)
        {
            return Task.FromResult(_keyspaces[name]);
        }

        public override Task<IEnumerable<KeyspaceMetadata>> GetKeyspaces(bool retry)
        {
            return Task.FromResult(_keyspaces.ToArray().Select(kvp => kvp.Value));
        }

        public override Task<TableMetadata> GetTable(string keyspaceName, string tableName)
        {
            throw new System.NotImplementedException();
        }

        public override Task<UdtColumnInfo> GetUdtDefinition(string keyspaceName, string typeName)
        {
            throw new System.NotImplementedException();
        }

        public override Task<MaterializedViewMetadata> GetView(string keyspaceName, string viewName)
        {
            throw new System.NotImplementedException();
        }
    }
}