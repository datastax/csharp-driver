// 
//       Copyright (C) DataStax Inc.
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
//       Copyright (C) DataStax Inc.
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

using System.Collections.Generic;
using System.Threading.Tasks;
using Cassandra.Tasks;

namespace Cassandra
{
    internal interface ISchemaParser
    {
        /// <summary>
        /// Gets the keyspace metadata
        /// </summary>
        /// <returns>The keyspace metadata or null if not found</returns>
        Task<KeyspaceMetadata> GetKeyspaceAsync(string name);

        /// <summary>
        /// Gets all the keyspaces metadata
        /// </summary>
        Task<IEnumerable<KeyspaceMetadata>> GetKeyspacesAsync(bool retry);

        Task<TableMetadata> GetTableAsync(string keyspaceName, string tableName);

        Task<MaterializedViewMetadata> GetViewAsync(string keyspaceName, string viewName);

        Task<ICollection<string>> GetTableNamesAsync(string keyspaceName);

        Task<ICollection<string>> GetKeyspacesNamesAsync();

        Task<FunctionMetadata> GetFunctionAsync(string keyspaceName, string functionName, string signatureString);

        Task<AggregateMetadata> GetAggregateAsync(string keyspaceName, string aggregateName, string signatureString);

        Task<UdtColumnInfo> GetUdtDefinitionAsync(string keyspaceName, string typeName);

        string ComputeFunctionSignatureString(string[] signature);

        Task<QueryTrace> GetQueryTraceAsync(QueryTrace trace, HashedWheelTimer timer);
    }
}