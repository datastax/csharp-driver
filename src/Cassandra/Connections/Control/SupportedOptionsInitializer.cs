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

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cassandra.Requests;
using Cassandra.Responses;

namespace Cassandra.Connections.Control
{
    internal class SupportedOptionsInitializer : ISupportedOptionsInitializer
    {
        private const string SupportedProductTypeKey = "PRODUCT_TYPE";
        private const string SupportedDbaas = "DATASTAX_APOLLO";

        private readonly Metadata _metadata;

        public SupportedOptionsInitializer(Metadata metadata)
        {
            _metadata = metadata;
        }

        public async Task ApplySupportedOptionsAsync(IConnection connection)
        {
            var request = new OptionsRequest();
            var response = await connection.Send(request).ConfigureAwait(false);

            if (response == null)
            {
                throw new NullReferenceException("Response can not be null");
            }

            if (!(response is SupportedResponse supportedResponse))
            {
                throw new DriverInternalError("Expected SupportedResponse, obtained " + response.GetType().FullName);
            }

            ApplyProductTypeOption(supportedResponse.Output.Options);
        }

        private void ApplyProductTypeOption(IDictionary<string, string[]> options)
        {
            if (!options.TryGetValue(SupportedOptionsInitializer.SupportedProductTypeKey, out var productTypeOptions))
            {
                return;
            }

            if (productTypeOptions.Length <= 0)
            {
                return;
            }

            if (string.Compare(productTypeOptions[0], SupportedOptionsInitializer.SupportedDbaas, StringComparison.OrdinalIgnoreCase) == 0)
            {
                _metadata.SetProductTypeAsDbaas();
            }
        }
    }
}