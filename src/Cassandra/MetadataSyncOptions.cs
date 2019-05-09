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

namespace Cassandra
{
    /// <summary>
    /// See the api reference for <see cref="Builder.WithMetadataSyncOptions"/> for an explanation of the Metadata synchronization feature.
    /// See the api reference for the properties of this class in order to know what the default values are.
    /// </summary>
    public class MetadataSyncOptions
    {
        /// <summary>
        /// <para>
        /// The default value is <code>true</code>.
        /// </para>
        /// <para>
        /// Enables or disables the Metadata synchronization feature of the driver.
        /// See the api reference for <see cref="Builder.WithMetadataSyncOptions"/> for a more thorough explanation.
        /// </para>
        /// </summary>
        public bool MetadataSyncEnabled { get; set; } = true;

        /// <summary>
        /// <para>
        /// The default value is <code>1000</code>.
        /// </para>
        /// <para>
        /// The driver will wait <see cref="RefreshSchemaDelayIncrement"/> milliseconds until it processes a schema or topology refresh event.
        /// If another event gets scheduled to be processed within this interval, then the driver will cancel the first execution and will wait
        /// another <see cref="RefreshSchemaDelayIncrement"/> milliseconds until it processes both events. As long as events keep coming in,
        /// then the driver will keep postponing the execution up until <see cref="MaxTotalRefreshSchemaDelay"/> milliseconds have passed since the first
        /// unprocessed event was scheduled.
        /// </para>
        /// </summary>
        public long RefreshSchemaDelayIncrement { get; set; } = 1000;

        /// <summary>
        /// <para>
        /// The default value is <code>10000</code>.
        /// </para>
        /// <para>
        /// The driver will never wait more than <see cref="MaxTotalRefreshSchemaDelay"/> milliseconds until a schema or topology refresh event is processed
        /// even if events keep being scheduled for processing.
        /// </para>
        /// </summary>
        public long MaxTotalRefreshSchemaDelay { get; set; } = 10000;

        public MetadataSyncOptions Clone()
        {
            return new MetadataSyncOptions
            {
                MetadataSyncEnabled = MetadataSyncEnabled,
                RefreshSchemaDelayIncrement = RefreshSchemaDelayIncrement,
                MaxTotalRefreshSchemaDelay = MaxTotalRefreshSchemaDelay
            };
        }
    }
}