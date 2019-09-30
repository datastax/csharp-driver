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

using Cassandra.Metrics.Abstractions;

namespace Cassandra.Metrics.Registries
{
    internal interface IRequestErrorMetrics
    {
        IDriverMeter Aborted { get; }

        IDriverMeter ReadTimeout { get; }

        IDriverMeter WriteTimeout { get; }

        IDriverMeter Unavailable { get; }

        IDriverMeter ClientTimeout { get; }

        IDriverMeter Other { get; }

        IDriverMeter Unsent { get; }

        IDriverMeter Total { get; }

        IDriverCounter ConnectionInitErrors { get; }

        IDriverCounter AuthenticationErrors { get; }

        IEnumerable<IDriverMeter> Meters { get; }
        
        IEnumerable<IDriverCounter> Counters { get; }
    }
}