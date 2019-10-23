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

using Dse.Observers.Abstractions;
using Dse.Serialization;

namespace Dse.Connections
{
    internal interface IHostConnectionPoolFactory
    {
        IHostConnectionPool Create(Host host, Configuration config, Serializer serializer, IObserverFactory observerFactory);
    }
}