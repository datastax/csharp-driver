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

using System;

namespace Cassandra.Observers.Abstractions
{
    /// <summary>
    /// Observer with handlers for connection events, used for metrics for example.
    /// </summary>
    internal interface IConnectionObserver
    {
        void OnBytesSent(long size);

        void OnBytesReceived(long size);

        void OnErrorOnOpen(Exception exception);

        /// <summary>
        /// Creates an operation observer for an operation associated with the connection that is being observed by
        /// this instance.
        /// </summary>
        IOperationObserver CreateOperationObserver();
    }
}