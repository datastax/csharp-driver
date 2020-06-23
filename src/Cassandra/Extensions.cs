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

using System.Threading.Tasks;
using Cassandra.SessionManagement;
using Cassandra.Tasks;

namespace Cassandra
{
    /// <summary>
    /// Provides C# extension methods for interfaces and classes within the root namespace.
    /// <remarks>
    /// Used to introduce new methods on interfaces without making it a breaking change for the users.
    /// </remarks>
    /// </summary>
    public static class Extensions
    {
        /// <summary>
        /// <para>
        /// This method will block if no connection has been opened yet. Please use <see cref="GetStateAsync"/> if
        /// your application uses the Task Parallel Library (e.g. async/await).
        /// </para>
        /// <para>
        /// Gets a snapshot containing information on the connections pools held by this Client at the current time.
        /// </para>
        /// <para>
        /// The information provided in the returned object only represents the state at the moment this method was
        /// called and it's not maintained in sync with the driver metadata.
        /// </para>
        /// </summary>
        /// <param name="instance"></param>
        /// <returns></returns>
        public static ISessionState GetState(this ISession instance)
        {
            return TaskHelper.WaitToComplete(
                instance.GetStateAsync(), 
                instance.Cluster.Configuration.DefaultRequestOptions.QueryAbortTimeout);
        }

        /// <summary>
        /// Gets a snapshot containing information on the connections pools held by this Client at the current time.
        /// <para>
        /// The information provided in the returned object only represents the state at the moment this method was
        /// called and it's not maintained in sync with the driver metadata.
        /// </para>
        /// </summary>
        /// <param name="instance"></param>
        /// <returns></returns>
        public static async Task<ISessionState> GetStateAsync(this ISession instance)
        {
            var session = instance as IInternalSession;
            var metadata = await instance.Cluster.GetMetadataAsync().ConfigureAwait(false);
            return session == null ? SessionState.Empty() : SessionState.From(session, metadata);
        }

        internal static ISessionState GetState(this IInternalSession instance, Metadata metadata)
        {
            return SessionState.From(instance, metadata);
        }
    }
}