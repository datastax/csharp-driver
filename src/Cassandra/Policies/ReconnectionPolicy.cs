//
//      Copyright (C) 2012 DataStax Inc.
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
﻿namespace Cassandra
{
    /// <summary>
    ///  Policy that decides how often the reconnection to a dead node is attempted.
    ///  Each time a node is detected dead (because a connection error occurs), a new
    ///  <code>IReconnectionSchedule</code> instance is created (through the
    ///  <link>NewSchedule()</link>). Then each call to the
    ///  <link>IReconnectionSchedule#NextDelayMs</link> method of this instance will
    ///  decide when the next reconnection attempt to this node will be tried. Note
    ///  that if the driver receives a push notification from the Cassandra cluster
    ///  that a node is UP, any existing <code>IReconnectionSchedule</code> on that
    ///  node will be cancelled and a new one will be created (in effect, the driver
    ///  reset the scheduler). The default <link>ExponentialReconnectionPolicy</link>
    ///  policy is usually adequate.
    /// </summary>
    public interface IReconnectionPolicy
    {
        /// <summary>
        ///  Creates a new schedule for reconnection attempts.
        /// </summary>
        IReconnectionSchedule NewSchedule();

    }

    /// <summary>
    ///  Schedules reconnection attempts to a node.
    /// </summary>
    public interface IReconnectionSchedule
    {

        /// <summary>
        ///  When to attempt the next reconnection. This method will be called once when
        ///  the host is detected down to schedule the first reconnection attempt, and
        ///  then once after each failed reconnection attempt to schedule the next one.
        ///  Hence each call to this method are free to return a different value.
        /// </summary>
        /// 
        /// <returns>a time in milliseconds to wait before attempting the next
        ///  reconnection.</returns>
        long NextDelayMs();
    }
}
