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

﻿namespace Cassandra
{
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