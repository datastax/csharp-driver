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

namespace Cassandra.DataStax.Search
{
    /// <summary>
    /// Defines the possible values of date range precision.
    /// </summary>
    public enum DateRangePrecision : byte
    {
        /// <summary>
        /// Year precision. Any timestamp precision beyond the year portion will be ignored.
        /// </summary>
        Year = 0,
        
        /// <summary>
        /// Year precision. Any timestamp precision beyond the years portion will be ignored.
        /// </summary>
        Month = 1,
        
        /// <summary>
        /// Day precision. Any timestamp precision beyond the days portion will be ignored.
        /// </summary>
        Day = 2,
        
        /// <summary>
        /// Hour precision. Any timestamp precision beyond the hours portion will be ignored.
        /// </summary>
        Hour = 3,
        
        /// <summary>
        /// Minute precision. Any timestamp precision beyond the minutes portion will be ignored.
        /// </summary>
        Minute = 4,
        
        /// <summary>
        /// Second precision. Any timestamp precision beyond the seconds portion will be ignored.
        /// </summary>
        Second = 5,

        /// <summary>
        /// Millisecond precision.
        /// </summary>
        Millisecond = 6
    }
}
