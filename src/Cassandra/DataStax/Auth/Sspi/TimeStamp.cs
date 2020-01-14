//
//  Copyright (c) 2014, Kevin Thompson
//  All rights reserved.
//  
//  Redistribution and use in source and binary forms, with or without
//  modification, are permitted provided that the following conditions are met:
//  
//  1. Redistributions of source code must retain the above copyright notice, this
//     list of conditions and the following disclaimer. 
//  2. Redistributions in binary form must reproduce the above copyright notice,
//     this list of conditions and the following disclaimer in the documentation
//     and/or other materials provided with the distribution.
//  
//  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
//  ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
//  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
//  DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
//  ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
//  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
//  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
//  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
//  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
//  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
//

using System;
using System.Runtime.InteropServices;

namespace Cassandra.DataStax.Auth.Sspi
{
    /// <summary>
    /// Represents a Windows API Timestamp structure, which stores time in units of 100 nanosecond 
    /// ticks, counting from January 1st, year 1601 at 00:00 UTC. Time is stored as a 64-bit value.
    /// </summary>
    [StructLayout( LayoutKind.Sequential )]
    internal struct TimeStamp
    {
        public static readonly DateTime Epoch = new DateTime( 1601, 1, 1, 0, 0, 0, DateTimeKind.Utc );

        /// <summary>
        /// Stores the time value. Infinite times are often represented as values near, but not exactly
        /// at the maximum signed 64-bit 2's complement value.
        /// </summary>
        private long time;

        /// <summary>
        /// Converts the TimeStamp to an equivalant DateTime object. If the TimeStamp represents
        /// a value larger than DateTime.MaxValue, then DateTime.MaxValue is returned.
        /// </summary>
        /// <returns></returns>
        public DateTime ToDateTime()
        {
            ulong test = (ulong)this.time + (ulong)(TimeStamp.Epoch.Ticks);

            // Sometimes the value returned is massive, eg, 0x7fffff154e84ffff, which is a value 
            // somewhere in the year 30848. This would overflow DateTime, since it peaks at 31-Dec-9999.
            // It turns out that this value corresponds to a TimeStamp's maximum value, reduced by my local timezone
            // http://stackoverflow.com/questions/24478056/
            if ( test > (ulong)DateTime.MaxValue.Ticks )
            {
                return DateTime.MaxValue;
            }
            else
            {
                return DateTime.FromFileTimeUtc( this.time );
            }
        }
    }
}
