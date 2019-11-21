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

namespace Cassandra.DataStax.Auth.Sspi
{
    /// <summary>
    /// Reads and writes value types to byte arrays with explicit endianness.
    /// </summary>
    internal static class ByteWriter
    {
        // Big endian: Most significant byte at lowest address in memory.
        
        /// <summary>
        /// Writes a 2-byte signed integer to the buffer in big-endian format.
        /// </summary>
        /// <param name="value">The value to write to the buffer.</param>
        /// <param name="buffer">The buffer to write to.</param>
        /// <param name="position">The index of the first byte to write to.</param>
        public static void WriteInt16_BE( Int16 value, byte[] buffer, int position )
        {
            buffer[position + 0] = (byte)( value >> 8 );
            buffer[position + 1] = (byte)( value );
        }

        /// <summary>
        /// Writes a 4-byte signed integer to the buffer in big-endian format.
        /// </summary>
        /// <param name="value">The value to write to the buffer.</param>
        /// <param name="buffer">The buffer to write to.</param>
        /// <param name="position">The index of the first byte to write to.</param>
        public static void WriteInt32_BE( Int32 value, byte[] buffer, int position )
        {
            buffer[position + 0] = (byte)( value >> 24 );
            buffer[position + 1] = (byte)( value >> 16 );
            buffer[position + 2] = (byte)( value >> 8 );
            buffer[position + 3] = (byte)( value);

        }

        /// <summary>
        /// Reads a 2-byte signed integer that is stored in the buffer in big-endian format.
        /// The returned value is in the native endianness.
        /// </summary>
        /// <param name="buffer">The buffer to read.</param>
        /// <param name="position">The index of the first byte to read.</param>
        /// <returns></returns>
        public static Int16 ReadInt16_BE( byte[] buffer, int position )
        {
            Int16 value;

            value = (Int16)( buffer[position + 0] << 8 );
            value += (Int16)( buffer[position + 1] );

            return value;
        }

        /// <summary>
        /// Reads a 4-byte signed integer that is stored in the buffer in big-endian format.
        /// The returned value is in the native endianness.
        /// </summary>
        /// <param name="buffer">The buffer to read.</param>
        /// <param name="position">The index of the first byte to read.</param>
        /// <returns></returns>
        public static Int32 ReadInt32_BE( byte[] buffer, int position )
        {
            Int32 value;

            value = (Int32)( buffer[position + 0] << 24 );
            value |= (Int32)( buffer[position + 1] << 16 );
            value |= (Int32)( buffer[position + 2] << 8 );
            value |= (Int32)( buffer[position + 3] );

            return value;
        }
    }
}
