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

namespace Cassandra.DataStax.Auth.Sspi.Buffers
{
    /// <summary>
    /// Represents the native layout of the secure buffer descriptor that is provided directly
    /// to native API calls.
    /// </summary>
    [StructLayout( LayoutKind.Sequential)]
    internal struct SecureBufferDescInternal
    {
        /// <summary>
        /// The buffer structure version.
        /// </summary>
        public int Version;

        /// <summary>
        /// The number of buffers represented by this descriptor.
        /// </summary>
        public int NumBuffers;

        /// <summary>
        /// A pointer to a array of buffers, where each buffer is a byte[].
        /// </summary>
        public IntPtr Buffers;

        /// <summary>
        /// Indicates the buffer structure version supported by this structure. Always 0.
        /// </summary>
        public const int ApiVersion = 0;
    }
}
