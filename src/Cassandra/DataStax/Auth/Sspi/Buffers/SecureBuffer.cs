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
    /// Represents a native SecureBuffer structure, which is used for communicating
    /// buffers to the native APIs.
    /// </summary>
    [StructLayout( LayoutKind.Sequential )]
    internal struct SecureBufferInternal
    {
        /// <summary>
        /// When provided to the native API, the total number of bytes available in the buffer.
        /// On return from the native API, the number of bytes that were filled or used by the
        /// native API.
        /// </summary>
        public int Count;

        /// <summary>
        /// The type or purpose of the buffer.
        /// </summary>
        public BufferType Type;

        /// <summary>
        /// An pointer to a pinned byte[] buffer.
        /// </summary>
        public IntPtr Buffer;
    }

    /// <summary>
    /// Stores buffers to provide tokens and data to the native SSPI APIs.
    /// </summary>
    /// <remarks>The buffer is translated into a SecureBufferInternal for the actual call.
    /// To keep the call setup code simple, and to centralize the buffer pinning code,
    /// this class stores and returns buffers as regular byte arrays. The buffer 
    /// pinning support code in SecureBufferAdapter handles conversion to SecureBufferInternal
    /// for pass to the managed api, as well as pinning relevant chunks of memory.
    /// 
    /// Furthermore, the native API may not use the entire buffer, and so a mechanism
    /// is needed to communicate the usage of the buffer separate from the length
    /// of the buffer.</remarks>
    internal class SecureBuffer
    {
        /// <summary>
        /// Initializes a new instance of the SecureBuffer class.
        /// </summary>
        /// <param name="buffer">The buffer to wrap.</param>
        /// <param name="type">The type or purpose of the buffer, for purposes of 
        /// invoking the native API.</param>
        public SecureBuffer( byte[] buffer, BufferType type )
        {
            this.Buffer = buffer;
            this.Type = type;
            this.Length = this.Buffer.Length;
        }

        /// <summary>
        /// The type or purposes of the API, for invoking the native API.
        /// </summary>
        public BufferType Type { get; set; }

        /// <summary>
        /// The buffer to provide to the native API.
        /// </summary>
        public byte[] Buffer { get; set; }

        /// <summary>
        /// The number of elements that were actually filled or used by the native API,
        /// which may be less than the total length of the buffer.
        /// </summary>
        public int Length { get; internal set; }
    }
}
