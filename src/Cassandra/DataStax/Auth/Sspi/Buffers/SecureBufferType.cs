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

namespace Cassandra.DataStax.Auth.Sspi.Buffers
{
    /// <summary>
    /// Describes the type and purpose of a secure buffer passed to the native API.
    /// </summary>
    internal enum BufferType : int
    {
        /// <summary>
        /// The buffer is empty.
        /// </summary>
        Empty = 0x00,

        /// <summary>
        /// The buffer contains message data. Message data can be plaintext or cipher text data.
        /// </summary>
        Data = 0x01,

        /// <summary>
        /// The buffer contains opaque authentication token data.
        /// </summary>
        Token = 0x02,

        /// <summary>
        /// The buffer contains parameters specific to the security package.
        /// </summary>
        Parameters = 0x03,

        /// <summary>
        /// The buffer placeholder indicating that some data is missing.
        /// </summary>
        Missing = 0x04,

        /// <summary>
        /// The buffer passed to an API call contained more data than was necessary for completing the action,
        /// such as the case when a streaming-mode connection that does not preserve message bounders, such as TCP 
        /// is used as the transport. The extra data is returned back to the caller in a buffer of this type.
        /// </summary>
        Extra = 0x05,

        /// <summary>
        /// The buffer contains a security data trailer, such as a message signature or marker, or framing data.
        /// </summary>
        Trailer = 0x06,

        /// <summary>
        /// The buffer contains a security data header, such as a message signature, marker, or framing data.
        /// </summary>
        Header = 0x07,

        Padding = 0x09,
        Stream = 0x0A,
        ChannelBindings = 0x0E,
        TargetHost = 0x10,
        ReadOnlyFlag = unchecked( (int)0x80000000 ),
        ReadOnlyWithChecksum = 0x10000000
    }
}
