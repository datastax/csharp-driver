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

namespace Cassandra.DataStax.Auth.Sspi.Contexts
{
    /// <summary>
    /// Defines options for creating a security context via win32 InitializeSecurityContext 
    /// (used by clients) and AcceptSecurityContext (used by servers).
    /// Required attribute flags are specified when creating the context. InitializeSecurityContext
    /// and AcceptSecurityContext returns a value indicating what final attributes the created context 
    /// actually has.
    /// </summary>
    [Flags]
    internal enum ContextAttrib : int 
    {
        /// <summary>
        /// No additional attributes are provided.
        /// </summary>
        Zero = 0,
        
        /// <summary>
        /// The server can use the context to authenticate to other servers as the client. The
        /// MutualAuth flag must be set for this flag to work. Valid for Kerberos. Ignore this flag for 
        /// constrained delegation.
        /// </summary>
        Delegate = 0x00000001,

        /// <summary>
        /// The mutual authentication policy of the service will be satisfied.
        /// *Caution* - This does not necessarily mean that mutual authentication is performed, only that
        /// the authentication policy of the service is satisfied. To ensure that mutual authentication is
        /// performed, query the context attributes after it is created.
        /// </summary>
        MutualAuth = 0x00000002,


        /// <summary>
        /// Detect replayed messages that have been encoded by using the EncryptMessage or MakeSignature 
        /// message support functionality.
        /// </summary>
        ReplayDetect = 0x00000004,

        /// <summary>
        /// Detect messages received out of sequence when using the message support functionality. 
        /// This flag implies all of the conditions specified by the Integrity flag - out-of-order sequence 
        /// detection can only be trusted if the integrity of any underlying sequence detection mechanism 
        /// in transmitted data can be trusted.
        /// </summary>
        SequenceDetect = 0x00000008,

        // The context must protect data while in transit.
        // Confidentiality is supported for NTLM with Microsoft
        // Windows NT version 4.0, SP4 and later and with the
        // Kerberos protocol in Microsoft Windows 2000 and later.
        
        /// <summary>
        /// The context must protect data while in transit. Encrypt messages by using the EncryptMessage function.
        /// </summary>
        Confidentiality = 0x00000010,
        
        /// <summary>
        /// A new session key must be negotiated.
        /// This value is supported only by the Kerberos security package.
        /// </summary>
        UseSessionKey = 0x00000020,

        /// <summary>
        /// The security package allocates output buffers for you. Buffers allocated by the security package have 
        /// to be released by the context memory management functions.
        /// </summary>
        AllocateMemory = 0x00000100,

        /// <summary>
        /// The security context will not handle formatting messages. This value is the default for the Kerberos, 
        /// Negotiate, and NTLM security packages.
        /// </summary>
        Connection = 0x00000800,

        /// <summary>
        /// When errors occur, the remote party will be notified.
        /// </summary>
        /// <remarks>
        /// A client specifies InitExtendedError in InitializeSecurityContext
        /// and the server specifies AcceptExtendedError in AcceptSecurityContext. 
        /// </remarks>
        InitExtendedError = 0x00004000,

        /// <summary>
        /// When errors occur, the remote party will be notified.
        /// </summary>
        /// <remarks>
        /// A client specifies InitExtendedError in InitializeSecurityContext
        /// and the server specifies AcceptExtendedError in AcceptSecurityContext. 
        /// </remarks>
        AcceptExtendedError = 0x00008000,

        /// <summary>
        /// Support a stream-oriented connection. Provided by clients.
        /// </summary>
        InitStream = 0x00008000,

        /// <summary>
        /// Support a stream-oriented connection. Provided by servers.
        /// </summary>
        AcceptStream = 0x00010000,

        /// <summary>
        /// Sign messages and verify signatures by using the EncryptMessage and MakeSignature functions.
        /// Replayed and out-of-sequence messages will not be detected with the setting of this attribute.
        /// Set ReplayDetect and SequenceDetect also if these behaviors are desired.
        /// </summary>
        InitIntegrity = 0x00010000, 

        /// <summary>
        /// Sign messages and verify signatures by using the EncryptMessage and MakeSignature functions.
        /// Replayed and out-of-sequence messages will not be detected with the setting of this attribute.
        /// Set ReplayDetect and SequenceDetect also if these behaviors are desired.
        /// </summary>
        AcceptIntegrity = 0x00020000,

        InitIdentify = 0x00020000,
        AcceptIdentify = 0x00080000,

        /// <summary>
        /// An Schannel provider connection is instructed to not authenticate the server automatically.
        /// </summary>
        InitManualCredValidation = 0x00080000,

        /// <summary>
        /// An Schannel provider connection is instructed to not authenticate the client automatically.
        /// </summary>
        InitUseSuppliedCreds = 0x00000080,
    }
}
