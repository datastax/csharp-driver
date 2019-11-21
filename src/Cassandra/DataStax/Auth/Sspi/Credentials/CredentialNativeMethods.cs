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
using System.Runtime.ConstrainedExecution;
using System.Runtime.InteropServices;

namespace Cassandra.DataStax.Auth.Sspi.Credentials
{
    internal static class CredentialNativeMethods
    {
        [ReliabilityContract( Consistency.WillNotCorruptState, Cer.MayFail)]
        [DllImport( "Secur32.dll", EntryPoint = "AcquireCredentialsHandle", CharSet = CharSet.Unicode )]
        internal static extern SecurityStatus AcquireCredentialsHandle(
            string principleName,
            string packageName,
            CredentialUse credentialUse,
            IntPtr loginId,
            IntPtr packageData,
            IntPtr getKeyFunc,
            IntPtr getKeyData,
            ref RawSspiHandle credentialHandle,
            ref TimeStamp expiry
        );

        [ReliabilityContract( Consistency.WillNotCorruptState, Cer.Success )]
        [DllImport( "Secur32.dll", EntryPoint = "FreeCredentialsHandle", CharSet = CharSet.Unicode )]
        internal static extern SecurityStatus FreeCredentialsHandle(
            ref RawSspiHandle credentialHandle
        );


        /// <summary>
        /// The overload of the QueryCredentialsAttribute method that is used for querying the name attribute.
        /// In this call, it takes a void* to a structure that contains a wide char pointer. The wide character
        /// pointer is allocated by the SSPI api, and thus needs to be released by a call to FreeContextBuffer().
        /// </summary>
        /// <param name="credentialHandle"></param>
        /// <param name="attributeName"></param>
        /// <param name="name"></param>
        /// <returns></returns>
        [ReliabilityContract( Consistency.WillNotCorruptState, Cer.Success )]
        [DllImport( "Secur32.dll", EntryPoint = "QueryCredentialsAttributes", CharSet = CharSet.Unicode )]
        internal static extern SecurityStatus QueryCredentialsAttribute_Name(
            ref RawSspiHandle credentialHandle,
            CredentialQueryAttrib attributeName,
            ref QueryNameAttribCarrier name
        );
    }
}
