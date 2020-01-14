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

namespace Cassandra.DataStax.Auth.Sspi.Contexts
{
    /// <summary>
    /// Defines the types of queries that can be performed with QueryContextAttribute.
    /// Each query has a different result buffer.
    /// </summary>
    internal enum ContextQueryAttrib : int
    {
        /// <summary>
        /// Queries the buffer size parameters when performing message functions, such
        /// as encryption, decryption, signing and signature validation.
        /// </summary>
        /// <remarks>
        /// Results for a query of this type are stored in a Win32 SecPkgContext_Sizes structure.
        /// </remarks>
        Sizes = 0,

        /// <summary>
        /// Queries the context for the name of the user assocated with a security context.
        /// </summary>
        /// <remarks>
        /// Results for a query of this type are stored in a Win32 SecPkgContext_Name structure.
        /// </remarks>
        Names = 1,

        /// <summary>
        /// Queries the name of the authenticating authority for the security context.
        /// </summary>
        /// <remarks>
        /// Results for a query of this type are stored in a Win32 SecPkgContext_Authority structure.
        /// </remarks>
        Authority = 6,
    }
}
