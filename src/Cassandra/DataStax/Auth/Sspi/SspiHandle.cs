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

namespace Cassandra.DataStax.Auth.Sspi
{
    /// <summary>
    /// Represents the raw structure for any handle created for the SSPI API, for example, credential 
    /// handles, context handles, and security package handles. Any SSPI handle is always the size 
    /// of two native pointers. 
    /// </summary>
    /// <remarks>
    /// The documentation for SSPI handles can be found here:
    /// http://msdn.microsoft.com/en-us/library/windows/desktop/aa380495(v=vs.85).aspx
    /// 
    /// This class is not reference safe - if used directly, or referenced directly, it may be leaked,
    /// or subject to finalizer races, or any of the hundred of things SafeHandles were designed to fix.
    /// Do not directly use this class - use only though SafeHandle wrapper objects. Any reference needed
    /// to this handle for performing work (InitializeSecurityContext, eg) should be performed a CER
    /// that employs handle reference counting across the native API invocation.
    /// </remarks>
    [StructLayout( LayoutKind.Sequential, Pack = 1 ) ]
    internal struct RawSspiHandle
    {
        private IntPtr lowPart;
        private IntPtr highPart;

        /// <summary>
        /// Returns whether or not the handle is set to the default, empty value.
        /// </summary>
        /// <returns></returns>
        public bool IsZero()
        {
            return this.lowPart == IntPtr.Zero && this.highPart == IntPtr.Zero;
        }

        /// <summary>
        /// Sets the handle to an invalid value.
        /// </summary>
        /// <remarks>
        /// This method is executed in a CER during handle release.
        /// </remarks>
        [ReliabilityContract( Consistency.WillNotCorruptState, Cer.Success)]
        public void SetInvalid()
        {
            this.lowPart = IntPtr.Zero;
            this.highPart = IntPtr.Zero;
        }
    }

    /// <summary>
    /// Safely encapsulates a raw handle used in the SSPI api.
    /// </summary>
    internal abstract class SafeSspiHandle : SafeHandle
    {
        internal RawSspiHandle rawHandle;

        protected SafeSspiHandle()
            : base( IntPtr.Zero, true )
        {
            this.rawHandle = new RawSspiHandle();
        }

        public override bool IsInvalid
        {
            get { return IsClosed || this.rawHandle.IsZero();  }
        }

        [ReliabilityContract( Consistency.WillNotCorruptState, Cer.Success )]
        protected override bool ReleaseHandle()
        {
            this.rawHandle.SetInvalid();
            return true;
        }
    }
}
