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
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace Cassandra.DataStax.Auth.Sspi
{
    /// <summary>
    /// Queries information about security packages.
    /// </summary>
    internal static class PackageSupport
    {
        /// <summary>
        /// Returns the properties of the named package.
        /// </summary>
        /// <param name="packageName">The name of the package.</param>
        /// <returns></returns>
        public static SecPkgInfo GetPackageCapabilities( string packageName )
        {
            SecPkgInfo info;
            SecurityStatus status = SecurityStatus.InternalError;

            IntPtr rawInfoPtr;
            
            rawInfoPtr = new IntPtr();
            info = new SecPkgInfo();

            RuntimeHelpers.PrepareConstrainedRegions();
            try
            { }
            finally
            {
                status = NativeMethods.QuerySecurityPackageInfo( packageName, ref rawInfoPtr );

                if ( rawInfoPtr != IntPtr.Zero )
                {
                    try
                    {
                        if ( status == SecurityStatus.OK )
                        {
                            // This performs allocations as it makes room for the strings contained in the SecPkgInfo class.
                            Marshal.PtrToStructure( rawInfoPtr, info );
                        }
                    }
                    finally
                    {
                        NativeMethods.FreeContextBuffer( rawInfoPtr );
                    }
                }
            }

            if( status != SecurityStatus.OK )
            {
                throw new SspiException( "Failed to query security package provider details", status );
            }

            return info;
        }

        /// <summary>
        /// Returns a list of all known security package providers and their properties.
        /// </summary>
        /// <returns></returns>
        public static SecPkgInfo[] EnumeratePackages()
        {
            SecurityStatus status = SecurityStatus.InternalError;
            SecPkgInfo[] packages = null;
            IntPtr pkgArrayPtr;
            IntPtr pkgPtr;
            int numPackages = 0;
            int pkgSize = Marshal.SizeOf( typeof(SecPkgInfo) );

            pkgArrayPtr = new IntPtr();

            RuntimeHelpers.PrepareConstrainedRegions();
            try { }
            finally
            {
                status = NativeMethods.EnumerateSecurityPackages( ref numPackages, ref pkgArrayPtr );

                if( pkgArrayPtr != IntPtr.Zero )
                {
                    try
                    {
                        if( status == SecurityStatus.OK )
                        {
                            // Bwooop Bwooop Alocation Alert
                            // 1) We allocate the array
                            // 2) We allocate the individual elements in the array (they're class objects).
                            // 3) We allocate the strings in the individual elements in the array when we 
                            //    call Marshal.PtrToStructure()

                            packages = new SecPkgInfo[numPackages];

                            for( int i = 0; i < numPackages; i++ )
                            {
                                packages[i] = new SecPkgInfo();
                            }
                            
                            for( int i = 0; i < numPackages; i++ )
                            {
                                pkgPtr = IntPtr.Add( pkgArrayPtr, i * pkgSize );

                                Marshal.PtrToStructure( pkgPtr, packages[i] );
                            }
                        }
                    }
                    finally
                    {
                        NativeMethods.FreeContextBuffer( pkgArrayPtr );
                    }
                }
            }

            if( status != SecurityStatus.OK )
            {
                throw new SspiException( "Failed to enumerate security package providers", status );
            }

            return packages;
        }
    }
}
