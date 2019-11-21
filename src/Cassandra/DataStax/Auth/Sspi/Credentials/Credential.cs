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

namespace Cassandra.DataStax.Auth.Sspi.Credentials
{
    /// <summary>
    /// Provides access to the pre-existing credentials of a security principle.
    /// </summary>
    internal class Credential : IDisposable
    {
        /// <summary>
        /// Whether the Credential has been disposed.
        /// </summary>
        private bool disposed;

        /// <summary>
        /// The name of the security package that controls the credential.
        /// </summary>
        private string securityPackage;

        /// <summary>
        /// A safe handle to the credential's handle.
        /// </summary>
        private SafeCredentialHandle safeCredHandle;

        /// <summary>
        /// The UTC time the credentials expire.
        /// </summary>
        private DateTime expiry;

        /// <summary>
        /// Initializes a new instance of the Credential class.
        /// </summary>
        /// <param name="package">The security package to acquire the credential from.</param>
        public Credential( string package )
        {
            this.disposed = false;
            this.securityPackage = package;

            this.expiry = DateTime.MinValue;

            this.PackageInfo = PackageSupport.GetPackageCapabilities( this.SecurityPackage );
        }
        
        /// <summary>
        /// Gets metadata for the security package associated with the credential.
        /// </summary>
        public SecPkgInfo PackageInfo { get; private set; }

        /// <summary>
        /// Gets the name of the security package that owns the credential.
        /// </summary>
        public string SecurityPackage
        {
            get
            {
                CheckLifecycle();

                return this.securityPackage;
            }
        }

        /// <summary>
        /// Returns the User Principle Name of the credential. Depending on the underlying security
        /// package used by the credential, this may not be the same as the Down-Level Logon Name
        /// for the user.
        /// </summary>
        public string PrincipleName
        {
            get
            {
                QueryNameAttribCarrier carrier;
                SecurityStatus status;
                string name = null;
                bool gotRef = false;

                CheckLifecycle();

                status = SecurityStatus.InternalError;
                carrier = new QueryNameAttribCarrier();

                RuntimeHelpers.PrepareConstrainedRegions();
                try
                {
                    this.safeCredHandle.DangerousAddRef( ref gotRef );
                }
                catch( Exception )
                {
                    if( gotRef == true )
                    {
                        this.safeCredHandle.DangerousRelease();
                        gotRef = false;
                    }
                    throw;
                }
                finally
                {
                    if( gotRef )
                    {
                        status = CredentialNativeMethods.QueryCredentialsAttribute_Name(
                            ref this.safeCredHandle.rawHandle,
                            CredentialQueryAttrib.Names,
                            ref carrier
                        );

                        this.safeCredHandle.DangerousRelease();

                        if( status == SecurityStatus.OK && carrier.Name != IntPtr.Zero )
                        {
                            try
                            {
                                name = Marshal.PtrToStringUni( carrier.Name );
                            }
                            finally
                            {
                                NativeMethods.FreeContextBuffer( carrier.Name );
                            }
                        }
                    }
                }

                if( status.IsError() )
                {
                    throw new SspiException( "Failed to query credential name", status );
                }

                return name;
            }
        }

        /// <summary>
        /// Gets the UTC time the credentials expire.
        /// </summary>
        public DateTime Expiry
        {
            get
            {
                CheckLifecycle();

                return this.expiry;
            }

            protected set
            {
                CheckLifecycle();
                                
                this.expiry = value;
            }
        }

        /// <summary>
        /// Gets a handle to the credential.
        /// </summary>
        public SafeCredentialHandle Handle
        {
            get
            {
                CheckLifecycle();

                return this.safeCredHandle;
            }

            protected set
            {
                CheckLifecycle();

                this.safeCredHandle = value;
            }
        }

        /// <summary>
        /// Releases all resources associated with the credential.
        /// </summary>
        public void Dispose()
        {
            Dispose( true );
            GC.SuppressFinalize( this );
        }

        protected virtual void Dispose( bool disposing )
        {
            if ( this.disposed == false )
            {
                if ( disposing )
                {
                    this.safeCredHandle.Dispose();
                }

                this.disposed = true;
            }
        }

        private void CheckLifecycle()
        {
            if( this.disposed )
            {
                throw new ObjectDisposedException( "Credential" );
            }
        }
    }
}
