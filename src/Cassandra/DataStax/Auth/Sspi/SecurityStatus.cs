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

namespace Cassandra.DataStax.Auth.Sspi
{
    /*
    // From winerror.h 
    #define SEC_E_OK                         ((HRESULT)0x00000000L)
    #define SEC_E_INSUFFICIENT_MEMORY        _HRESULT_TYPEDEF_(0x80090300L)
    #define SEC_E_INVALID_HANDLE             _HRESULT_TYPEDEF_(0x80090301L)
    #define SEC_E_UNSUPPORTED_FUNCTION       _HRESULT_TYPEDEF_(0x80090302L)
    #define SEC_E_TARGET_UNKNOWN             _HRESULT_TYPEDEF_(0x80090303L)
    #define SEC_E_INTERNAL_ERROR             _HRESULT_TYPEDEF_(0x80090304L)
    #define SEC_E_SECPKG_NOT_FOUND           _HRESULT_TYPEDEF_(0x80090305L)
    #define SEC_E_NOT_OWNER                  _HRESULT_TYPEDEF_(0x80090306L)
    #define SEC_E_UNKNOWN_CREDENTIALS        _HRESULT_TYPEDEF_(0x8009030DL)
    #define SEC_E_NO_CREDENTIALS             _HRESULT_TYPEDEF_(0x8009030EL)
    */

    /// <summary>
    /// Defines the results of invoking the SSPI api.
    /// </summary>
    internal enum SecurityStatus : uint
    {
        // --- Success / Informational ---

        /// <summary>
        /// The request completed successfully
        /// </summary>
        [EnumString( "No error" )]
        OK                  = 0x00000000,
        
        /// <summary>
        /// The token returned by the context needs to be provided to the cooperating party
        /// to continue construction of the context.
        /// </summary>
        [EnumString( "Authentication cycle needs to continue" )]
        ContinueNeeded = 0x00090312,

        /// <summary>
        /// Occurs after a client calls InitializeSecurityContext to indicate that the client
        /// must call CompleteAuthToken.
        /// </summary>
        [EnumString( "Authentication cycle needs to perform a 'complete'." )]
        CompleteNeeded      = 0x00090313,

        /// <summary>
        /// Occurs after a client calls InitializeSecurityContext to indicate that the client
        /// must call CompleteAuthToken and pass the result to the server.
        /// </summary>
        [EnumString( "Authentication cycle needs to perform a 'complete' and then continue." )]
        CompAndContinue = 0x00090314,

        /// <summary>
        /// An attempt to use the context was performed after the context's expiration time elapsed.
        /// </summary>
        [EnumString( "The security context was used after its expiration time passed." )]
        ContextExpired = 0x00090317,

        [EnumString( "The credentials supplied to the security context were not fully initialized." )]
        CredentialsNeeded = 0x00090320,

        [EnumString( "The context data must be re-negotiated with the peer" )]
        Renegotiate         = 0x00090321,

        // Errors
        [EnumString( "Not enough memory.")]
        OutOfMemory         = 0x80090300,

        [EnumString( "The handle provided to the API was invalid.")]
        InvalidHandle       = 0x80090301,

        [EnumString( "The attempted operation is not supported")]
        Unsupported         = 0x80090302,

        [EnumString( "The specified principle is not known in the authentication system.")]
        TargetUnknown       = 0x80090303,
        
        [EnumString( "An internal error occurred" )]
        InternalError       = 0x80090304,

        /// <summary>
        /// No security provider package was found with the given name.
        /// </summary>
        [EnumString( "The requested security package was not found.")]
        PackageNotFound     = 0x80090305,

        NotOwner            = 0x80090306,
        CannotInstall       = 0x80090307,

        /// <summary>
        /// A token was provided that contained incorrect or corrupted data.
        /// </summary>
        [EnumString("The provided authentication token is invalid or corrupted.")]
        InvalidToken        = 0x80090308,
        
        CannotPack          = 0x80090309,
        QopNotSupported     = 0x8009030A,

        /// <summary>
        /// Impersonation is not supported.
        /// </summary>
        [EnumString("Impersonation is not supported with the current security package.")]
        NoImpersonation     = 0x8009030B,

        [EnumString("The logon was denied, perhaps because the provided credentials were incorrect.")]
        LogonDenied         = 0x8009030C,


        [EnumString( "The credentials provided are not recognized by the selected security package.")]
        UnknownCredentials  = 0x8009030D,

        [EnumString( "No credentials are available in the selected security package.")]
        NoCredentials       = 0x8009030E,

        [EnumString( "A message that was provided to the Decrypt or VerifySignature functions was altered " +
        "after it was created.")]
        MessageAltered      = 0x8009030F,

        [EnumString( "A message was received out of the expected order.")]
        OutOfSequence       = 0x80090310,

        [EnumString( "The current security package cannot contact an authenticating authority.")]
        NoAuthenticatingAuthority = 0x80090311,

        /// <summary>
        /// The buffer provided to an SSPI API call contained a message that was not complete.
        /// </summary>
        /// <remarks>
        /// This occurs regularly with SSPI contexts that exchange data using a streaming context,
        /// where the data returned from the streaming communications channel, such as a TCP socket,
        /// did not contain the complete message. 
        /// Similarly, a streaming channel may return too much data, in which case the API function
        /// will indicate success, but will save off the extra, unrelated data in a buffer of
        /// type 'extra'.
        /// </remarks>
        IncompleteMessage   = 0x80090318,
        IncompleteCredentials = 0x80090320,
        BufferNotEnough     = 0x80090321,
        WrongPrincipal      = 0x80090322,
        TimeSkew            = 0x80090324,
        UntrustedRoot       = 0x80090325,
        IllegalMessage      = 0x80090326,
        CertUnknown         = 0x80090327,
        CertExpired         = 0x80090328,
        AlgorithmMismatch   = 0x80090331,
        SecurityQosFailed   = 0x80090332,
        SmartcardLogonRequired = 0x8009033E,
        UnsupportedPreauth  = 0x80090343,
        BadBinding          = 0x80090346
    }

    /// <summary>
    /// Provides extension methods for the SecurityStatus enumeration.
    /// </summary>
    internal static class SecurityStatusExtensions
    {
        /// <summary>
        /// Returns whether or not the status represents an error.
        /// </summary>
        /// <param name="status"></param>
        /// <returns>True if the status represents an error condition.</returns>
        public static bool IsError( this SecurityStatus status )
        {
            return (uint)status > 0x80000000u;
        }
    }

}
