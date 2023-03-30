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
using System.Runtime.Serialization;

namespace Cassandra.DataStax.Auth.Sspi
{
    /// <summary>
    /// The exception that is thrown when a problem occurs hwen using the SSPI system.
    /// </summary>
#if NET452
    [Serializable]
#endif
    public class SspiException : Exception
    {
        private SecurityStatus errorCode;
        private string message;

        /// <summary>
        /// Initializes a new instance of the SSPIException class with the given message and status.
        /// </summary>
        /// <param name="message">A message explaining what part of the system failed.</param>
        /// <param name="errorCode">The error code observed during the failure.</param>
        internal SspiException( string message, SecurityStatus errorCode )
        {
            this.message = message;
            this.errorCode = errorCode;
        }

        /// <summary>
        /// Creates a new instance of <see cref="SspiException"/>.
        /// </summary>
        public SspiException(string message, int errorCode) : this(message, (SecurityStatus) errorCode)
        {
            
        }
        
#if NET452
        /// <summary>
        /// Initializes a new instance of the SSPIException class from serialization data.
        /// </summary>
        /// <param name="info"></param>
        /// <param name="context"></param>
        protected SspiException( SerializationInfo info, StreamingContext context )
            : base( info, context )
        {
            this.message = info.GetString( "messsage" );
            this.errorCode = (SecurityStatus)info.GetUInt32( "errorCode" );
        }

        /// <summary>
        /// Serializes the exception.
        /// </summary>
        /// <param name="info"></param>
        /// <param name="context"></param>
        public override void GetObjectData( SerializationInfo info, StreamingContext context )
        {
            base.GetObjectData( info, context );

            info.AddValue( "message", this.message );
            info.AddValue( "errorCode", this.errorCode );
        }
#endif

        /// <summary>
        /// The error code that was observed during the SSPI call.
        /// </summary>
        public int ErrorCode
        {
            get
            {
                return (int) this.errorCode;
            }
        }

        /// <summary>
        /// A human-readable message indicating the nature of the exception.
        /// </summary>
        public override string Message
        {
            get
            {
                return string.Format( 
                    "{0}. Error Code = '0x{1:X}' - \"{2}\".", 
                    this.message, 
                    this.errorCode, 
                    EnumMgr.ToText(this.errorCode) 
                );
            }
        }
    }
}
