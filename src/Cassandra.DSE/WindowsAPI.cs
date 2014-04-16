//
//      Copyright (C) 2012 DataStax Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

//based on http://www.pinvoke.net/default.aspx/secur32.initializesecuritycontext


using System;
using System.Runtime.InteropServices;

internal static class WindowsAPI
{
    public const uint SEC_E_OK = 0x0;
    public const uint SEC_E_INSUFFICENT_MEMORY = 0x80090300;
    public const uint SEC_E_INVALID_HANDLE = 0x80090301;
    public const uint SEC_E_TARGET_UNKNOWN = 0x80090303;
    public const uint SEC_E_INTERNAL_ERROR = 0x80090304;
    public const uint SEC_E_SECPKG_NOT_FOUND = 0x80090305;
    public const uint SEC_E_INVALID_TOKEN = 0x80090308;
    public const uint SEC_E_QOP_NOT_SUPPORTED = 0x8009030A;
    public const uint SEC_E_LOGON_DENIED = 0x8009030C;
    public const uint SEC_E_UNKNOWN_CREDENTIALS = 0x8009030D;
    public const uint SEC_E_NO_CREDENTIALS = 0x8009030E;
    public const uint SEC_E_MESSAGE_ALTERED = 0x8009030F;
    public const uint SEC_E_OUT_OF_SEQUENCE = 0x80090310;
    public const uint SEC_E_NO_AUTHENTICATING_AUTHORITY = 0x80090311;
    public const uint SEC_E_CONTEXT_EXPIRED = 0x80090317;
    public const uint SEC_E_INCOMPLETE_MESSAGE = 0x80090318;
    public const uint SEC_E_BUFFER_TOO_SMALL = 0x80090321;
    public const uint SEC_E_CRYPTO_SYSTEM_INVALID = 0x80090337;

    public const uint SEC_I_CONTINUE_NEEDED = 0x00090312;
    public const uint SEC_I_CONTEXT_EXPIRED = 0x00090317;
    public const uint SEC_I_RENEGOTIATE = 0x00090321;

    public const uint SECQOP_WRAP_NO_ENCRYPT = 0x80000001;


    public const int TOKEN_QUERY = 0x00008;
    public const int SECPKG_CRED_OUTBOUND = 2;
    public const int SECURITY_NATIVE_DREP = 0x10;
    public const int SECPKG_CRED_INBOUND = 1;
    public const int MAX_TOKEN_SIZE = 12288;

    public const int ISC_REQ_DELEGATE = 0x00000001;
    public const int ISC_REQ_MUTUAL_AUTH = 0x00000002;
    public const int ISC_REQ_REPLAY_DETECT = 0x00000004;
    public const int ISC_REQ_SEQUENCE_DETECT = 0x00000008;
    public const int ISC_REQ_CONFIDENTIALITY = 0x00000010;
    public const int ISC_REQ_USE_SESSION_KEY = 0x00000020;
    public const int ISC_REQ_PROMPT_FOR_CREDS = 0x00000040;
    public const int ISC_REQ_USE_SUPPLIED_CREDS = 0x00000080;
    public const int ISC_REQ_ALLOCATE_MEMORY = 0x00000100;
    public const int ISC_REQ_USE_DCE_STYLE = 0x00000200;
    public const int ISC_REQ_DATAGRAM = 0x00000400;
    public const int ISC_REQ_CONNECTION = 0x00000800;
    public const int ISC_REQ_CALL_LEVEL = 0x00001000;
    public const int ISC_REQ_FRAGMENT_SUPPLIED = 0x00002000;
    public const int ISC_REQ_EXTENDED_ERROR = 0x00004000;
    public const int ISC_REQ_STREAM = 0x00008000;
    public const int ISC_REQ_INTEGRITY = 0x00010000;
    public const int ISC_REQ_IDENTIFY = 0x00020000;
    public const int ISC_REQ_NULL_SESSION = 0x00040000;
    public const int ISC_REQ_MANUAL_CRED_VALIDATION = 0x00080000;
    public const int ISC_REQ_RESERVED1 = 0x00100000;
    public const int ISC_REQ_FRAGMENT_TO_FIT = 0x00200000;

    public const int SECPKG_ATTR_SIZES = 0;

    [DllImport("secur32", CharSet = CharSet.Auto)]
    public static extern int AcquireCredentialsHandle(
    string pszPrincipal, //SEC_CHAR*
    string pszPackage, //SEC_CHAR* //"Kerberos","NTLM","Negotiative"
    int fCredentialUse,
    IntPtr PAuthenticationID,//_LUID AuthenticationID,//pvLogonID, //PLUID
    ref AuthIdentityEx pAuthData,//PVOID
    int pGetKeyFn, //SEC_GET_KEY_FN
    IntPtr pvGetKeyArgument, //PVOID
    ref SECURITY_HANDLE phCredential, //SecHandle //PCtxtHandle ref
    ref SECURITY_INTEGER ptsExpiry); //PTimeStamp //TimeStamp ref

    [DllImport("secur32", CharSet = CharSet.Auto)]
    public static extern int AcquireCredentialsHandle(
    System.IntPtr pszPrincipal, //SEC_CHAR*
    string pszPackage, //SEC_CHAR* //"Kerberos","NTLM","Negotiative"
    int fCredentialUse,
    IntPtr PAuthenticationID,//_LUID AuthenticationID,//pvLogonID, //PLUID
    ref AuthIdentityEx pAuthData,//PVOID
    int pGetKeyFn, //SEC_GET_KEY_FN
    IntPtr pvGetKeyArgument, //PVOID
    ref SECURITY_HANDLE phCredential, //SecHandle //PCtxtHandle ref
    ref SECURITY_INTEGER ptsExpiry); //PTimeStamp //TimeStamp ref

    [DllImport("secur32", CharSet = CharSet.Auto)]
    public static extern int AcquireCredentialsHandle(
    System.IntPtr pszPrincipal, //SEC_CHAR*
    string pszPackage, //SEC_CHAR* //"Kerberos","NTLM","Negotiative"
    int fCredentialUse,
    IntPtr PAuthenticationID,//_LUID AuthenticationID,//pvLogonID, //PLUID
    System.IntPtr pAuthData,//PVOID
    int pGetKeyFn, //SEC_GET_KEY_FN
    IntPtr pvGetKeyArgument, //PVOID
    ref SECURITY_HANDLE phCredential, //SecHandle //PCtxtHandle ref
    ref SECURITY_INTEGER ptsExpiry); //PTimeStamp //TimeStamp ref

    [DllImport("secur32", CharSet = CharSet.Auto, SetLastError = true)]
    public static extern int InitializeSecurityContext(ref SECURITY_HANDLE phCredential,//PCredHandle
    System.IntPtr phContext, //PCtxtHandle
    string pszTargetName,
    int fContextReq,
    int Reserved1,
    int TargetDataRep,
    IntPtr pInput, //PSecBufferDesc SecBufferDesc
    int Reserved2,
    out SECURITY_HANDLE phNewContext, //PCtxtHandle
    out SecBufferDesc pOutput, //PSecBufferDesc SecBufferDesc
    out uint pfContextAttr, //managed ulong == 64 bits!!!
    out SECURITY_INTEGER ptsExpiry); //PTimeStamp

    [DllImport("secur32", CharSet = CharSet.Auto, SetLastError = true)]
    public static extern int InitializeSecurityContext(ref SECURITY_HANDLE phCredential,//PCredHandle
        ref SECURITY_HANDLE phContext, //PCtxtHandle
        string pszTargetName,
        int fContextReq,
        int Reserved1,
        int TargetDataRep,
        ref SecBufferDesc SecBufferDesc, //PSecBufferDesc SecBufferDesc
        int Reserved2,
        out SECURITY_HANDLE phNewContext, //PCtxtHandle
        out SecBufferDesc pOutput, //PSecBufferDesc SecBufferDesc
        out uint pfContextAttr, //managed ulong == 64 bits!!!
        out SECURITY_INTEGER ptsExpiry); //PTimeStamp

    [DllImport("secur32.Dll", CharSet = CharSet.Auto, SetLastError = false)]
    public static extern int AcceptSecurityContext(ref SECURITY_HANDLE phCredential,
                                            IntPtr phContext,
                                            ref SecBufferDesc pInput,
                                            uint fContextReq,
                                            uint TargetDataRep,
                                            out SECURITY_HANDLE phNewContext,
                                            out SecBufferDesc pOutput,
                                            out uint pfContextAttr,    //managed ulong == 64 bits!!!
                                            out SECURITY_INTEGER ptsTimeStamp);

    [DllImport("secur32.Dll", CharSet = CharSet.Auto, SetLastError = false)]
    public static extern int AcceptSecurityContext(ref SECURITY_HANDLE phCredential,
                                            ref SECURITY_HANDLE phContext,
                                            ref SecBufferDesc pInput,
                                            uint fContextReq,
                                            uint TargetDataRep,
                                            out SECURITY_HANDLE phNewContext,
                                            out SecBufferDesc pOutput,
                                            out uint pfContextAttr,    //managed ulong == 64 bits!!!
                                            out SECURITY_INTEGER ptsTimeStamp);

    [DllImport("secur32.Dll", CharSet = CharSet.Auto, SetLastError = false)]
    public static extern int ImpersonateSecurityContext(ref SECURITY_HANDLE phContext);

    [DllImport("secur32.Dll", CharSet = CharSet.Auto, SetLastError = false)]
    public static extern int QueryContextAttributes(ref SECURITY_HANDLE phContext,
                                                    uint ulAttribute,
                                                    out SecPkgContext_Sizes pContextAttributes);

    [DllImport("secur32.Dll", CharSet = CharSet.Auto, SetLastError = false)]
    public static extern int EncryptMessage(ref SECURITY_HANDLE phContext,
                                            uint fQOP,        //managed ulong == 64 bits!!!
                                            ref SecBufferDesc pMessage,
                                            uint MessageSeqNo);    //managed ulong == 64 bits!!!

    [DllImport("secur32.Dll", CharSet = CharSet.Auto, SetLastError = false)]
    public static extern int DecryptMessage(ref SECURITY_HANDLE phContext,
                                             ref SecBufferDesc pMessage,
                                             uint MessageSeqNo,
                                             out uint pfQOP);

    [DllImport("secur32.Dll", CharSet = CharSet.Auto, SetLastError = false)]
    public static extern int MakeSignature(ref SECURITY_HANDLE phContext,          // Context to use
                                            uint fQOP,         // Quality of Protection
                                            ref SecBufferDesc pMessage,        // Message to sign
                                            uint MessageSeqNo);      // Message Sequence Num.

    [DllImport("secur32.Dll", CharSet = CharSet.Auto, SetLastError = false)]
    public static extern int VerifySignature(ref SECURITY_HANDLE phContext,          // Context to use
                                            ref SecBufferDesc pMessage,        // Message to sign
                                            uint MessageSeqNo,            // Message Sequence Num.
                                            out uint pfQOP);      // Quality of Protection




    internal enum SecBufferType
    {
        SECBUFFER_VERSION = 0,
        SECBUFFER_EMPTY = 0,
        SECBUFFER_DATA = 1,
        SECBUFFER_TOKEN = 2,
        SECBUFFER_PADDING = 9,
        SECBUFFER_STREAM = 10,
    }

    [StructLayout(LayoutKind.Sequential)]
    internal struct SecHandle //=PCtxtHandle
    {
        IntPtr dwLower; // ULONG_PTR translates to IntPtr not to uint
        IntPtr dwUpper; // this is crucial for 64-Bit Platforms
    }

    [StructLayout(LayoutKind.Sequential)]
    internal struct SecBuffer : IDisposable
    {
        public int cbBuffer;
        public int BufferType;
        public IntPtr pvBuffer;


        public SecBuffer(int bufferSize)
        {
            cbBuffer = bufferSize;
            BufferType = (int)SecBufferType.SECBUFFER_TOKEN;
            pvBuffer = Marshal.AllocHGlobal(bufferSize);
        }

        public SecBuffer(byte[] secBufferBytes)
        {
            cbBuffer = secBufferBytes.Length;
            BufferType = (int)SecBufferType.SECBUFFER_TOKEN;
            pvBuffer = Marshal.AllocHGlobal(cbBuffer);
            Marshal.Copy(secBufferBytes, 0, pvBuffer, cbBuffer);
        }

        public SecBuffer(byte[] secBufferBytes, SecBufferType bufferType)
        {
            cbBuffer = secBufferBytes.Length;
            BufferType = (int)bufferType;
            pvBuffer = Marshal.AllocHGlobal(cbBuffer);
            Marshal.Copy(secBufferBytes, 0, pvBuffer, cbBuffer);
        }

        public void Dispose()
        {
            if (pvBuffer != IntPtr.Zero)
            {
                Marshal.FreeHGlobal(pvBuffer);
                pvBuffer = IntPtr.Zero;
            }
        }
    }

    internal struct MultipleSecBufferHelper
    {
        public byte[] Buffer;
        public SecBufferType BufferType;

        public MultipleSecBufferHelper(byte[] buffer, SecBufferType bufferType)
        {
            Buffer = buffer;
            BufferType = bufferType;
        }

        public MultipleSecBufferHelper(uint bufSize, SecBufferType bufferType)
        {
            Buffer = new byte[bufSize];
            BufferType = bufferType;
        }
    };

    [StructLayout(LayoutKind.Sequential)]
    internal struct SecBufferDesc : IDisposable
    {

        public int ulVersion;
        public int cBuffers;
        public IntPtr pBuffers; //Point to SecBuffer

        public SecBufferDesc(int bufferSize)
        {
            ulVersion = (int)SecBufferType.SECBUFFER_VERSION;
            if (bufferSize < 0)
            {
                cBuffers = 0;
                pBuffers = IntPtr.Zero;
            }
            else
            {
                cBuffers = 1;
                SecBuffer ThisSecBuffer = new SecBuffer(bufferSize);
                pBuffers = Marshal.AllocHGlobal(Marshal.SizeOf(ThisSecBuffer));
                Marshal.StructureToPtr(ThisSecBuffer, pBuffers, false);
            }
        }

        public SecBufferDesc(byte[] secBufferBytes)
        {
            ulVersion = (int)SecBufferType.SECBUFFER_VERSION;
            cBuffers = 1;
            SecBuffer ThisSecBuffer = new SecBuffer(secBufferBytes);
            pBuffers = Marshal.AllocHGlobal(Marshal.SizeOf(ThisSecBuffer));
            Marshal.StructureToPtr(ThisSecBuffer, pBuffers, false);
        }

        public SecBufferDesc(MultipleSecBufferHelper[] secBufferBytesArray)
        {
            if (secBufferBytesArray == null || secBufferBytesArray.Length == 0)
            {
                throw new ArgumentException("secBufferBytesArray cannot be null or 0 length");
            }

            ulVersion = (int)SecBufferType.SECBUFFER_VERSION;
            cBuffers = secBufferBytesArray.Length;

            //Allocate memory for SecBuffer Array....
            pBuffers = Marshal.AllocHGlobal(Marshal.SizeOf(typeof(SecBuffer)) * cBuffers);

            for (int Index = 0; Index < secBufferBytesArray.Length; Index++)
            {
                //Super hack: Now allocate memory for the individual SecBuffers
                //and just copy the bit values to the SecBuffer array!!!
                SecBuffer ThisSecBuffer = secBufferBytesArray[Index].Buffer == null ? new SecBuffer() :
                    new SecBuffer(secBufferBytesArray[Index].Buffer, secBufferBytesArray[Index].BufferType);

                //We will write out bits in the following order:
                //int cbBuffer;
                //int BufferType;
                //pvBuffer;
                //Note that we won't be releasing the memory allocated by ThisSecBuffer until we
                //are disposed...
                int CurrentOffset = Index * Marshal.SizeOf(typeof(SecBuffer));
                Marshal.WriteInt32(pBuffers, CurrentOffset, ThisSecBuffer.cbBuffer);
                Marshal.WriteInt32(pBuffers, CurrentOffset + Marshal.SizeOf(ThisSecBuffer.cbBuffer), ThisSecBuffer.BufferType);
                Marshal.WriteIntPtr(pBuffers, CurrentOffset + Marshal.SizeOf(ThisSecBuffer.cbBuffer) + Marshal.SizeOf(ThisSecBuffer.BufferType), ThisSecBuffer.pvBuffer);
            }
        }

        public void Dispose()
        {
            if (pBuffers != IntPtr.Zero)
            {
                if (cBuffers == 1)
                {
                    SecBuffer ThisSecBuffer = (SecBuffer)Marshal.PtrToStructure(pBuffers, typeof(SecBuffer));
                    ThisSecBuffer.Dispose();
                }
                else
                {
                    for (int Index = 0; Index < cBuffers; Index++)
                    {
                        //The bits were written out the following order:
                        //int cbBuffer;
                        //int BufferType;
                        //pvBuffer;
                        //What we need to do here is to grab a hold of the pvBuffer allocate by the individual
                        //SecBuffer and release it...
                        int CurrentOffset = Index * Marshal.SizeOf(typeof(SecBuffer));
                        IntPtr SecBufferpvBuffer = Marshal.ReadIntPtr(pBuffers, CurrentOffset + Marshal.SizeOf(typeof(int)) + Marshal.SizeOf(typeof(int)));
                        Marshal.FreeHGlobal(SecBufferpvBuffer);
                    }
                }

                Marshal.FreeHGlobal(pBuffers);
                pBuffers = IntPtr.Zero;
            }
        }

        public byte[] GetSecBufferByteArray()
        {
            byte[] Buffer = null;

            if (pBuffers == IntPtr.Zero)
            {
                throw new InvalidOperationException("Object has already been disposed!!!");
            }

            if (cBuffers == 1)
            {
                SecBuffer ThisSecBuffer = (SecBuffer)Marshal.PtrToStructure(pBuffers, typeof(SecBuffer));

                if (ThisSecBuffer.cbBuffer > 0)
                {
                    Buffer = new byte[ThisSecBuffer.cbBuffer];
                    Marshal.Copy(ThisSecBuffer.pvBuffer, Buffer, 0, ThisSecBuffer.cbBuffer);
                }
            }
            else
            {
                int BytesToAllocate = 0;

                for (int Index = 0; Index < cBuffers; Index++)
                {
                    //The bits were written out the following order:
                    //int cbBuffer;
                    //int BufferType;
                    //pvBuffer;
                    //What we need to do here calculate the total number of bytes we need to copy...
                    int CurrentOffset = Index * Marshal.SizeOf(typeof(SecBuffer));
                    BytesToAllocate += Marshal.ReadInt32(pBuffers, CurrentOffset);
                }

                Buffer = new byte[BytesToAllocate];

                for (int Index = 0, BufferIndex = 0; Index < cBuffers; Index++)
                {
                    //The bits were written out the following order:
                    //int cbBuffer;
                    //int BufferType;
                    //pvBuffer;
                    //Now iterate over the individual buffers and put them together into a
                    //byte array...
                    int CurrentOffset = Index * Marshal.SizeOf(typeof(SecBuffer));
                    int BytesToCopy = Marshal.ReadInt32(pBuffers, CurrentOffset);
                    if (BytesToCopy == 0)
                        break;
                    IntPtr SecBufferpvBuffer = Marshal.ReadIntPtr(pBuffers, CurrentOffset + Marshal.SizeOf(typeof(int)) + Marshal.SizeOf(typeof(int)));
                    Marshal.Copy(SecBufferpvBuffer, Buffer, BufferIndex, BytesToCopy);
                    BufferIndex += BytesToCopy;
                }
            }

            return (Buffer);
        }
    }

    [StructLayout(LayoutKind.Sequential)]
    internal struct SECURITY_INTEGER
    {
        public uint LowPart;
        public int HighPart;
        public SECURITY_INTEGER(int dummy)
        {
            LowPart = 0;
            HighPart = 0;
        }
    };

    [StructLayout(LayoutKind.Sequential)]
    internal struct SECURITY_HANDLE
    {
        public IntPtr LowPart;
        public IntPtr HighPart;
        public SECURITY_HANDLE(int dummy)
        {
            LowPart = HighPart = IntPtr.Zero;
        }
    };

    [StructLayout(LayoutKind.Sequential)]
    internal struct SecPkgContext_Sizes
    {
        public uint cbMaxToken;
        public uint cbMaxSignature;
        public uint cbBlockSize;
        public uint cbSecurityTrailer;
    };

    [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Auto)]
    internal struct AuthIdentityEx
    {
        // see SEC_WINNT_AUTH_IDENTITY_EX
        internal int Version;

        internal int Length;

        internal string UserName;

        internal int UserNameLength;

        internal string Domain;

        internal int DomainLength;

        internal string Password;

        internal int PasswordLength;

        internal int Flags;

        internal string PackageList;

        internal int PackageListLength;

        // sspi.h: #define SEC_WINNT_AUTH_IDENTITY_VERSION 0x200
        static readonly int WinNTAuthIdentityVersion = 0x200;

        internal AuthIdentityEx(string userName, string password, string domain, params string[] additionalPackages)
        {
            Version = WinNTAuthIdentityVersion;
            Length = Marshal.SizeOf(typeof(AuthIdentityEx));
            UserName = userName;
            UserNameLength = userName == null ? 0 : userName.Length;
            Password = password;
            PasswordLength = password == null ? 0 : password.Length;
            Domain = domain;
            DomainLength = domain == null ? 0 : domain.Length;

            // Flags are 2 for Unicode and 1 for ANSI. We use 2 on NT
            Flags = 2;

            if (null == additionalPackages)
            {
                PackageList = null;
                PackageListLength = 0;
            }
            else
            {
                PackageList = String.Join(",", additionalPackages);
                PackageListLength = PackageList.Length;
            }
        }

    }

    public static Exception CreateException(long errorCode, string defaultMessage)
    {
        string message = defaultMessage + " ERROR:(" + errorCode + ")";
        switch ((uint)errorCode)
        {
            case SEC_E_BUFFER_TOO_SMALL:
                message = "The message buffer is too small. Used with the Digest SSP.";
                break;
            case SEC_E_CONTEXT_EXPIRED:
                message = "The application is referencing a context that has already been closed.";
                break;
            case SEC_E_CRYPTO_SYSTEM_INVALID:
                message = "The cipher chosen for the security context is not supported. Used with the Digest SSP.";
                break;
            case SEC_E_INCOMPLETE_MESSAGE:
                message = "The data in the input buffer is incomplete.";
                break;
            case SEC_E_INSUFFICENT_MEMORY:
                message = "There is not enough memory available to complete the requested action.";
                break;
            case SEC_E_INTERNAL_ERROR:
                message = "An error occurred that did not map to an SSPI error code.";
                break;
            case SEC_E_INVALID_HANDLE:
                message = "The handle passed to the function is not valid.";
                break;
            case SEC_E_INVALID_TOKEN:
                message = "The input token is malformed. Possible causes include a token corrupted in transit, a token of incorrect size, and a token passed into the wrong security package. This last condition can happen if the client and server did not negotiate the proper security package.";
                break;
            case SEC_E_LOGON_DENIED:
                message = "The logon failed.";
                break;
            case SEC_E_MESSAGE_ALTERED:
                message = "The message has been altered. Used with the Digest and Schannel SSPs.";
                break;
            case SEC_E_NO_AUTHENTICATING_AUTHORITY:
                message = "No authority could be contacted for authentication. The domain name of the authenticating party could be wrong, the domain could be unreachable, or there might have been a trust relationship failure.";
                break;
            case SEC_E_NO_CREDENTIALS:
                message = "No credentials are available in the security package.";
                break;
            case SEC_E_OUT_OF_SEQUENCE:
                message = "The message was not received in the correct sequence.";
                break;
            case SEC_E_QOP_NOT_SUPPORTED:
                message = "Neither confidentiality nor integrity are supported by the security context. Used with the Digest SSP.";
                break;
            case SEC_E_SECPKG_NOT_FOUND:
                message = "The requested security package does not exist.";
                break;
            case SEC_E_TARGET_UNKNOWN:
                message = "The target was not recognized.";
                break;
            case SEC_E_UNKNOWN_CREDENTIALS:
                message = "The credentials supplied to the package were not recognized.";
                break;
            case SEC_I_CONTEXT_EXPIRED:
                message = "The message sender has finished using the connection and has initiated a shutdown.";
                break;
            case SEC_I_RENEGOTIATE:
                message = "The remote party requires a new handshake sequence or the application has just initiated a shutdown.";
                break;
        }

        return new Exception(message);
    }
}
