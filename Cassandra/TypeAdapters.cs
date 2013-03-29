using System;
using System.Collections.Generic;
using System.Text;
#if NET_40_OR_GREATER
using System.Numerics;
using System.IO;
#endif

namespace Cassandra
{
    public interface ITypeAdapter
    {
        Type GetDataType();
        object ConvertFrom(byte[] decimalBuf);
        byte[] ConvertTo(object value);
    }

    public static class TypeAdapters
    {
        public static ITypeAdapter DecimalTypeAdapter = new DecimalTypeAdapter();
#if NET_40_OR_GREATER
        public static ITypeAdapter VarIntTypeAdapter = new BigIntegerTypeAdapter();
#else
        public static ITypeAdapter VarIntTypeAdapter = new NullTypeAdapter();
#endif
    }

    public class NullTypeAdapter : ITypeAdapter
    {
        public Type GetDataType()
        {
            return typeof(byte[]);
        }

        public object ConvertFrom(byte[] decimalBuf)
        {
            return decimalBuf;
        }

        public byte[] ConvertTo(object value)
        {
            TypeInterpreter.CheckArgument<byte[]>(value);
            return (byte[])value;
        }
    }

    public class DecimalTypeAdapter : ITypeAdapter
    {
        public Type GetDataType()
        {
            return typeof(decimal);
        }

        public object ConvertFrom(byte[] decimalBuf)
        {
            if (decimalBuf.Length > 16)
                throw new ArgumentOutOfRangeException("this java.math.BigDecimal is too big to fit into System.Decimal. Think about using other TypeAdapter for java.math.BigDecimal (e.g. J#, IKVM,...)");            
            
            byte[] scaleBytes = new byte[4];
            for(int i = 0; i < 4 ; i++)
                scaleBytes[i] = decimalBuf[i];

            byte[] bigIntBytes = new byte[decimalBuf.Length - 4];
            for(int i = 0; i < bigIntBytes.Length; i++)
                bigIntBytes[i] = decimalBuf[i+4];

            bool isNegative = (bigIntBytes[0] & 0x80) != 0;

            byte[] bytes = new byte[12];
            if (isNegative)
                for (int i = 0; i < 12; i++)
                    bytes[i] = 0xff;

            var offset = 12 - bigIntBytes.Length;
            for (int i = 0; i < bigIntBytes.Length; i++)
                bytes[offset + i] = bigIntBytes[i];
            
            byte[] lowB = new byte[4];
            byte[] midB = new byte[4];
            byte[] highB = new byte[4];

            Array.Copy(bytes, 8, lowB, 0, 4);
            Array.Copy(bytes, 4, midB, 0, 4);
            Array.Copy(bytes, 0, highB, 0, 4);

            Array.Reverse(lowB);
            Array.Reverse(midB);            
            Array.Reverse(highB);

            uint low = BitConverter.ToUInt32(lowB, 0);
            uint mid = BitConverter.ToUInt32(midB, 0);
            uint high = BitConverter.ToUInt32(highB, 0);
            byte scale = (byte)TypeInterpreter.BytesToInt32(scaleBytes, 0);

            if (isNegative)
            {
                low = ~low;
                mid = ~mid;
                high = ~high;
                
                high += (mid == 0xFFFFFFF && low == 0xFFFFFFF) ? 1u : 0;
                mid += (low == 0xFFFFFFF) ? 1u : 0;
                low += 1;
            }               
            return new decimal((int)low, (int)mid, (int)high, isNegative, scale);
        }


        public byte[] ConvertTo(object value)
        {
            TypeInterpreter.CheckArgument<decimal>(value);
            int[] bits = decimal.GetBits((decimal)value);

            byte[] bytes = new byte[16];

            int scale = (bits[3] >> 16) & 31;
            
            byte[] scaleB = BitConverter.GetBytes(scale);
            byte[] lowB = BitConverter.GetBytes(bits[0]);
            byte[] midB = BitConverter.GetBytes(bits[1]);
            byte[] highB = BitConverter.GetBytes(bits[2]);

            Array.Copy(lowB, 0, bytes, 0, 4);
            Array.Copy(midB, 0, bytes, 4, 4); 
            Array.Copy(highB, 0, bytes, 8, 4);
            Array.Copy(scaleB, 0, bytes,12, 4);

            if ((decimal)value < 0)
            {
                for (int i = 0; i < 12; i++)
                    bytes[i] = (byte)~bytes[i];
                bytes[0] += 1;
            }
            Array.Reverse(bytes);
            return bytes;
        }
    }

//    public struct DecimalConverter
//{
//private readonly bool _hasValue;
//private readonly byte[] _unscaled;
//private readonly int _scale;

//public DecimalConverter( decimal value )
//{
//_hasValue = true;
//int[] ints = decimal.GetBits( value );
//byte[] bytes = new byte[12];
//writeInt( ints[0], bytes, 0 );
//writeInt( ints[1], bytes, 4 );
//writeInt( ints[2], bytes, 8 );
//BigInteger unscaled = new BigInteger( bytes );
//BigInteger signed = value < 0 ? -unscaled : unscaled;
//_unscaled = signed.ToByteArray();
//BigIntegerConverter.SwitchEndianess( _unscaled );
//_scale = (ints[3] >> 16) & 0xFF;
//}

//public DecimalConverter( bool hasValue, byte[] unscaled, int scale
//)
//{
//_hasValue = hasValue;
//_unscaled = unscaled;
//_scale = scale;
//}

//public bool HasValue
//{
//get { return _hasValue; }
//}

//public byte[] Unscaled
//{
//get { return _unscaled; }
//}

//public int Scale
//{
//get { return _scale; }
//}

//public static implicit operator decimal?( DecimalConverter value )
//{
//if( !value.HasValue )
//return null;

//BigIntegerConverter.SwitchEndianess( value._unscaled );
//BigInteger signed = new BigInteger( value._unscaled );
//BigInteger unscaled = signed < 0 ? -signed : signed;
//int scale = value._scale;

//if( scale < 0 )
//{
//unscaled = unscaled * BigInteger.Pow( BigInteger.One,
//-scale + 1 );
//scale = 0;
//}

//byte[] bytes = unscaled.ToByteArray();

//if( bytes.Length > 12 || scale > 28 )
//{
//// Throwing would break the source, e.g. close the network
//connection
//DotNetLog.write( new OverflowException( "Java BigDecimal
//was too large for .NET decimal" ) );
//return null;
//}

//byte[] twelve = new byte[12];
//Array.Copy( bytes, 0, twelve, twelve.Length - bytes.Length,
//bytes.Length );
//return new decimal( readInt( twelve, 0 ), readInt( twelve, 4 ),
//readInt( twelve, 8 ), signed.Sign < 0, (byte) scale );
//}

//public static implicit operator DecimalConverter( decimal? value )
//{
//if( !value.HasValue )
//return new DecimalConverter( false, null, 0 );

//return new DecimalConverter( value.Value );
//}

//private static int readInt( byte[] array, int index )
//{
//int b0 = array[index + 0] << 24;
//int b1 = array[index + 1] << 16;
//int b2 = array[index + 2] << 8;
//int b3 = array[index + 3] << 0;
//return b3 | b2 | b1 | b0;
//}

//private static void writeInt( int value, byte[] array, int index )
//{
//array[index + 0] = ((byte) ((value >> 0) & 0xFF));
//array[index + 1] = ((byte) ((value >> 8) & 0xFF));
//array[index + 2] = ((byte) ((value >> 16) & 0xFF));
//array[index + 3] = ((byte) ((value >> 24) & 0xFF));
//}
//    }



#if NET_40_OR_GREATER
    public class BigIntegerTypeAdapter : ITypeAdapter
    {
        public Type GetDataType()
        {
            return typeof(BigInteger);
        }

        public object ConvertFrom(byte[] decimalBuf)
        {
            return new BigInteger(decimalBuf);
        }

        public byte[] ConvertTo(object value)
        {
            TypeInterpreter.CheckArgument<BigInteger>(value);
            return ((BigInteger)value).ToByteArray();
        }
    }
#endif
}
