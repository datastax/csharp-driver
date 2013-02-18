using System;
using System.Diagnostics;

// based on https://github.com/managedfusion/fluentcassandra/blob/master/src/GuidGenerator.cs


namespace Cassandra
{
	/// <summary>
	/// Used for generating UUID based on RFC 4122.
	/// </summary>
	/// <seealso href="http://www.ietf.org/rfc/rfc4122.txt">RFC 4122 - A Universally Unique IDentifier (UUID) URN Namespace</seealso>
	public static class GuidGenerator
	{
		// number of bytes in guid
		public const int ByteArraySize = 16;

		// multiplex variant info
		public const int VariantByte = 8;
		public const int VariantByteMask = 0x3f;
		public const int VariantByteShift = 0x80;

		// multiplex version info
		public const int VersionByte = 7;
		public const int VersionByteMask = 0x0f;
		public const int VersionByteShift = 4;

		// indexes within the uuid array for certain boundaries
		private const byte TimestampByte = 0;
		private const byte GuidClockSequenceByte = 8;
		private const byte NodeByte = 10;

		// offset to move from 1/1/0001, which is 0-time for .NET, to gregorian 0-time of 10/15/1582
		private static readonly DateTimeOffset GregorianCalendarStart = new DateTimeOffset(1582, 10, 15, 0, 0, 0, TimeSpan.Zero);

		// random clock sequence and node
		public static byte[] DefaultClockSequence { get; set; }
		public static byte[] DefaultNode { get; set; }

		static GuidGenerator()
		{
			DefaultClockSequence = new byte[2];
			DefaultNode = new byte[6];

			var random = new Random();
			random.NextBytes(DefaultClockSequence);
			random.NextBytes(DefaultNode);
		}

		public static GuidVersion GetVersion(Guid guid)
		{
			byte[] bytes = guid.ToByteArray();
			return (GuidVersion)((bytes[VersionByte] & 0xFF) >> VersionByteShift);
		}

        public static long GetTimestamp(Guid guid)
        {
            byte[] bytes = guid.ToByteArray();

            // reverse the version
            bytes[VersionByte] &= (byte)VersionByteMask;
            bytes[VersionByte] |= (byte)((byte)GuidVersion.TimeBased >> VersionByteShift);

            byte[] timestampBytes = new byte[8];
            Array.Copy(bytes, TimestampByte, timestampBytes, 0, 8);

            return BitConverter.ToInt64(timestampBytes, 0);
        }

		public static DateTimeOffset GetDateTimeOffset(Guid guid)
		{
            long ticks = GetTimestamp(guid) + GregorianCalendarStart.Ticks;
			return new DateTimeOffset(ticks, TimeSpan.Zero);
		}

		public static DateTime GetDateTime(Guid guid)
		{
			return GetDateTimeOffset(guid).DateTime;
		}

		public static DateTime GetLocalDateTime(Guid guid)
		{
			return GetDateTimeOffset(guid).LocalDateTime;
		}

		public static DateTime GetUtcDateTime(Guid guid)
		{
			return GetDateTimeOffset(guid).UtcDateTime;
		}

		public static Guid GenerateTimeBasedGuid()
		{
			return GenerateTimeBasedGuid(DateTimePrecise.UtcNowOffset, DefaultClockSequence, DefaultNode);
		}

		public static Guid GenerateTimeBasedGuid(DateTime dateTime)
		{
			return GenerateTimeBasedGuid(dateTime, DefaultClockSequence, DefaultNode);
		}

		public static Guid GenerateTimeBasedGuid(DateTimeOffset dateTime)
		{
			return GenerateTimeBasedGuid(dateTime, DefaultClockSequence, DefaultNode);
		}

		public static Guid GenerateTimeBasedGuid(DateTime dateTime, byte[] clockSequence, byte[] node)
		{
			return GenerateTimeBasedGuid(new DateTimeOffset(dateTime), clockSequence, node);
		}

		public static Guid GenerateTimeBasedGuid(DateTimeOffset dateTime, byte[] clockSequence, byte[] node)
		{
			if (clockSequence == null)
				throw new ArgumentNullException("clockSequence");

			if (node == null)
				throw new ArgumentNullException("node");

			if (clockSequence.Length != 2)
				throw new ArgumentOutOfRangeException("clockSequence", "The clockSequence must be 2 bytes.");

			if (node.Length != 6)
				throw new ArgumentOutOfRangeException("node", "The node must be 6 bytes.");

			long ticks = (dateTime - GregorianCalendarStart).Ticks;
			byte[] guid = new byte[ByteArraySize];
			byte[] timestamp = BitConverter.GetBytes(ticks);

			// copy node
			Array.Copy(node, 0, guid, NodeByte, Math.Min(6, node.Length));

			// copy clock sequence
			Array.Copy(clockSequence, 0, guid, GuidClockSequenceByte, Math.Min(2, clockSequence.Length));

			// copy timestamp
			Array.Copy(timestamp, 0, guid, TimestampByte, Math.Min(8, timestamp.Length));

			// set the variant
			guid[VariantByte] &= (byte)VariantByteMask;
			guid[VariantByte] |= (byte)VariantByteShift;

			// set the version
			guid[VersionByte] &= (byte)VersionByteMask;
			guid[VersionByte] |= (byte)((byte)GuidVersion.TimeBased << VersionByteShift);

			return new Guid(guid);
		}
	}
}
