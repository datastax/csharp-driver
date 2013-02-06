using System;

namespace Cassandra
{
	// guid version types
	public enum GuidVersion
	{
		TimeBased = 0x01,
		Reserved = 0x02,
		NameBased = 0x03,
		Random = 0x04
	}
}
