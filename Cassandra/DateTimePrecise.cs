using System;
using System.Diagnostics;

namespace Cassandra
{
	/// DateTimePrecise class in C# -- an improvement to DateTime.Now
	/// By jamesdbrock
	/// http://www.codeproject.com/KB/cs/DateTimePrecise.aspx
	/// Licensed via The Code Project Open License (CPOL) 1.02
	/// http://www.codeproject.com/info/cpol10.aspx
	/// 
	/// DateTimePrecise provides a way to get a DateTime that exhibits the
	/// relative precision of
	/// System.Diagnostics.Stopwatch, and the absolute accuracy of DateTime.Now.
	public class DateTimePrecise
	{
		private static readonly DateTimePrecise PrivateHelperInstance = new DateTimePrecise(10);

		/// <summary>Returns the current date and time, just like DateTime.Now.</summary>
		public static DateTime Now
		{
			get { return PrivateHelperInstance.GetNow(); }
		}

		/// <summary>Returns the current date and time, just like DateTime.UtcNow.</summary>
		public static DateTime UtcNow
		{
			get { return PrivateHelperInstance.GetUtcNow(); }
		}

		/// <summary>Returns the current date and time, just like DateTimeOffset.Now.</summary>
		public static DateTimeOffset NowOffset
		{
			get { return PrivateHelperInstance.GetNowOffset(); }
		}

		/// <summary>Returns the current date and time, just like DateTimeOffset.UtcNow.</summary>
		public static DateTimeOffset UtcNowOffset
		{
			get { return PrivateHelperInstance.GetUtcNowOffset(); }
		}

		private const long ClockTickFrequency = 10000000;

		private readonly Stopwatch _stopwatch;
		private readonly long _synchronizePeriodStopwatchTicks;
		private DateTimePreciseSafeImmutable _immutable;

		/// <summary>Creates a new instance of DateTimePrecise.
		/// A large value of synchronizePeriodSeconds may cause arithmetic overthrow
		/// exceptions to be thrown. A small value may cause the time to be unstable.
		/// A good value is 10.
		/// synchronizePeriodSeconds = The number of seconds after which the
		/// DateTimePrecise will synchronize itself with the system clock.</summary>
		public DateTimePrecise(long synchronizePeriodSeconds)
		{
			_stopwatch = Stopwatch.StartNew();
			_stopwatch.Start();

			var utc = DateTime.UtcNow;
			_immutable = new DateTimePreciseSafeImmutable(utc, utc, _stopwatch.ElapsedTicks, Stopwatch.Frequency);

			_synchronizePeriodStopwatchTicks = synchronizePeriodSeconds * Stopwatch.Frequency;
		}

		/// <summary>Returns the current date and time, just like DateTime.UtcNow.</summary>
		public DateTime GetUtcNow()
		{
			var stopWatchTicks = _stopwatch.ElapsedTicks;
			var immutable = _immutable;

			if (stopWatchTicks < immutable.ObservedTicks + _synchronizePeriodStopwatchTicks)
			{
				return immutable.BaseTime.AddTicks(((
					stopWatchTicks - immutable.ObservedTicks) * ClockTickFrequency) / (immutable.StopWatchFrequency));
			}
			else
			{
				var utc = DateTime.UtcNow;

				DateTime tBaseNew = immutable.BaseTime.AddTicks(((stopWatchTicks - immutable.ObservedTicks) * ClockTickFrequency) / (immutable.StopWatchFrequency));

				_immutable = new DateTimePreciseSafeImmutable(
					utc,
					tBaseNew,
					stopWatchTicks,
					((stopWatchTicks - immutable.ObservedTicks) * ClockTickFrequency * 2) / (3 * utc.Ticks - 2 * immutable.ObservedTime.Ticks - tBaseNew.Ticks));

				return tBaseNew;
			}
		}

		/// <summary>Returns the current date and time, just like DateTime.Now.</summary>
		public DateTime GetNow()
		{
			return GetUtcNow().ToLocalTime();
		}

		/// <summary>Returns the current date and time, just like DateTimeOffset.UtcNow.</summary>
		public DateTimeOffset GetUtcNowOffset()
		{
			return new DateTimeOffset(GetUtcNow());
		}

		/// <summary>Returns the current date and time, just like DateTimeOffset.Now.</summary>
		public DateTimeOffset GetNowOffset()
		{
			return new DateTimeOffset(GetNow());
		}

		private struct DateTimePreciseSafeImmutable
		{
			public DateTimePreciseSafeImmutable(DateTime observedTime, DateTime baseTime, long observedTicks, long stopWatchFrequency)
			{
				ObservedTime = observedTime;
				BaseTime = baseTime;
				ObservedTicks = observedTicks;
				StopWatchFrequency = Math.Max(1, stopWatchFrequency);
			}

			public readonly DateTime ObservedTime;
			public readonly DateTime BaseTime;
			public readonly long ObservedTicks;
			public readonly long StopWatchFrequency;
		}
	}
}