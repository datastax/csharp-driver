//
//      Copyright (C) DataStax Inc.
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

using System;
using System.Text.RegularExpressions;

namespace Cassandra
{
    /// <summary>
    /// A date without a time-zone in the ISO-8601 calendar system.
    /// LocalDate is an immutable date-time object that represents a date, often viewed as year-month-day.
    /// This class is implemented to match the Date representation CQL string literals.
    /// </summary>
    public class LocalDate: IComparable<LocalDate>, IEquatable<LocalDate>
    {
        /// <summary>
        /// Day number relatively to the year based on the month index
        /// </summary>
        private static readonly int[] DaysToMonth = { 0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365};
        private static readonly int[] DaysToMonthLeap = {  0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366};
        // unix epoch is represented by the number  2 ^ 31
        private const long DateCenter = 2147483648L;
        private const long DaysFromYear0ToUnixEpoch = 719528L;
        // ReSharper disable once InconsistentNaming
        private const long DaysFromYear0March1stToUnixEpoch = LocalDate.DaysFromYear0ToUnixEpoch - 60L;
        
        private static readonly Regex RegexInteger = new Regex("^-?\\d+$", RegexOptions.Compiled);

        private const string BadFormatMessage =
            "LocalDate must be provided in the yyyy-mm-dd format or in days since epoch";

        /// <summary>
        /// An unsigned integer representing days with epoch centered at 2^31 (unix epoch January 1st, 1970).
        /// </summary>
        internal uint DaysSinceEpochCentered { get; }

        public int Day { get; set; }

        public int Month { get; set; }

        public int Year { get; set; }

        /// <summary>
        /// Creates a new instance based on the days since unix epoch.
        /// </summary>
        /// <param name="days">An unsigned integer representing days with epoch centered at 2^31.</param>
        internal LocalDate(uint days)
        {
            DaysSinceEpochCentered = days;
            InitializeFromDaysSinceEpoch(days - LocalDate.DateCenter);
        }

        /// <summary>
        /// Port from https://github.com/HowardHinnant/date which is based on http://howardhinnant.github.io/date_algorithms.html
        /// There's a good explanation of the algorithm over there.
        /// </summary>
        /// <param name="daysSinceEpoch">Days since Epoch (January 1st, 1970), can be negative.</param>
        private void InitializeFromDaysSinceEpoch(long daysSinceEpoch)
        {
            var daysSinceShiftedEpoch = daysSinceEpoch + LocalDate.DaysFromYear0March1stToUnixEpoch;    // shift epoch from 1970-01-01 to 0000-03-01
            var era =                                                                                   // compute era (400 year period)
                (daysSinceShiftedEpoch >= 0 ? daysSinceShiftedEpoch : daysSinceShiftedEpoch - 146096) 
                / 146097;                                          
            
            var dayOfEra = (daysSinceShiftedEpoch - era * 146097);                                      // [0, 146096]
            var yearOfEra = (dayOfEra - dayOfEra/1460 + dayOfEra/36524 - dayOfEra/146096) / 365;        // [0, 399]
            var dayOfYear = dayOfEra - (365*yearOfEra + yearOfEra/4 - yearOfEra/100);                   // [0, 365]
            var monthInternal = (5*dayOfYear + 2)/153;                                                  // [0, 11]
            var yearInternal = yearOfEra + era * 400;                                                   // beginning in Mar 1st, NOT in Jan 1st

            var day = dayOfYear - (153*monthInternal+2)/5 + 1;                                          // [1, 31]
            var monthCivil = monthInternal + (monthInternal < 10 ? 3 : -9);                             // [1, 12]
            var yearCivil = yearInternal + (monthCivil <= 2 ? 1 : 0);                                   // shift to beginning in Jan 1st

            Year = (int) yearCivil;
            Month = (int) monthCivil;
            Day = (int) day;
        }

        /// <summary>
        /// Creates a new instance of LocalDate
        /// </summary>
        /// <param name="year">Year according to ISO-8601. Year 0 represents 1 BC.</param>
        /// <param name="month">The month number from 1 to 12</param>
        /// <param name="day">A day of the month from 1 to 31.</param>
        public LocalDate(int year, int month, int day)
        {
            if (year > 5881580 || year < -5877641)
            {
                throw new ArgumentOutOfRangeException("year", month, "Year is out of range");
            }
            if (month < 1 || month > 12)
            {
                throw new ArgumentOutOfRangeException("month", month, "Month is out of range");
            }
            if (year == 5881580 && month * 100 + day > 711)
            {
                throw new ArgumentOutOfRangeException("year", "Date is outside the boundaries of a LocalDate representation");
            }
            if (year == -5877641 && month * 100 + day < 623)
            {
                throw new ArgumentOutOfRangeException("year", "Date is outside the boundaries of a LocalDate representation");
            }

            Year = year;
            Month = month;
            Day = day;

            var value =
                DaysSinceYearZero(year) +
                DaysSinceJan1(year, month, day) -
                DaysFromYear0ToUnixEpoch +
                DateCenter + (year > 0 ? 1L : 0L);
            DaysSinceEpochCentered = Convert.ToUInt32(value);
        }

        /// <summary>
        /// Returns the value in days since year zero (1 BC).
        /// </summary>
        /// <param name="year"></param>
        private static long DaysSinceYearZero(int year)
        {
            return (
                //days per year
                year * 365L + 
                //adjusted per leap years
                LeapDays(year));
        }

        /// <summary>
        /// Returns the amount of days since Jan 1, for a given month/day
        /// </summary>
        public static int DaysSinceJan1(int year, int month, int day)
        {
            var daysToMonth = IsLeapYear(year) ? DaysToMonthLeap : DaysToMonth;
            if (day < 1 || day > daysToMonth[month] - daysToMonth[month - 1])
            {
                throw new ArgumentOutOfRangeException("day");
            }
            return (
                //days to the month in the year
                daysToMonth[month - 1]
                //the amount of month days
                + day - 1);
        }

        /// <param name="year">0-based year number: 0 equals to 1 AD</param>
        private static long LeapDays(long year)
        {
            var result = year/4 - year/100 + year/400;
            if (year > 0 && IsLeapYear((int)year))
            {
                result--;
            }
            return result;
        }

        private static bool IsLeapYear(int year)
        {
            return year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
        }

        /// <summary>
        /// Compares this instance value to another and returns an indication of their relative values.
        /// </summary>
        /// <param name="other"></param>
        /// <returns></returns>
        public int CompareTo(LocalDate other)
        {
            if ((object) other == null)
            {
                return 1;
            }
            return DaysSinceEpochCentered.CompareTo(other.DaysSinceEpochCentered);
        }

        /// <summary>
        /// Determines if the value is equal to this instance.
        /// </summary>
        public bool Equals(LocalDate other)
        {
            return CompareTo(other) == 0;
        }

        /// <summary>
        /// Creates a new instance of <see cref="LocalDate"/> using the year, month and day provided in the form:
        /// yyyy-mm-dd or days since epoch (i.e. -1 for Dec 31, 1969).
        /// </summary>
        public static LocalDate Parse(string value)
        {
            if (value == null)
            {
                throw new ArgumentNullException(nameof(value));
            }
            if (RegexInteger.IsMatch(value))
            {
                return new LocalDate(Convert.ToUInt32(DateCenter + long.Parse(value)));
            }
            var parts = value.Split('-');
            if (parts.Length < 3)
            {
                throw new FormatException(BadFormatMessage);
            }
            var sign = parts[0].Length == 0 ? -1 : 1;
            var index = 0;
            if (sign == -1)
            {
                index = 1;
                if (parts.Length != 4)
                {
                    throw new FormatException(BadFormatMessage);
                }
            }
            return new LocalDate(sign * Convert.ToInt32(parts[index]), Convert.ToInt32(parts[index + 1]),
                Convert.ToInt32(parts[index + 2]));
        }

        /// <summary>
        /// Returns the DateTimeOffset representation of the LocalDate for dates between 0001-01-01 and 9999-12-31
        /// </summary>
        public DateTimeOffset ToDateTimeOffset()
        {
            if (Year < 1 || Year > 9999)
            {
                throw new ArgumentOutOfRangeException("The LocalDate can not be converted to DateTimeOffset", (Exception) null);
            }
            return new DateTimeOffset(Year, Month, Day, 0, 0, 0, 0, TimeSpan.Zero);
        }

        /// <summary>
        /// Returns the string representation of the LocalDate in yyyy-MM-dd format
        /// </summary>
        public override string ToString()
        {
            var yearString = Year.ToString();
            if (Year >= 0 && Year < 1000)
            {
                yearString = Utils.FillZeros(Year, 4);
            }
            return yearString + "-" + Utils.FillZeros(Month) + "-" + Utils.FillZeros(Day);
        }

        public static bool operator ==(LocalDate value1, LocalDate value2)
        {
            // If both are null, or both are same instance, return true.
            if (ReferenceEquals(value1, value2))
            {
                return true;
            }
            // If one is null, but not both, return false.
            if (((object)value1 == null) || ((object)value2 == null))
            {
                return false;
            }
            return value1.Equals(value2);
        }

        public static bool operator >=(LocalDate value1, LocalDate value2)
        {
            return value1.CompareTo(value2) >= 0;
        }

        public static bool operator >(LocalDate value1, LocalDate value2)
        {
            return value1.CompareTo(value2) > 0;
        }

        public static bool operator <=(LocalDate value1, LocalDate value2)
        {
            return value1.CompareTo(value2) <= 0;
        }

        public static bool operator <(LocalDate value1, LocalDate value2)
        {
            return value1.CompareTo(value2) < 0;
        }

        public static bool operator !=(LocalDate value1, LocalDate value2)
        {
            // If both are null, or both are same instance, return false.
            if (ReferenceEquals(value1, value2))
            {
                return false;
            }
            // If one is null, but not both, return true.
            if (((object)value1 == null) || ((object)value2 == null))
            {
                return true;
            }
            return !value1.Equals(value2);
        }

        public override int GetHashCode()
        {
            return DaysSinceEpochCentered.GetHashCode();
        }

        public override bool Equals(object obj)
        {
            var other = obj as LocalDate;
            return other != null && CompareTo(other) == 0;
        }
    }
}



