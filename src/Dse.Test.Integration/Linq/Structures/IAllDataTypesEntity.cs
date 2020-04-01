//
//  Copyright (C) DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;

namespace Dse.Test.Integration.Linq.Structures
{
    public interface IAllDataTypesEntity
    {
        string StringType { get; set; }
        Guid GuidType { get; set; }
        DateTime DateTimeType { get; set; }
        DateTime? NullableDateTimeType { get; set; }
        DateTimeOffset DateTimeOffsetType { get; set; }
        bool BooleanType { get; set; }
        Decimal DecimalType { get; set; }
        double DoubleType { get; set; }
        float FloatType { get; set; }
        int? NullableIntType { get; set; }
        int IntType { get; set; }
        Int64 Int64Type { get; set; }
        TimeUuid TimeUuidType { get; set; }
        TimeUuid? NullableTimeUuidType { get; set; }
        Dictionary<string, long> DictionaryStringLongType { get; set; }
        Dictionary<string, string> DictionaryStringStringType { get; set; }
        List<Guid> ListOfGuidsType { get; set; }
        List<string> ListOfStringsType { get; set; }

    }
}