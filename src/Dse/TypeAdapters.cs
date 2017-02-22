//
//  Copyright (C) 2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

namespace Dse
{
    /// <summary>
    /// TypeAdapters are deprecated and will be removed in future versions. Use <see cref="Cassandra.Serialization.TypeSerializer{T}"/> instead.
    /// <para>
    /// Backwards compatibility only.
    /// </para>
    /// </summary>
    public static class TypeAdapters
    {
        public static ITypeAdapter DecimalTypeAdapter = new DecimalTypeAdapter();
        public static ITypeAdapter VarIntTypeAdapter = new BigIntegerTypeAdapter();
        public static ITypeAdapter CustomTypeAdapter = new DynamicCompositeTypeAdapter();
    }
}
