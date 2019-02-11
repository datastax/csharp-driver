//
//  Copyright (C) 2016 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//
using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using Dse.Serialization;
using Dse.Serialization.Graph;
using Newtonsoft.Json;

namespace Dse.Geometry
{
    /// <summary>
    /// The driver-side representation for a DSE geospatial type.
    /// </summary>
#if NET45
    [Serializable]
#endif
    public abstract class GeometryBase
#if NET45
        : ISerializable
#endif
    {
        private static readonly JsonSerializer DefaultJsonSerializer = JsonSerializer.CreateDefault();

        /// <summary>
        /// Gets the type name to be used for GeoJSON serialization.
        /// </summary>
        protected virtual string GeoJsonType { get { return GetType().Name; } }

        /// <summary>
        /// Gets the coordinates property for GeoJSON serialization.
        /// </summary>
        protected abstract IEnumerable GeoCoordinates { get; }

        /// <summary>
        /// Checks for null items and returns a read-only collection with an array as underlying list.
        /// </summary>
        protected ReadOnlyCollection<T> AsReadOnlyCollection<T>(IList<T> elements, Func<T, T> itemCallback = null)
        {
            if (itemCallback == null)
            {
                //Use identity function
                itemCallback = t => t;
            }
            var elementsArray = new T[elements.Count];
            for (var i = 0; i < elements.Count; i++)
            {
                var item = elements[i];
                if (item == null)
                {
                    throw new ArgumentException("Collection must not contain null items.");
                }
                elementsArray[i] = itemCallback(item);
            }
            return new ReadOnlyCollection<T>(elementsArray);
        }

        /// <summary>
        /// Combines the hash code based on the value of items.
        /// </summary>
        protected int CombineHashCode<T>(IEnumerable<T> items)
        {
            unchecked
            {
                var hash = 17;
                foreach (var item in items)
                {
                    hash = hash * 23 + item.GetHashCode();   
                }
                return hash;
            }
        }

        /// <summary>
        /// Returns the GeoJSON representation of the instance.
        /// </summary>
        public virtual string ToGeoJson()
        {
            var stringWriter = new StringWriter();
            var writer = new JsonTextWriter(stringWriter);
            WriteJson(writer, DefaultJsonSerializer);
            return stringWriter.ToString();
        }

#if NET45
        /// <inheritdoc />
        public virtual void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            info.AddValue("type", GeoJsonType);
            info.AddValue("coordinates", GeoCoordinates);
        }
#endif

        internal virtual void WriteJson(JsonWriter writer, JsonSerializer serializer)
        {
            writer.WriteStartObject();
            writer.WritePropertyName("type");
            writer.WriteValue(GeoJsonType);
            writer.WritePropertyName("coordinates");
            serializer.Serialize(writer, GeoCoordinates);
            writer.WriteEndObject();
        }

        internal static FormatException InvalidFormatException(string textValue)
        {
            return new FormatException("Format for Geometry type is incorrect: " + textValue);
        }
    }
}
