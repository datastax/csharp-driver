using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;

namespace Dse.Geometry
{
    /// <summary>
    /// The driver-side representation for a DSE geospatial type.
    /// </summary>
    [Serializable]
    public abstract class GeometryBase : ISerializable
    {
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
        /// When overridden, sets the serialization info.
        /// </summary>
        public abstract void GetObjectData(SerializationInfo info, StreamingContext context);
    }
}
