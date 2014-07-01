using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

namespace Cassandra
{
    /// <summary>
    /// Represents a map between a UDT in Cassandra and a .NET Type, including data on how to map from UDT field names
    /// to Properties on the .NET Type.
    /// </summary>
    /// <typeparam name="T">The .NET Type to map the UDT to.</typeparam>
    public class UdtMap<T> : UdtMap
    {
        private const string NotPropertyMessage = "The expression '{0}' does not refer to a property.";

        internal UdtMap(string udtName)
            : base(typeof(T), udtName)
        {
        }

        /// <summary>
        /// Configures the driver to map the specified property on the .NET Type to the specified UDT field name.
        /// </summary>
        public virtual UdtMap<T> Map<TProperty>(Expression<Func<T, TProperty>> propertyExpression, string udtFieldName)
        {
            var prop = propertyExpression.Body as MemberExpression;
            if (prop == null)
            {
                throw new ArgumentException(string.Format(NotPropertyMessage, propertyExpression));
            }

            var propInfo = prop.Member as PropertyInfo;
            if (propInfo == null)
            {
                throw new ArgumentException(string.Format(NotPropertyMessage, propertyExpression));
            }

            AddPropertyMapping(propInfo, udtFieldName);

            // Allow chaining
            return this;
        }

        public virtual UdtMap<T> SetIgnoreCase(bool value)
        {
            IgnoreCase = value;
            return this;
        }
    }

    /// <summary>
    /// Represents a map between a user defined type in Cassandra and a .NET Type, with data on how
    /// to map field names in the UDT to .NET property names.
    /// </summary>
    public abstract class UdtMap
    {
        private readonly Dictionary<string, PropertyInfo> _fieldNameToProperty;
        private readonly Dictionary<PropertyInfo, string> _propertyToFieldName;
        protected const BindingFlags PropertyFlags = BindingFlags.FlattenHierarchy | BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance;

        protected internal Type NetType { get; protected set; }

        protected internal string UdtName { get; protected set; }

        protected internal bool IgnoreCase { get; set; }

        protected UdtMap(Type netType, string udtName)
        {
            if (netType == null)
            {
                throw new ArgumentNullException("netType");
            }
            NetType = netType;
            UdtName = string.IsNullOrWhiteSpace(udtName) ? NetType.Name : udtName;
            IgnoreCase = true;

            _fieldNameToProperty = new Dictionary<string, PropertyInfo>();
            _propertyToFieldName = new Dictionary<PropertyInfo, string>();
        }

        protected void AddPropertyMapping(PropertyInfo propInfo, string udtFieldName)
        {
            if (_fieldNameToProperty.ContainsKey(udtFieldName))
            {
                throw new ArgumentException(string.Format("A mapping has already been defined for '{0}'.", udtFieldName));
            }

            if (_propertyToFieldName.ContainsKey(propInfo))
            {
                throw new ArgumentException(string.Format("A mapping has already been defined for property '{0}'", propInfo.Name));
            }

            if (propInfo.CanRead == false || propInfo.CanWrite == false)
            {
                throw new ArgumentException(string.Format("Must be able to read and write to property '{0}'.", propInfo.Name));
            }

            _fieldNameToProperty.Add(udtFieldName, propInfo);
            _propertyToFieldName.Add(propInfo, udtFieldName);
        }

        /// <summary>
        /// Gets the UDT field name for a given property.
        /// </summary>
        protected internal string GetUdtFieldName(PropertyInfo property)
        {
            // See if there is a mapping registered for the specific property
            string fieldName;
            if (_propertyToFieldName.TryGetValue(property, out fieldName))
            {
                return fieldName;
            }

            // Just use the property's name
            return property.Name;
        }

        /// <summary>
        /// Gets the PropertyInfo that corresponds to the specified UDT field name.
        /// </summary>
        protected internal PropertyInfo GetPropertyForUdtField(string udtFieldName)
        {
            // See if there is a mapping registered for the field
            PropertyInfo prop;
            if (_fieldNameToProperty.TryGetValue(udtFieldName, out prop))
            {
                return prop;
            }

            string propertyName = udtFieldName;

            // Try to find a property with the UDT field name on type T
            prop = NetType.GetProperty(propertyName, PropertyFlags);
            if (prop == null)
            {
                return null;
            }

            // Make sure that the property we found wasn't one that was individually mapped
            return _propertyToFieldName.ContainsKey(prop) ? null : prop;
        }

        /// <summary>
        /// Creates a new UdtMap for the specified .NET type, optionally mapped to the specified UDT name.
        /// </summary>
        public static UdtMap<T> For<T>(string udtName = null)
        {
            return new UdtMap<T>(udtName);
        }

        /// <summary>
        /// Deserializes a byte array into a object of type defined in the map
        /// </summary>
        public virtual object Decode(byte[] value)
        {
            throw new NotImplementedException();
        }
    }
}