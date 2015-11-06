//
//      Copyright (C) 2012-2014 DataStax Inc.
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

﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Cassandra
{
    /// <summary>
    /// Represents a map between a UDT in Cassandra and a .NET Type, including data on how to map from UDT field names
    /// to Properties on the .NET Type.
    /// </summary>
    /// <typeparam name="T">The .NET Type to map the UDT to.</typeparam>
    public class UdtMap<T> : UdtMap where T : new()
    {
        private const string NotPropertyMessage = "The expression '{0}' does not refer to a property.";

        internal UdtMap(string udtName)
            : base(typeof(T), udtName)
        {
        }

        /// <summary>
        /// Maps properties by name
        /// </summary>
        public new virtual UdtMap<T> Automap()
        {
            base.Automap();
            return this;
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
        protected readonly Dictionary<string, PropertyInfo> _fieldNameToProperty;
        protected readonly Dictionary<PropertyInfo, string> _propertyToFieldName;
        protected const BindingFlags PropertyFlags = BindingFlags.FlattenHierarchy | BindingFlags.IgnoreCase | BindingFlags.Public | BindingFlags.Instance;

        protected internal Type NetType { get; protected set; }

        protected internal string UdtName { get; protected set; }

        protected internal bool IgnoreCase { get; set; }

        protected internal UdtColumnInfo Definition { get; protected set; }

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

            _fieldNameToProperty[udtFieldName] = propInfo;
            _propertyToFieldName[propInfo] = udtFieldName;
        }

        /// <summary>
        /// Maps properties and fields by name
        /// </summary>
        protected virtual void Automap()
        {
            if (Definition == null)
            {
                throw new ArgumentException("Udt definition not specified");
            }
            //Use auto mapping
            foreach (var field in Definition.Fields)
            {
                var prop = NetType.GetProperty(field.Name, PropertyFlags);
                if (prop != null)
                {
                    AddPropertyMapping(prop, field.Name);
                }
            }
        }

        /// <summary>
        /// Builds the mapping using the Udt definition.
        /// Sets the definition, validates the fields vs the mapped fields.
        /// In case there isn't any property mapped defined, it auto maps the properties by name
        /// </summary>
        protected internal virtual void Build(UdtColumnInfo definition)
        {
            Definition = definition;
            if (_fieldNameToProperty.Count == 0)
            {
                Automap();
            }
            Validate();
        }

        private void Validate()
        {
            //Check that the field type and the property type matches
            foreach (var field in Definition.Fields)
            {
                if (field.TypeCode == ColumnTypeCode.Udt)
                {
                    //We deal with nested UDTs later
                    continue;
                }
                var prop = this.GetPropertyForUdtField(field.Name);
                if (prop == null)
                {
                    //No mapping defined
                    //MAYBE: throw an exception
                    continue;
                }
                //Check if its assignable to and from
                var fieldTargetType = TypeCodec.GetDefaultTypeFromCqlType(field.TypeCode, field.TypeInfo);
                if (!prop.PropertyType.IsAssignableFrom(fieldTargetType))
                {
                    throw new InvalidTypeException(String.Format("{0} type {1} is not assignable to {2}", field.Name, fieldTargetType.Name, prop.PropertyType.Name));
                }
            }
            //Check that there isn't a map to a non existent field
            foreach (var fieldName in _fieldNameToProperty.Keys)
            {
                if (Definition.Fields.All(f => f.Name != fieldName))
                {
                    throw new InvalidTypeException("Mapping defined for not existent field " + fieldName);
                }
            }
        }

        /// <summary>
        /// Creates a new instance of the target .NET type
        /// </summary>
        protected virtual object CreateInstance()
        {
            return Activator.CreateInstance(NetType);
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
        public static UdtMap<T> For<T>(string udtName = null) where T : new()
        {
            return new UdtMap<T>(udtName);
        }

        /// <summary>
        /// Creates a new instance of the mapped object and sets the values
        /// </summary>
        internal object ToObject(object[] values)
        {
            var obj = CreateInstance();
            for (var i = 0; i < Definition.Fields.Count && i < values.Length; i++)
            {
                var field = Definition.Fields[i];
                var prop = GetPropertyForUdtField(field.Name);
                if (prop == null)
                {
                    continue;
                }
                prop.SetValue(obj, values[i], null);
            }
            return obj;
        }
    }
}