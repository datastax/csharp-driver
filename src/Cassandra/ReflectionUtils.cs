using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace Cassandra
{
    internal static class ReflectionUtils
    {
        /// <summary>
        /// Returns an array of custom attributes applied to this member.
        /// </summary>
        public static object[] GetCustomAttributesLocal(this Type member, Type attributeType, bool inherit)
        {
#if !NETCORE
            return member.GetCustomAttributes(attributeType, inherit);
#else
            return (object[])member.GetTypeInfo().GetCustomAttributes(attributeType, inherit);
#endif
        }

        /// <summary>
        /// Returns a custom attribute applied to this member or null.
        /// </summary>
        public static object GetCustomAttributeLocal(this Type member, Type attributeType, bool inherit)
        {
            return member.GetCustomAttributesLocal(attributeType, inherit).FirstOrDefault();
        }

        /// <summary>
        /// Gets a value indicating whether the type is generic
        /// </summary>
        public static bool IsGenericTypeLocal(this Type type)
        {
#if !NETCORE
            return type.IsGenericType;
#else
            return type.GetTypeInfo().IsGenericType;
#endif
        }

        /// <summary>
        /// Gets a value indicating whether the type is an interface; that is, not a class or a value type.
        /// </summary>
        public static bool IsInterfaceLocal(this Type type)
        {
#if !NETCORE
            return type.IsInterface;
#else
            return type.GetTypeInfo().IsInterface;
#endif
        }

        /// <summary>
        /// Gets a value indicating whether the type is an enumeration.
        /// </summary>
        public static bool IsEnumLocal(this Type type)
        {
#if !NETCORE
            return type.IsEnum;
#else
            return type.GetTypeInfo().IsEnum;
#endif
        }

        /// <summary>
        /// Gets a value indicating whether the type is assignable from another type.
        /// </summary>
        public static bool IsAssignableFromLocal(this Type type, Type other)
        {
#if !NETCORE
            return type.IsAssignableFrom(other);
#else
            return type.GetTypeInfo().IsAssignableFrom(other);
#endif
        }

        /// <summary>
        /// Gets an array of types representing the type argument of a generic type.
        /// </summary>
        public static Type[] GetGenericArgumentsLocal(this Type type)
        {
#if !NETCORE
            return type.GetGenericArguments();
#else
            return type.GetTypeInfo().GetGenericArguments();
#endif
        }

        /// <summary>
        /// Gets a type interface by name
        /// </summary>
        public static Type GetInterfaceLocal(this Type type, string name)
        {
#if !NETCORE
            return type.GetInterface(name);
#else
            return type.GetTypeInfo().GetInterface(name);
#endif
        }

        /// <summary>
        /// Gets the interfaces implemented by a given type
        /// </summary>
        public static Type[] GetInterfacesLocal(this Type type)
        {
#if !NETCORE
            return type.GetInterfaces();
#else
            return type.GetTypeInfo().GetInterfaces();
#endif
        }

        /// <summary>
        /// Gets the attributes of a given type
        /// </summary>
        public static TypeAttributes GetAttributesLocal(this Type type)
        {
#if !NETCORE
            return type.Attributes;
#else
            return type.GetTypeInfo().Attributes;
#endif
        }

        /// <summary>
        /// Gets a property by name
        /// </summary>
        public static PropertyInfo GetPropertyLocal(this Type type, string name)
        {
#if !NETCORE
            return type.GetProperty(name);
#else
            return type.GetTypeInfo().GetProperty(name);
#endif
        }

        /// <summary>
        /// Returns true if the type has the given attribute defined.
        /// </summary>
        public static bool IsAttributeDefinedLocal(this Type type, Type attributeType, bool inherit)
        {
#if !NETCORE
            return Attribute.IsDefined(type, attributeType, inherit);
#else
            return type.GetTypeInfo().IsDefined(attributeType, inherit);
#endif
        }

        /// <summary>
        /// Returns true if the type has the given attribute defined.
        /// </summary>
        public static bool IsSubclassOfLocal(this Type type, Type other)
        {
#if !NETCORE
            return type.IsSubclassOf(other);
#else
            return type.GetTypeInfo().IsSubclassOf(other);
#endif
        }
    }
}
