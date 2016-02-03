using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Dse.Graph
{
    /// <summary>
    /// Represents an item of a graph query result, it can be a vertex, an edge or an scalar value.
    /// </summary>
    public class GraphResult : DynamicObject, IEquatable<GraphResult>
    {
        private readonly string _json;
        private readonly dynamic _parsedGraphItem;

        /// <summary>
        /// Creates a new instance of <see cref="GraphResult"/>.
        /// </summary>
        /// <param name="json">The graph string json with the form: "{\"result\": ...}".</param>
        public GraphResult(string json)
        {
            if (json == null)
            {
                throw new ArgumentNullException("json");
            }
            _json = json;
            dynamic parsedJson = JsonConvert.DeserializeObject(json);
            Console.WriteLine(parsedJson.result.GetType());
            _parsedGraphItem = parsedJson.result;
        }

        /// <summary>
        /// Gets the typed value of a property of the result.
        /// </summary>
        /// <typeparam name="T">Type of the property. Use dynamic for object trees.</typeparam>
        /// <param name="propertyName">Name of the property.</param>
        public T Get<T>(string propertyName)
        {
            var type = typeof(T);
            var value = GetValue(propertyName);
            if (value == null && default(T) != null)
            {
                throw new NullReferenceException(string.Format("Cannot convert null to {0} because it is a value type, try using Nullable<{0}>", type.Name));
            }
            return (T)value;
        }

        private object GetValue(string name)
        {
            if (!(_parsedGraphItem is JObject))
            {
                if (_parsedGraphItem is JValue)
                {
                    throw new KeyNotFoundException("Cannot retrieve properties of scalar value of type '{0}'" + ((JValue)_parsedGraphItem).Type);
                }
                throw new KeyNotFoundException("Cannot retrieve properties of scalar value");
            }
            var result = _parsedGraphItem[name];
            if (result != null)
            {
                if (result is JObject)
                {
                    //is a object tree
                    return ToExpando((JObject)result);
                }
                return ((JValue)result).Value;
            }
            throw new KeyNotFoundException(string.Format("Graph result has no top-level property '{0}'", name));
        }

        private ExpandoObject ToExpando(JObject jsonNode)
        {
            throw new NotImplementedException();
        }

        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            result = GetValue(binder.Name);
            return true;
        }

        public bool Equals(GraphResult other)
        {
            if (ReferenceEquals(this, other))
            {
                return true;
            }
            return _json == other._json;
        }

        public override bool Equals(object obj)
        {
            return Equals(obj as GraphResult);
        }

        public override int GetHashCode()
        {
            return _json.GetHashCode();
        }

        private object GetScalarValue()
        {
            var jsonValue = _parsedGraphItem as JValue;
            if (jsonValue == null)
            {
                throw new InvalidOperationException("Cannot retrieve an scalar value from result");
            }
            return jsonValue.Value;
        }

        /// <summary>
        /// Returns the representation of the result as a double.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// It throws an InvalidOperationException when the internal value is not an scalar.
        /// </exception>
        /// <exception cref="InvalidCastException">When the scalar value is not convertible to target type.</exception>
        public double ToDouble()
        {
            return Convert.ToDouble(GetScalarValue());
        }

        /// <summary>
        /// Returns the representation of the result as an int.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// It throws an InvalidOperationException when the internal value is not an scalar.
        /// </exception>
        /// <exception cref="InvalidCastException">When the scalar value is not convertible to target type.</exception>
        public int ToInt32()
        {
            return Convert.ToInt32(GetScalarValue());
        }

        /// <summary>
        /// Returns the representation of the result as a boolean.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// It throws an InvalidOperationException when the internal value is not an scalar.
        /// </exception>
        /// <exception cref="InvalidCastException">When the scalar value is not convertible to target type.</exception>
        public bool ToBoolean()
        {
            return Convert.ToBoolean(GetScalarValue());
        }

        /// <summary>
        /// Returns the json representation of the result.
        /// </summary>
        public override string ToString()
        {
            return _parsedGraphItem.ToString();
        }

        public static bool operator ==(GraphResult result1, GraphResult result2)
        {
            if (ReferenceEquals(result1, result2))
            {
                return true;
            }
            if (((object)result1 == null) || ((object)result2 == null))
            {
                return false;
            }
            return result1.Equals(result2);
        }

        public static bool operator !=(GraphResult result1, GraphResult result2)
        {
            return !(result1 == result2);
        }
    }
}
