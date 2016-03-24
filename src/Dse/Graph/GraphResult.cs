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
            _parsedGraphItem = parsedJson.result;
        }

        private GraphResult(dynamic parsedGraphItem)
        {
            if (parsedGraphItem == null)
            {
                throw new ArgumentNullException("parsedGraphItem");
            }
            _parsedGraphItem = parsedGraphItem;
            _json = _parsedGraphItem.ToString();
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
            if (!(value is T))
            {
                value = Convert.ChangeType(value, typeof (T));
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
                if (result is JArray)
                {
                    //is an array
                    return ToArray((JArray)result);
                }
                return ((JValue)result).Value;
            }
            throw new KeyNotFoundException(string.Format("Graph result has no top-level property '{0}'", name));
        }

        private object GetTokenValue(JToken token)
        {
            if (token is JValue)
            {
                return ((JValue)token).Value;
            }
            if (token is JObject)
            {
                return ToExpando((JObject)token);
            }
            if (token is JArray)
            {
                return ToArray((JArray)token);
            }
            throw new NotSupportedException(string.Format("Token of type {0} is not supported", token.GetType()));
        }

        /// <summary>
        /// Provides the implementation for operations that get member values.
        /// </summary>
        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            result = GetValue(binder.Name);
            return true;
        }

        /// <summary>
        /// Returns true if the value represented by this instance is the same.
        /// </summary>
        public bool Equals(GraphResult other)
        {
            if (ReferenceEquals(this, other))
            {
                return true;
            }
            return _json == other._json;
        }

        /// <summary>
        /// Returns true if the value represented by this instance is the same.
        /// </summary>
        public override bool Equals(object obj)
        {
            return Equals(obj as GraphResult);
        }

        /// <summary>
        /// Gets the hash code for this instance, based on its value.
        /// </summary>
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

        private dynamic[] ToArray(JArray jArray)
        {
            var arr = new dynamic[jArray.Count];
            for (var i = 0; i < arr.Length; i++)
            {
                arr[i] = GetTokenValue(jArray[i]);
            }
            return arr;
        }

        /// <summary>
        /// Converts the instance into an array when the internal representation is a json array.
        /// </summary>
        public dynamic[] ToArray()
        {
            if (!(_parsedGraphItem is JArray))
            {
                throw new InvalidOperationException(string.Format("Cannot convert to array from {0}", _json));
            }
            return ToArray(_parsedGraphItem as JArray);
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

        private ExpandoObject ToExpando(JObject jsonNode)
        {
            var expando = new ExpandoObject();
            var dictionary = (IDictionary<string, object>)expando;
            foreach (var property in jsonNode)
            {
                dictionary[property.Key] = GetTokenValue(property.Value);
            }
            return expando;
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
        /// Returns an edge representation of the current instance.
        /// </summary>
        public Edge ToEdge()
        {
            if (!(_parsedGraphItem is JObject))
            {
                throw new InvalidOperationException(string.Format("Cannot create an Edge from {0}", _json));
            }
            var properties = new Dictionary<string, GraphResult>();
            foreach (JProperty prop in _parsedGraphItem["properties"])
            {
                properties.Add(prop.Name, new GraphResult(prop.Value));
            }
            return new Edge(
                new GraphResult(_parsedGraphItem.id),
                GetValue("label").ToString(),
                properties,
                new GraphResult(_parsedGraphItem.inV),
                GetValue("inVLabel").ToString(),
                new GraphResult(_parsedGraphItem.outV),
                GetValue("outVLabel").ToString());
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
        /// Returns the json representation of the result.
        /// </summary>
        public override string ToString()
        {
            return _parsedGraphItem.ToString();
        }

        /// <summary>
        /// Returns a vertex representation of the current instance.
        /// </summary>
        public Vertex ToVertex()
        {
            if (!(_parsedGraphItem is JObject))
            {
                throw new InvalidOperationException(string.Format("Cannot create a Vertex from {0}", _json));
            }
            var properties = new Dictionary<string, GraphResult>();
            foreach (JProperty prop in _parsedGraphItem["properties"])
            {
                properties.Add(prop.Name, new GraphResult(prop.Value));
            }
            return new Vertex(
                new GraphResult(_parsedGraphItem.id),
                GetValue("label").ToString(),
                properties);
        }

        /// <summary>
        /// Returns true if the value represented by the instances are the same.
        /// </summary>
        public static bool operator ==(GraphResult result1, GraphResult result2)
        {
            if (ReferenceEquals(result1, result2))
            {
                return true;
            }
            //Cast is needed to prevent a recursive call
            // ReSharper disable RedundantCast.0
            if (((object)result1 == null) || ((object)result2 == null))
            {
                return false;
            }
            // ReSharper enable RedundantCast.0
            return result1.Equals(result2);
        }

        /// <summary>
        /// Compares the values for inequality.
        /// </summary>
        public static bool operator !=(GraphResult result1, GraphResult result2)
        {
            return !(result1 == result2);
        }

        /// <summary>
        /// Converts this instance to a <see cref="Vertex"/>.
        /// </summary>
        public static implicit operator Vertex(GraphResult b)
        {
            return b.ToVertex();
        }

        /// <summary>
        /// Converts this instance to an <see cref="Edge"/>.
        /// </summary>
        public static implicit operator Edge(GraphResult b)
        {
            return b.ToEdge();
        }
    }
}
