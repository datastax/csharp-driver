using System;
using System.Collections;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Runtime.Serialization;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Dse.Graph
{
    /// <summary>
    /// Represents an item of a graph query result, it can be a vertex, an edge, a path or an scalar value.
    /// </summary>
    [Serializable]
    public class GraphNode : DynamicObject, IEquatable<GraphNode>, ISerializable
    {
        private volatile string _json;
        private readonly JToken _parsedGraphItem;

        /// <summary>
        /// Returns true if the underlying value is an array.
        /// </summary>
        public bool IsArray
        {
            get { return _parsedGraphItem is JArray; }
        }

        /// <summary>
        /// Returns true if the underlying value is an object tree.
        /// </summary>
        public bool IsObjectTree
        {
            get { return _parsedGraphItem is JObject; }
        }

        /// <summary>
        /// Returns true if the underlying value is a scalar value (string, double, boolean, ...).
        /// </summary>
        public bool IsScalar
        {
            get { return _parsedGraphItem is JValue; }
        }

        /// <summary>
        /// Creates a new instance of <see cref="GraphNode"/>.
        /// </summary>
        /// <param name="json">The graph string json with the form: "{\"result\": ...}".</param>
        public GraphNode(string json)
        {
            if (json == null)
            {
                throw new ArgumentNullException("json");
            }
            JObject parsedJson = (JObject)JsonConvert.DeserializeObject(json);
            _parsedGraphItem = parsedJson["result"];
        }

        private GraphNode(JToken parsedGraphItem)
        {
            if (parsedGraphItem == null)
            {
                throw new ArgumentNullException("parsedGraphItem");
            }
            _parsedGraphItem = parsedGraphItem;
        }

        /// <summary>
        /// Creates a new instance of <see cref="GraphNode"/> using a serialization information.
        /// </summary>
        protected GraphNode(SerializationInfo info, StreamingContext context)
        {
            var objectTree = new JObject();
            foreach (var field in info)
            {
                if (field.Value is JToken)
                {
                    objectTree.Add(field.Name, (JToken)field.Value);
                    continue;
                }
                if (field.Value is IEnumerable<object>)
                {
                    var values = (IEnumerable<object>) field.Value;
                    objectTree.Add(field.Name, new JArray(values.ToArray()));
                    continue;
                }
                objectTree.Add(field.Name, new JValue(field.Value));
            }
            _parsedGraphItem = objectTree;
        }

        /// <summary>
        /// Gets the typed value of a property of the result.
        /// </summary>
        /// <typeparam name="T">Type of the property. Use dynamic for object trees.</typeparam>
        /// <param name="propertyName">Name of the property.</param>
        public T Get<T>(string propertyName)
        {
            var type = typeof(T);
            var value = GetValue(propertyName, false);
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

        private string GetJson()
        {
            var json = _json;
            if (json != null)
            {
                return json;
            }
            json = _parsedGraphItem.ToString();
            //Converting a JToken to string is an operation we want to avoid
            _json = json;
            return json;
        }

        private object GetValue(string name, bool throwIfNotFound)
        {
            if (!(_parsedGraphItem is JObject))
            {
                if (_parsedGraphItem is JValue)
                {
                    throw new KeyNotFoundException("Cannot retrieve properties of scalar value of type '{0}'" + ((JValue)_parsedGraphItem).Type);
                }
                throw new KeyNotFoundException("Cannot retrieve properties of scalar value");
            }
            var graphObject = (JObject)_parsedGraphItem;
            var property = graphObject.Property(name);
            if (property == null)
            {
                if (throwIfNotFound)
                {
                    throw new KeyNotFoundException(string.Format("Graph result has no top-level property '{0}'", name));   
                }
                return null;
            }
            var value = property.Value;
            if (value == null)
            {
                return null;
            }
            return GetTokenValue(value);
        }

        /// <summary>
        /// Returns either an scalar value, a GraphNode or an Array of GraphNodes.
        /// </summary>
        private object GetTokenValue(JToken token)
        {
            if (token is JValue)
            {
                return ((JValue)token).Value;
            }
            if (token is JObject)
            {
                return new GraphNode(token);
            }
            if (token is JArray)
            {
                return ToArray((JArray)token);
            }
            throw new NotSupportedException(string.Format("Token of type {0} is not supported", token.GetType()));
        }

        /// <summary>
        /// Returns true if the property is defined in this instance.
        /// </summary>
        /// <exception cref="InvalidOperationException">When the underlying value is not an object tree</exception>
        public bool HasProperty(string name)
        {
            if (!(_parsedGraphItem is JObject))
            {
                return false;
            }
            return ((JObject) _parsedGraphItem).Property(name) != null;
        }

        /// <summary>
        /// Provides the implementation for operations that get member values.
        /// </summary>
        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            result = GetValue(binder.Name, false);
            return true;
        }

        /// <summary>
        /// Returns true if the value represented by this instance is the same.
        /// </summary>
        public bool Equals(GraphNode other)
        {
            if (other == null)
            {
                return false;
            }
            if (ReferenceEquals(this, other))
            {
                return true;
            }
            return GetJson() == other.GetJson();
        }

        /// <summary>
        /// Returns true if the value represented by this instance is the same.
        /// </summary>
        public override bool Equals(object obj)
        {
            return Equals(obj as GraphNode);
        }

        /// <summary>
        /// Gets the hash code for this instance, based on its value.
        /// </summary>
        public override int GetHashCode()
        {
            return GetJson().GetHashCode();
        }

        /// <inheritdoc />
        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            if (!IsObjectTree)
            {
                throw new NotSupportedException("Deserialization of GraphNodes that don't represent object trees is not supported");
            }
            foreach (var prop in ((JObject) _parsedGraphItem).Properties())
            {
                info.AddValue(prop.Name, prop.Value);
            }
        }

        /// <summary>
        /// Gets the a dictionary of properties of this node.
        /// </summary>
        public IDictionary<string, GraphNode> GetProperties()
        {
            return GetProperties(_parsedGraphItem);
        }

        private IDictionary<string, GraphNode> GetProperties(JToken item)
        {
            if (!(item is JObject))
            {
                throw new InvalidOperationException(string.Format("Can not get properties from '{0}'", item));
            }
            return ((JObject) item)
                .Properties()
                .ToDictionary(prop => prop.Name, prop => new GraphNode(prop.Value));
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

        private GraphNode[] ToArray(JArray jArray)
        {
            var arr = new GraphNode[jArray.Count];
            for (var i = 0; i < arr.Length; i++)
            {
                arr[i] = new GraphNode(jArray[i]);
            }
            return arr;
        }

        /// <summary>
        /// Converts the instance into an array when the internal representation is a json array.
        /// </summary>
        public GraphNode[] ToArray()
        {
            if (!(_parsedGraphItem is JArray))
            {
                throw new InvalidOperationException(string.Format("Cannot convert to array from {0}", GetJson()));
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
                throw new InvalidOperationException(string.Format("Cannot create an Edge from {0}", GetJson()));
            }
            IDictionary<string, GraphNode> properties = ((JObject)_parsedGraphItem["properties"])
                .Properties()
                .ToDictionary(prop => prop.Name, prop => new GraphNode(prop.Value));
            return new Edge(
                (GraphNode) GetValue("id", true),
                GetValue("label", true).ToString(),
                properties,
                (GraphNode) GetValue("inV", true),
                GetValue("inVLabel", true).ToString(),
                (GraphNode) GetValue("outV", true),
                GetValue("outVLabel", true).ToString());
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
        /// Returns a <see cref="Path"/> representation of the current instance.
        /// </summary>
        public Path ToPath()
        {
            if (!(_parsedGraphItem is JObject))
            {
                throw new InvalidOperationException(string.Format("Cannot create an Path from {0}", _json));
            }
            ICollection<ICollection<string>> labels = null;
            var labelsProp = (GraphNode[]) GetValue("labels", true);
            if (labelsProp != null)
            {
                labels = labelsProp
                    .Select(node => node.ToArray().Select(value => value.ToString()).ToArray())
                    .ToArray();
            }
            return new Path(labels, (GraphNode[]) GetValue("objects", true));
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
                throw new InvalidOperationException(string.Format("Cannot create a Vertex from {0}", GetJson()));
            }
            var properties = ((JObject) _parsedGraphItem["properties"])
                .Properties()
                .ToDictionary(prop => prop.Name, prop => new GraphNode(prop.Value));
            return new Vertex(
                (GraphNode)GetValue("id", true),
                GetValue("label", true).ToString(),
                properties);
        }

        /// <summary>
        /// Returns true if the value represented by the instances are the same.
        /// </summary>
        public static bool operator ==(GraphNode result1, GraphNode result2)
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
        public static bool operator !=(GraphNode result1, GraphNode result2)
        {
            return !(result1 == result2);
        }

        /// <summary>
        /// Converts this instance to a <see cref="Vertex"/> instance.
        /// </summary>
        public static implicit operator Vertex(GraphNode b)
        {
            return b.ToVertex();
        }

        /// <summary>
        /// Converts this instance to an <see cref="Edge"/> instance.
        /// </summary>
        public static implicit operator Edge(GraphNode b)
        {
            return b.ToEdge();
        }

        /// <summary>
        /// Converts this instance to a <see cref="Path"/> instance.
        /// </summary>
        public static implicit operator Path(GraphNode b)
        {
            return b.ToPath();
        }
    }
}
