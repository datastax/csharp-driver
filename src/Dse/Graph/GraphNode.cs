//
//  Copyright (C) 2016-2017 DataStax, Inc.
//
//  Please see the license for details:
//  http://www.datastax.com/terms/datastax-dse-driver-license-terms
//

using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq;
using System.Runtime.Serialization;
using Dse.Serialization.Graph;
using Dse.Serialization.Graph.GraphSON1;
using Dse.Serialization.Graph.GraphSON2;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Dse.Graph
{
    /// <summary>
    /// Represents an item of a graph query result, it can be a vertex, an edge, a path or an scalar value.
    /// </summary>
#if !NETCORE
    [Serializable]
#endif
    public class GraphNode : DynamicObject, IEquatable<GraphNode>, IGraphNode
#if !NETCORE
        , ISerializable
#endif
    {
        private readonly INode _node;

        /// <summary>
        /// Returns true if the underlying value is an array.
        /// </summary>
        public bool IsArray => _node.IsArray;

        /// <summary>
        /// Returns true if the underlying value is an object tree.
        /// </summary>
        public bool IsObjectTree => _node.IsObjectTree;

        /// <summary>
        /// Returns true if the underlying value is a scalar value (string, double, boolean, ...).
        /// </summary>
        public bool IsScalar => _node.IsScalar;

        /// <summary>
        /// Gets the number of identical results represented by this instance.
        /// Defaults to 1.
        /// </summary>
        internal long Bulk => _node.Bulk;

        /// <summary>
        /// Creates a new instance of <see cref="GraphNode"/>.
        /// </summary>
        /// <param name="json">The graph string json with the form: "{\"result\": ...}".</param>
        public GraphNode(string json)
        {
            // A local default
            _node = new GraphSON1Node(json);
        }
        
        internal GraphNode(INode node)
        {
            _node = node;
        }

#if !NETCORE
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
            if (objectTree["@type"] != null)
            {
                _node = new GraphSON2Node(objectTree);
            }
            else
            {
                _node = new GraphSON1Node(objectTree);   
            }
        }
#endif

        /// <summary>
        /// Gets the typed value of a property of the result.
        /// </summary>
        /// <typeparam name="T">Type of the property. Use dynamic for object trees.</typeparam>
        /// <param name="propertyName">Name of the property.</param>
        /// <exception cref="NotSupportedException">
        /// Throws NotSupportedException when the target type is not supported
        /// </exception>
        public T Get<T>(string propertyName)
        {
            return Get<T>(propertyName, false);
        }

        internal T Get<T>(string propertyName, bool throwIfNotFound) => _node.Get<T>(propertyName, throwIfNotFound);

        /// <summary>
        /// Gets the raw data represented by this instance.
        /// <para>
        /// Raw internal representation might be different depending on the graph serialization format and
        /// it is subject to change without any prior notice.
        /// </para>
        /// </summary>
        public dynamic GetRaw()
        {
            return _node.GetRaw();
        }

        /// <summary>
        /// Returns true if the property is defined in this instance.
        /// </summary>
        /// <exception cref="InvalidOperationException">When the underlying value is not an object tree</exception>
        public bool HasProperty(string name) => _node.HasProperty(name);

        /// <summary>
        /// Provides the implementation for operations that get member values.
        /// </summary>
        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            return _node.TryGetMember(binder, out result);
        }

        public override bool TryConvert(ConvertBinder binder, out object result)
        {
            result = To(binder.ReturnType);
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
            return _node.GetHashCode() == other._node.GetHashCode();
        }

        public bool Equals(IGraphNode other)
        {
            if (other == null)
            {
                return false;
            }
            if (ReferenceEquals(this, other))
            {
                return true;
            }
            var otherNode = other as GraphNode;
            if (otherNode == null)
            {
                return false;
            }
            return _node.GetHashCode() == otherNode._node.GetHashCode();
        }

        /// <summary>
        /// Returns true if the value represented by this instance is the same.
        /// </summary>
        public override bool Equals(object obj) => Equals(obj as GraphNode);

        /// <summary>
        /// Gets the hash code for this instance, based on its value.
        /// </summary>
        public override int GetHashCode() => _node.GetHashCode();

#if !NETCORE
        /// <inheritdoc />
        public void GetObjectData(SerializationInfo info, StreamingContext context)
        {
            _node.GetObjectData(info, context);
        }
#endif

        /// <summary>
        /// Gets the a dictionary of properties of this node.
        /// </summary>
        public IDictionary<string, GraphNode> GetProperties() => _node.GetProperties();

        IDictionary<string, IGraphNode> IGraphNode.GetProperties() => _node.GetIProperties();

        /// <summary>
        /// Returns the representation of the <see cref="GraphNode"/> as an instance of type T.
        /// </summary>
        /// <typeparam name="T">The type to which the current instance is going to be converted to.</typeparam>
        /// <exception cref="NotSupportedException">
        /// Throws NotSupportedException when the target type is not supported
        /// </exception>
        public T To<T>() => (T) To(typeof(T));

        /// <summary>
        /// Returns the representation of the <see cref="GraphNode"/> as an instance of the type provided.
        /// </summary>
        /// <exception cref="NotSupportedException">
        /// Throws NotSupportedException when the target type is not supported
        /// </exception>
        public object To(Type type)
        {
            if (type == null)
            {
                throw new ArgumentNullException(nameof(type));
            }
            if (type == typeof(object) || type == typeof(GraphNode) || type == typeof(IGraphNode))
            {
                return this;
            }
            return _node.To(type);
        }

        /// <summary>
        /// Converts the instance into an array when the internal representation is a json array.
        /// </summary>
        public GraphNode[] ToArray() => _node.ToArray();

        /// <summary>
        /// Converts the instance into an array when the internal representation is a json array.
        /// </summary>
        // ReSharper disable once CoVariantArrayConversion It should not be written.
        public IGraphNode[] ToIArray() => _node.ToArray();

        /// <summary>
        /// Returns the representation of the result as a boolean.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// It throws an InvalidOperationException when the internal value is not an scalar.
        /// </exception>
        /// <exception cref="InvalidCastException">When the scalar value is not convertible to target type.</exception>
        public bool ToBoolean() => To<bool>();

        /// <summary>
        /// Returns the representation of the result as a double.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// It throws an InvalidOperationException when the internal value is not an scalar.
        /// </exception>
        /// <exception cref="InvalidCastException">When the scalar value is not convertible to target type.</exception>
        public double ToDouble() => To<double>();

        /// <summary>
        /// Returns an edge representation of the current instance.
        /// <para>
        /// This method is maintained for backward compatibity. It's recommended that you use
        /// <see cref="To{IEdge}()"/> instead, providing <see cref="IEdge"/> as type parameter
        /// </para>
        /// </summary>
        public Edge ToEdge() => To<Edge>();

        /// <summary>
        /// Returns the representation of the result as an int.
        /// </summary>
        /// <exception cref="InvalidOperationException">
        /// It throws an InvalidOperationException when the internal value is not an scalar.
        /// </exception>
        /// <exception cref="InvalidCastException">When the scalar value is not convertible to target type.</exception>
        public int ToInt32() => To<int>();

        /// <summary>
        /// Returns a <see cref="Path"/> representation of the current instance.
        /// <para>
        /// This method is maintained for backward compatibity. It's recommended that you use
        /// <see cref="To{IPath}()"/> instead, providing <see cref="IPath"/> as type parameter.
        /// </para>
        /// </summary>
        public Path ToPath() => To<Path>();

        /// <summary>
        /// Returns the json representation of the result.
        /// </summary>
        public override string ToString() => _node.ToString();

        /// <summary>
        /// Returns a vertex representation of the current instance.
        /// <para>
        /// This method is maintained for backward compatibity. It's recommended that you use
        /// <see cref="To{IVertex}()"/> instead, providing <see cref="IVertex"/> as type parameter.
        /// </para>
        /// </summary>
        public Vertex ToVertex() => To<Vertex>();

        internal void WriteJson(JsonWriter writer, JsonSerializer serializer)
        {
            _node.WriteJson(writer, serializer);
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

        /// <summary>
        /// Converts this instance to a string representation.
        /// </summary>
        public static implicit operator string(GraphNode b)
        {
            return b.ToString();
        }

        /// <summary>
        /// Converts this instance to a short representation.
        /// </summary>
        public static implicit operator short(GraphNode b)
        {
            return b.To<short>();
        }

        /// <summary>
        /// Converts this instance to an int representation.
        /// </summary>
        public static implicit operator int(GraphNode b)
        {
            return b.To<int>();
        }

        /// <summary>
        /// Converts this instance to a long representation.
        /// </summary>
        public static implicit operator long(GraphNode b)
        {
            return b.To<long>();
        }

        /// <summary>
        /// Converts this instance to a float representation.
        /// </summary>
        public static implicit operator float(GraphNode b)
        {
            return b.To<float>();
        }

        /// <summary>
        /// Converts this instance to a long representation.
        /// </summary>
        public static implicit operator double(GraphNode b)
        {
            return b.To<double>();
        }

        /// <summary>
        /// Converts this instance to a boolean representation.
        /// </summary>
        public static implicit operator bool(GraphNode b)
        {
            return b.To<bool>();
        }
    }
}
