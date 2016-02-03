using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Dse.Graph;
using NUnit.Framework;

namespace Dse.Test.Unit
{
    public class GraphResultTests : BaseUnitTest
    {
        [Test]
        public void Constructor_Should_Throw_When_Json_Is_Null()
        {
            // ReSharper disable once ObjectCreationAsStatement
            Assert.Throws<ArgumentNullException>(() => new GraphResult(null));
        }

        [Test]
        public void Constructor_Should_Parse_Json()
        {
            dynamic result = new GraphResult("{\"result\": \"something\"}");
            Assert.AreEqual("something", result.ToString());

            result = new GraphResult("{\"result\": {\"something\": 1.2 }}");
            Assert.AreEqual(1.2D, result.something);
        }

        [Test]
        public void Should_Return_Throw_For_Non_Existent_Dynamic_Property_Name()
        {
            dynamic result = new GraphResult("{\"result\": 1.2}");
            Assert.Throws<KeyNotFoundException>(() =>
            {
                var zeta = result.zeta;
            });

            result = new GraphResult("{\"result\": {\"something\": 1.2 }}");
            Assert.Throws<KeyNotFoundException>(() =>
            {
                var gamma = result.gamma;
            });
        }

        [Test]
        public void ToDouble_Should_Convert_To_Double()
        {
            var result = new GraphResult("{\"result\": 1.9}");
            Assert.AreEqual(1.9, result.ToDouble());
        }

        [Test]
        public void ToDouble_Should_Throw_For_Non_Scalar_Values()
        {
            var result = new GraphResult("{\"result\": {\"something\": 0 }}");
            Assert.Throws<InvalidOperationException>(() => result.ToDouble());
        }

        [Test]
        public void Get_T_Should_Get_A_Typed_Value_By_Name()
        {
            throw new NotImplementedException();
        }

        [Test]
        public void Get_T_Should_Allow_Dynamic_For_Object_Trees()
        {
            var result = new GraphResult("{\"result\": {\"something\": {\"is_awesome\": true} }}");
            Assert.AreEqual(true, result.Get<dynamic>("something").is_awesome.Value);
        }

        [Test]
        public void Get_T_Should_Throw_For_Non_Existent_Dynamic_Property_Name()
        {
            throw new NotImplementedException();
        }

        [Test]
        public void Equals_Should_Return_True_For_The_Same_Json()
        {
            throw new NotImplementedException();
        }
    }
}
