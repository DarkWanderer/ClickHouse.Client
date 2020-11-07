using System;
using System.Collections.Generic;
using System.Text;
using ClickHouse.Client.ADO.Parameters;
using NUnit.Framework;

namespace ClickHouse.Client.Tests
{
    public class ParameterCollectionTests
    {
        [Test]
        public void TestParameterCollectionOperations()
        {
            var param1 = new ClickHouseDbParameter() { ParameterName = "param1", ClickHouseType = "Int32", Value = 1 };
            var param2 = new ClickHouseDbParameter() { ParameterName = "param2", ClickHouseType = "Int32", Value = 2 };
            var param3 = new ClickHouseDbParameter() { ParameterName = "param3", ClickHouseType = "String", Value = "ASD" };
            var param4 = new ClickHouseDbParameter() { ParameterName = "param4", ClickHouseType = "Nothing", Value = null };

            var collection = new ClickHouseParameterCollection();
            collection.Add(param1);
            collection.AddRange(new[] { param2, param3 });

            CollectionAssert.AllItemsAreNotNull(collection);
            CollectionAssert.AllItemsAreUnique(collection);
            Assert.AreEqual(3, collection.Count);
            Assert.IsTrue(collection.Contains(param2));
            Assert.IsTrue(collection.Contains("param3"));
            collection.CopyTo(new object[collection.Count], 0);
            Assert.AreEqual(0, collection.IndexOf(param1));
            Assert.AreEqual(2, collection.IndexOf("param3"));

            collection["param4"] = param4;
            collection.Insert(3, param2);
            Assert.AreEqual(param2, collection[3]);
            Assert.AreEqual(param4, collection["param4"]);
            collection.RemoveAt("param4");
            collection.RemoveAt(3);
            collection.Remove(param2);
            Assert.AreEqual(2, collection.Count);
            collection.Clear();
            Assert.AreEqual(0, collection.Count);
        }
    }
}
