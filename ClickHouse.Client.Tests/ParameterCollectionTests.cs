using ClickHouse.Client.ADO.Parameters;

namespace ClickHouse.Client.Tests;

public class ParameterCollectionTests
{
    [Test]
    public void TestParameterCollectionOperations()
    {
        var param1 = new ClickHouseDbParameter() { ParameterName = "param1", ClickHouseType = "Int32", Value = 1 };
        var param2 = new ClickHouseDbParameter() { ParameterName = "param2", ClickHouseType = "Int32", Value = 2 };
        var param3 = new ClickHouseDbParameter() { ParameterName = "param3", ClickHouseType = "String", Value = "ASD" };
        var param4 = new ClickHouseDbParameter() { ParameterName = "param4", ClickHouseType = "Nothing", Value = null };

        var collection = new ClickHouseParameterCollection
        {
            param1
        };
        collection.AddRange(new[] { param2, param3 });

        CollectionAssert.AllItemsAreNotNull(collection);
        CollectionAssert.AllItemsAreUnique(collection);
        ClassicAssert.AreEqual(3, collection.Count);
        ClassicAssert.IsTrue(collection.Contains(param2));
        ClassicAssert.IsTrue(collection.Contains("param3"));
        collection.CopyTo(new object[collection.Count], 0);
        ClassicAssert.AreEqual(0, collection.IndexOf(param1));
        ClassicAssert.AreEqual(2, collection.IndexOf("param3"));

        collection["param4"] = param4;
        collection.Insert(3, param2);
        ClassicAssert.AreEqual(param2, collection[3]);
        ClassicAssert.AreEqual(param4, collection["param4"]);

        var sql = "SELECT @param1, @param2, @param3, @param4";
        ClassicAssert.AreEqual("SELECT {param1:Int32}, {param2:Int32}, {param3:String}, {param4:Nothing}", collection.ReplacePlaceholders(sql));

        collection.RemoveAt("param4");
        collection.RemoveAt(3);
        collection.Remove(param2);
        ClassicAssert.AreEqual(2, collection.Count);
        collection.Clear();
        ClassicAssert.AreEqual(0, collection.Count);
    }
}
