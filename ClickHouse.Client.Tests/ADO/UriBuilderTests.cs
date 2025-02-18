using System;
using ClickHouse.Client;
using NUnit.Framework;

namespace ClickHouse.Client.Tests.ADO
{
    public class UriBuilderTests
    {
        [Test]
        public void ShouldEncodeTurkishCharactersCorrectly()
        {
            var baseUri = new Uri("http://localhost");
            var builder = new ClickHouseUriBuilder(baseUri)
            {
                Sql = "SELECT * FROM table WHERE name = {PRM1:String}"
            };
            builder.AddSqlQueryParameter("PRM1", "Bardak Çay");

            var result = builder.ToString();

            Assert.IsTrue(result.Contains("param_PRM1=Bardak+%c3%87ay"));
        }
    }
}
