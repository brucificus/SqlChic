using System.Data.SqlClient;
using System.Linq;
using NUnit.Framework;

namespace SqlChic.Tests
{
    public class Tests45
    {
        private SqlConnection GetOpenConnection()
        {
            var connection = new SqlConnection("Data Source=.;Initial Catalog=tempdb;Integrated Security=True;MultipleActiveResultSets=True");
            connection.Open();
            return connection;
        }

        [Test]
        public void TestBasicStringUsageAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var query = connection.QueryAsync<string>("select 'abc' as [Value] union all select @txt", new { txt = "def" });
                var arr = query.Result.ToArray();
                arr.IsSequenceEqualTo(new[] { "abc", "def" });
            }
        }

        [Test]
        public void TestClassWithStringUsageAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var query = connection.QueryAsync<BasicType>("select 'abc' as [Value] union all select @txt", new { txt = "def" });
                var arr = query.Result.ToArray();
                arr.Select(x => x.Value).IsSequenceEqualTo(new[] { "abc", "def" });
            }
        }

        [Test]
        public void TestMultiMapWithSplitAsync()
        {
            var sql = @"select 1 as id, 'abc' as name, 2 as id, 'def' as name";
            using (var connection = GetOpenConnection())
            {
                var productQuery = connection.QueryAsync<Product, Category, Product>(sql, (prod, cat) =>
                {
                    prod.Category = cat;
                    return prod;
                });

                var product = productQuery.Result.First();
                // assertions
                product.Id.IsEqualTo(1);
                product.Name.IsEqualTo("abc");
                product.Category.Id.IsEqualTo(2);
                product.Category.Name.IsEqualTo("def");
            }
        }

        class Product
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public Category Category { get; set; }
        }
        class Category
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public string Description { get; set; }
        }

        class BasicType
        {
            public string Value { get; set; }
        }
    }
}