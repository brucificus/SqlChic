using System;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Diagnostics;
using System.Linq;
using System.Reactive.Linq;
using System.Threading.Tasks;
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

		private SqlConnection GetClosedConnection()
		{
			var connection = new SqlConnection("Data Source=.;Initial Catalog=tempdb;Integrated Security=True;MultipleActiveResultSets=True");
			return connection;
		}

        [Test]
        public async Task TestBasicStringUsageAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var query = connection.Query<string>("select 'abc' as [Value] union all select @txt", new { txt = "def" });
                var arr = await query.ToArray();
                arr.IsSequenceEqualTo(new[] { "abc", "def" });
            }
        }

		[Test]
		public async Task TestBasicStringUsageClosedAsync()
		{
			using (var connection = GetClosedConnection())
			{
				var query = connection.Query<string>("select 'abc' as [Value] union all select @txt", new { txt = "def" });
				var arr = await query.ToArray();
				arr.IsSequenceEqualTo(new[] { "abc", "def" });
			}
		}

        [Test]
        public async Task TestClassWithStringUsageAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var query = connection.Query<BasicType>("select 'abc' as [Value] union all select @txt", new { txt = "def" });
                var arr = await query.ToArray();
                arr.Select(x => x.Value).IsSequenceEqualTo(new[] { "abc", "def" });
            }
        }

		public void TestExecuteAsync()
		{
			using (var connection = GetOpenConnection())
			{
				var query = connection.ExecuteAsync("declare @foo table(id int not null); insert @foo values(@id);", new { id = 1 });
				var val = query.Result;
				val.Equals(1);
			}
		}
		public void TestExecuteClosedConnAsync()
		{
			using (var connection = GetClosedConnection())
			{
				var query = connection.ExecuteAsync("declare @foo table(id int not null); insert @foo values(@id);", new { id = 1 });
				var val = query.Result;
				val.Equals(1);
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

		[Test]
		public void TestMultiMapWithSplitClosedConnAsync()
		{
			var sql = @"select 1 as id, 'abc' as name, 2 as id, 'def' as name";
			using (var connection = GetClosedConnection())
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
		
		[Test]
		public void TestMultiAsync()
		{
			using (var conn = GetOpenConnection())
			{
				using (SqlChic.SqlMapper.GridReader multi = conn.QueryMultipleAsync("select 1; select 2").Result)
				{
					multi.Read<int>().Single().IsEqualTo(1);
					multi.Read<int>().Single().IsEqualTo(2);
				}
			}
		}

		[Test]
		public void TestMultiClosedConnAsync()
		{
			using (var conn = GetClosedConnection())
			{
				using (SqlChic.SqlMapper.GridReader multi = conn.QueryMultipleAsync("select 1; select 2").Result)
				{
					multi.Read<int>().Single().IsEqualTo(1);
					multi.Read<int>().Single().IsEqualTo(2);
				}
			}
		}

		[Test]
		public void ExecuteReaderOpenAsync()
		{
			using (var conn = GetOpenConnection())
			{
				var dt = new DataTable();
				dt.Load(conn.ExecuteReaderAsync("select 3 as [three], 4 as [four]").Result);
				dt.Columns.Count.IsEqualTo(2);
				dt.Columns[0].ColumnName.IsEqualTo("three");
				dt.Columns[1].ColumnName.IsEqualTo("four");
				dt.Rows.Count.IsEqualTo(1);
				((int)dt.Rows[0][0]).IsEqualTo(3);
				((int)dt.Rows[0][1]).IsEqualTo(4);
			}
		}

		[Test]
		public void ExecuteReaderClosedAsync()
		{
			using (var conn = GetClosedConnection())
			{
				var dt = new DataTable();
				dt.Load(conn.ExecuteReaderAsync("select 3 as [three], 4 as [four]").Result);
				dt.Columns.Count.IsEqualTo(2);
				dt.Columns[0].ColumnName.IsEqualTo("three");
				dt.Columns[1].ColumnName.IsEqualTo("four");
				dt.Rows.Count.IsEqualTo(1);
				((int)dt.Rows[0][0]).IsEqualTo(3);
				((int)dt.Rows[0][1]).IsEqualTo(4);
			}
		}

		[Test]
		public async Task LiteralReplacementOpenAsync()
		{
			using (var conn = GetOpenConnection()) await LiteralReplacementAsync(conn);
		}
		
		[Test]
		public async Task LiteralReplacementClosedAsync()
		{
			using (var conn = GetClosedConnection()) await LiteralReplacementAsync(conn);
		}

		private async Task LiteralReplacementAsync(DbConnection connection)
		{
			try { connection.ExecuteAsync("drop table literal1").Wait(); }
			catch { }
			connection.ExecuteAsync("create table literal1 (id int not null, foo int not null)").Wait();
			connection.ExecuteAsync("insert literal1 (id,foo) values ({=id}, @foo)", new { id = 123, foo = 456 }).Wait();
			var rows = new[] { new { id = 1, foo = 2 }, new { id = 3, foo = 4 } };
			connection.ExecuteAsync("insert literal1 (id,foo) values ({=id}, @foo)", rows).Wait();
			var count = await connection.Query<int>("select count(1) from literal1 where id={=foo}", new { foo = 123 }).SingleAsync();
			count.IsEqualTo(1);
			int sum = await connection.Query<int>("select sum(id) + sum(foo) from literal1").SingleAsync();
			sum.IsEqualTo(123 + 456 + 1 + 2 + 3 + 4);
		}

		[Test]
		public async Task LiteralReplacementDynamicOpenAsync()
		{
			using (var conn = GetOpenConnection()) await LiteralReplacementDynamicAsync(conn);
		}
		
		[Test]
		public async Task LiteralReplacementDynamicClosedAsync()
		{
			using (var conn = GetClosedConnection()) await LiteralReplacementDynamicAsync(conn);
		}

		private async Task LiteralReplacementDynamicAsync(DbConnection connection)
		{
			var args = new DynamicParameters();
			args.Add("id", 123);
			try { connection.ExecuteAsync("drop table literal2").Wait(); }
			catch { }
			connection.ExecuteAsync("create table literal2 (id int not null)").Wait();
			connection.ExecuteAsync("insert literal2 (id) values ({=id})", args).Wait();

			args = new DynamicParameters();
			args.Add("foo", 123);
			var count = await connection.Query<int>("select count(1) from literal2 where id={=foo}", args).SingleAsync();
			count.IsEqualTo(1);
		}

		[Test]
		public async Task LiteralInAsync()
		{
			using (var connection = GetOpenConnection())
			{
				connection.ExecuteAsync("create table #literalin(id int not null);").Wait();
				connection.ExecuteAsync("insert #literalin (id) values (@id)", new[] {
                    new { id = 1 },
                    new { id = 2 },
                    new { id = 3 },
                }).Wait();
				var count = await connection.Query<int>("select count(1) from #literalin where id in {=ids}",
					new { ids = new[] { 1, 3, 4 } }).SingleAsync();
				count.IsEqualTo(2);
			}
		}

		[Test]
		public void RunSequentialVersusParallelAsync()
		{
			var ids = Enumerable.Range(1, 20000).Select(id => new { id }).ToArray();
			using (var connection = GetOpenConnection())
			{
				connection.ExecuteAsync(new CommandDefinition("select @id", ids.Take(5), flags: CommandFlags.None)).Wait();

				var watch = Stopwatch.StartNew();
				connection.ExecuteAsync(new CommandDefinition("select @id", ids, flags: CommandFlags.None)).Wait();
				watch.Stop();
				System.Console.WriteLine("No pipeline: {0}ms", watch.ElapsedMilliseconds);

				watch = Stopwatch.StartNew();
				connection.ExecuteAsync(new CommandDefinition("select @id", ids, flags: CommandFlags.Pipelined)).Wait();
				watch.Stop();
				System.Console.WriteLine("Pipeline: {0}ms", watch.ElapsedMilliseconds);
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

		[Test]
		public async Task TypeBasedViaTypeAsync()
		{
			Type type = GetSomeType();

			using (var connection = GetOpenConnection())
			{
				dynamic actual = await connection.Query(type, "select @A as [A], @B as [B]", new { A = 123, B = "abc" }).FirstOrDefaultAsync();
				((object)actual).GetType().IsEqualTo(type);
				int a = actual.A;
				string b = actual.B;
				a.IsEqualTo(123);
				b.IsEqualTo("abc");
			}
		}

		static Type GetSomeType()
		{
			return typeof(SomeType);
		}

		public class SomeType
		{
			public int A { get; set; }
			public string B { get; set; }
		}
    }
}