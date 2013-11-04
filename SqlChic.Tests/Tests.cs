//#define POSTGRESQL // uncomment to run postgres tests

using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using System.Reactive.Linq;
using System.Threading.Tasks;
using System.Data.SqlServerCe;
using System.IO;
using System.Data;
using System.Dynamic;
using Microsoft.CSharp.RuntimeBinder;
using NUnit.Framework;
using DescriptionAttribute = System.ComponentModel.DescriptionAttribute;

#if POSTGRESQL
using Npgsql;
#endif

namespace SqlChic.Tests
{

    public class Tests
    {
        //SqlConnection connection = Program.GetOpenConnection();
        private const string connectionString = "Data Source=.;Initial Catalog=tempdb;Integrated Security=True;MultipleActiveResultSets=True";
        private SqlConnection GetOpenConnection()
        {
            var connection = new SqlConnection(connectionString);
            connection.Open();
            return connection;
        }

        private SqlConnection GetClosedConnection()
        {
            var conn = new SqlConnection(connectionString);
            if (conn.State != ConnectionState.Closed) throw new InvalidOperationException("should be closed!");
            return conn;
        }

        public class AbstractInheritance
        {
            public abstract class Order
            {
                internal int Internal { get; set; }
                protected int Protected { get; set; }
                public int Public { get; set; }

                public int ProtectedVal { get { return Protected; } }
            }

            public class ConcreteOrder : Order
            {
                public int Concrete { get; set; }
            }
        }

        class UserWithConstructor
        {
            public UserWithConstructor(int id, string name)
            {
                Ident = id;
                FullName = name;
            }
            public int Ident { get; set; }
            public string FullName { get; set; }
        }

        class PostWithConstructor
        {
            public PostWithConstructor(int id, int ownerid, string content)
            {
                Ident = id;
                FullContent = content;
            }

            public int Ident { get; set; }
            public UserWithConstructor Owner { get; set; }
            public string FullContent { get; set; }
            public Comment Comment { get; set; }
        }

        [Test]
        public async Task TestMultiMapWithConstructorAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var createSql = @"
                create table #Users (Id int, Name varchar(20))
                create table #Posts (Id int, OwnerId int, Content varchar(20))

                insert #Users values(99, 'Sam')
                insert #Users values(2, 'I am')

                insert #Posts values(1, 99, 'Sams Post1')
                insert #Posts values(2, 99, 'Sams Post2')
                insert #Posts values(3, null, 'no ones post')";
                await connection.Execute(createSql);
                string sql = @"select * from #Posts p 
                           left join #Users u on u.Id = p.OwnerId 
                           Order by p.Id";
                PostWithConstructor[] data =
                    await
                    connection.Query<PostWithConstructor, UserWithConstructor, PostWithConstructor>(sql, (post, user) =>
                        {
                            post.Owner = user;
                            return post;
                        }).ToArray();
                var p = data.First();

                p.FullContent.IsEqualTo("Sams Post1");
                p.Ident.IsEqualTo(1);
                p.Owner.FullName.IsEqualTo("Sam");
                p.Owner.Ident.IsEqualTo(99);

                data[2].Owner.IsNull();

                await connection.Execute("drop table #Users drop table #Posts");
            }
        }


        class MultipleConstructors
        {
            public MultipleConstructors()
            {

            }
            public MultipleConstructors(int a, string b)
            {
                A = a + 1;
                B = b + "!";
            }
            public int A { get; set; }
            public string B { get; set; }
        }

        [Test]
        public async Task TestMultipleConstructorsAsync()
        {
            using (var connection = GetOpenConnection())
            {
                MultipleConstructors mult =
                    await connection.Query<MultipleConstructors>("select 0 A, 'Dapper' b").FirstAsync();
                mult.A.IsEqualTo(0);
                mult.B.IsEqualTo("Dapper");
            }
        }

        class ConstructorsWithAccessModifiers
        {
            private ConstructorsWithAccessModifiers()
            {
            }
            public ConstructorsWithAccessModifiers(int a, string b)
            {
                A = a + 1;
                B = b + "!";
            }
            public int A { get; set; }
            public string B { get; set; }
        }

        [Test]
        public async Task TestConstructorsWithAccessModifiersAsync()
        {
            using (var connection = GetOpenConnection())
            {
                ConstructorsWithAccessModifiers value =
                    await connection.Query<ConstructorsWithAccessModifiers>("select 0 A, 'Dapper' b").FirstAsync();
                value.A.IsEqualTo(1);
                value.B.IsEqualTo("Dapper!");
            }
        }

        class NoDefaultConstructor
        {
            public NoDefaultConstructor(int a1, int? b1, float f1, string s1, Guid G1)
            {
                A = a1;
                B = b1;
                F = f1;
                S = s1;
                G = G1;
            }
            public int A { get; set; }
            public int? B { get; set; }
            public float F { get; set; }
            public string S { get; set; }
            public Guid G { get; set; }
        }

        [Test]
        public async Task TestNoDefaultConstructorAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var guid = Guid.NewGuid();
                NoDefaultConstructor nodef =
                    await
                    connection.Query<NoDefaultConstructor>(
                        "select CAST(NULL AS integer) A1,  CAST(NULL AS integer) b1, CAST(NULL AS real) f1, 'Dapper' s1, G1 = @id",
                        new {Id = guid}).FirstAsync();
                nodef.A.IsEqualTo(0);
                nodef.B.IsEqualTo(null);
                nodef.F.IsEqualTo(0);
                nodef.S.IsEqualTo("Dapper");
                nodef.G.IsEqualTo(guid);
            }
        }

        class NoDefaultConstructorWithChar
        {
            public NoDefaultConstructorWithChar(char c1, char? c2, char? c3)
            {
                Char1 = c1;
                Char2 = c2;
                Char3 = c3;
            }
            public char Char1 { get; set; }
            public char? Char2 { get; set; }
            public char? Char3 { get; set; }
        }

        [Test]
        public async Task TestNoDefaultConstructorWithCharAsync()
        {
            using (var connection = GetOpenConnection())
            {
                const char c1 = 'ą';
                const char c3 = 'ó';
                NoDefaultConstructorWithChar nodef =
                    await
                    connection.Query<NoDefaultConstructorWithChar>("select @c1 c1, @c2 c2, @c3 c3",
                                                                   new {c1 = c1, c2 = (char?) null, c3 = c3})
                              .FirstAsync();
                nodef.Char1.IsEqualTo(c1);
                nodef.Char2.IsEqualTo(null);
                nodef.Char3.IsEqualTo(c3);
            }
        }

        class NoDefaultConstructorWithEnum
        {
            public NoDefaultConstructorWithEnum(ShortEnum e1, ShortEnum? n1, ShortEnum? n2)
            {
                E = e1;
                NE1 = n1;
                NE2 = n2;
            }
            public ShortEnum E { get; set; }
            public ShortEnum? NE1 { get; set; }
            public ShortEnum? NE2 { get; set; }
        }

        [Test]
        public async Task TestNoDefaultConstructorWithEnumAsync()
        {
            using (var connection = GetOpenConnection())
            {
                NoDefaultConstructorWithEnum nodef =
                    await
                    connection.Query<NoDefaultConstructorWithEnum>(
                        "select cast(2 as smallint) E1, cast(5 as smallint) n1, cast(null as smallint) n2").FirstAsync();
                nodef.E.IsEqualTo(ShortEnum.Two);
                nodef.NE1.IsEqualTo(ShortEnum.Five);
                nodef.NE2.IsEqualTo(null);
            }
        }

        class NoDefaultConstructorWithBinary
        {
            public System.Data.Linq.Binary Value { get; set; }
            public int Ynt { get; set; }
            public NoDefaultConstructorWithBinary(System.Data.Linq.Binary val)
            {
                Value = val;
            }
        }

        [Test]
        public async Task TestNoDefaultConstructorBinaryAsync()
        {
            using (var connection = GetOpenConnection())
            {
                byte[] orig = new byte[20];
                new Random(123456).NextBytes(orig);
                var input = new System.Data.Linq.Binary(orig);
                var output =
                    (await
                     connection.Query<NoDefaultConstructorWithBinary>("select @input as val", new {input}).FirstAsync())
                        .Value;
                output.ToArray().IsSequenceEqualTo(orig);
            }
        }

        // http://stackoverflow.com/q/8593871
        [Test]
        public async Task TestAbstractInheritanceAsync() 
        {
            using (var connection = GetOpenConnection())
            {
                var order =
                    await
                    connection.Query<AbstractInheritance.ConcreteOrder>(
                        "select 1 Internal,2 Protected,3 [Public],4 Concrete").FirstAsync();

                order.Internal.IsEqualTo(1);
                order.ProtectedVal.IsEqualTo(2);
                order.Public.IsEqualTo(3);
                order.Concrete.IsEqualTo(4);
            }
        }

        [Test]
        public async Task TestListOfAnsiStringsAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var results = await connection.Query<string>("select * from (select 'a' str union select 'b' union select 'c') X where str in @strings",
                    new { strings = new[] { new DbString { IsAnsi = true, Value = "a" }, new DbString { IsAnsi = true, Value = "b" } } }).ToList();

                results[0].IsEqualTo("a");
                results[1].IsEqualTo("b");
            }
        }

        [Test]
        public async Task TestNullableGuidSupportAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var guid = await connection.Query<Guid?>("select null").FirstAsync();
                guid.IsNull();

                guid = Guid.NewGuid();
                var guid2 = await connection.Query<Guid?>("select @guid", new {guid}).FirstAsync();
                guid.IsEqualTo(guid2);
            }
        }

        [Test]
        public async Task TestNonNullableGuidSupportAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var guid = Guid.NewGuid();
                var guid2 = await connection.Query<Guid?>("select @guid", new {guid}).FirstAsync();
                Assert.IsTrue(guid == guid2);
            }
        }

        struct Car
        {
            public enum TrapEnum : int
            {
                A = 1,
                B = 2
            }
#pragma warning disable 0649
            public string Name;
#pragma warning restore 0649
            public int Age { get; set; }
            public TrapEnum Trap { get; set; }
        
        }

        [Test]
        public async Task TestStructsAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var car = await connection.Query<Car>("select 'Ford' Name, 21 Age, 2 Trap").FirstAsync();

                car.Age.IsEqualTo(21);
                car.Name.IsEqualTo("Ford");
                ((int) car.Trap).IsEqualTo(2);
            }
        }

        [Test]
        public async Task SelectListIntAsync()
        {
            using (var connection = GetOpenConnection())
            {
                (await connection.Query<int>("select 1 union all select 2 union all select 3").ToList())
                    .IsSequenceEqualTo(new[] {1, 2, 3});
            }
        }
        
        [Test]
        public async Task SelectBinaryAsync()
        {
            using (var connection = GetOpenConnection())
            {
                (await connection.Query<byte[]>("select cast(1 as varbinary(4))").FirstAsync()).IsSequenceEqualTo(new byte[] { 0, 0, 0, 1 });
            }
        }

        [Test]
        public async Task PassInIntArrayAsync()
        {
            using (var connection = GetOpenConnection())
            {
                (await connection.Query<int>("select * from (select 1 as Id union all select 2 union all select 3) as X where Id in @Ids", new { Ids = new int[] { 1, 2, 3 }.AsEnumerable() }).ToList())
                 .IsSequenceEqualTo(new[] { 1, 2, 3 });
            }
        }

        [Test]
        public async Task PassInEmptyIntArrayAsync()
        {
            using (var connection = GetOpenConnection())
            {
                (await connection.Query<int>("select * from (select 1 as Id union all select 2 union all select 3) as X where Id in @Ids", new { Ids = new int[0] }).ToList())
                 .IsSequenceEqualTo(new int[0]);
            }
        }

        [Test]
        public async Task TestSchemaChangedAsync()
        {
            using (var connection = GetOpenConnection())
            {
                await connection.Execute("create table #dog(Age int, Name nvarchar(max)) insert #dog values(1, 'Alf')");
                var d = await connection.Query<Dog>("select * from #dog").SingleAsync();
                d.Name.IsEqualTo("Alf");
                d.Age.IsEqualTo(1);
                await connection.Execute("alter table #dog drop column Name");
                d = await connection.Query<Dog>("select * from #dog").SingleAsync();
                d.Name.IsNull();
                d.Age.IsEqualTo(1);
                await connection.Execute("drop table #dog");
            }
        }

        [Test]
        public async Task TestSchemaChangedMultiMapAsync()
        {
            using (var connection = GetOpenConnection())
            {
                await connection.Execute("create table #dog(Age int, Name nvarchar(max)) insert #dog values(1, 'Alf')");
                var tuple = await connection.Query<Dog, Dog, Tuple<Dog, Dog>>("select * from #dog d1 join #dog d2 on 1=1", (d1, d2) => Tuple.Create(d1, d2), splitOn: "Age").SingleAsync();

                tuple.Item1.Name.IsEqualTo("Alf");
                tuple.Item1.Age.IsEqualTo(1);
                tuple.Item2.Name.IsEqualTo("Alf");
                tuple.Item2.Age.IsEqualTo(1);

                await connection.Execute("alter table #dog drop column Name");
                tuple = await connection.Query<Dog, Dog, Tuple<Dog, Dog>>("select * from #dog d1 join #dog d2 on 1=1", (d1, d2) => Tuple.Create(d1, d2), splitOn: "Age").SingleAsync();

                tuple.Item1.Name.IsNull();
                tuple.Item1.Age.IsEqualTo(1);
                tuple.Item2.Name.IsNull();
                tuple.Item2.Age.IsEqualTo(1);

                await connection.Execute("drop table #dog");
            }
        }

        [Test]
        public async Task TestReadMultipleIntegersWithSplitOnAnyAsync()
        {
            using (var connection = GetOpenConnection())
            {
                (await connection.Query<int, int, int, Tuple<int, int, int>>(
                    "select 1,2,3 union all select 4,5,6", Tuple.Create, splitOn: "*").ToList())
                 .IsSequenceEqualTo(new[] { Tuple.Create(1, 2, 3), Tuple.Create(4, 5, 6) });
            }
        }

        [Test]
        public async Task TestDoubleParamAsync()
        {
            using (var connection = GetOpenConnection())
            {
                (await connection.Query<double>("select @d", new {d = 0.1d}).FirstAsync())
                    .IsEqualTo(0.1d);
            }
        }

        [Test]
        public async Task TestBoolParamAsync()
        {
            using (var connection = GetOpenConnection())
            {
                (await connection.Query<bool>("select @b", new {b = false}).FirstAsync())
                    .IsFalse();
            }
        }

        // http://code.google.com/p/dapper-dot-net/issues/detail?id=70
        // https://connect.microsoft.com/VisualStudio/feedback/details/381934/sqlparameter-dbtype-dbtype-time-sets-the-parameter-to-sqldbtype-datetime-instead-of-sqldbtype-time
        [Test]
        public async Task TestTimeSpanParamAsync()
        {
            using (var connection = GetOpenConnection())
            {
                (await connection.Query<TimeSpan>("select @ts", new {ts = TimeSpan.FromMinutes(42)}).FirstAsync())
                    .IsEqualTo(TimeSpan.FromMinutes(42));
            }
        }

        [Test]
        public async Task TestStringsAsync()
        {
            using (var connection = GetOpenConnection())
            {
                (await connection.Query<string>(@"select 'a' a union select 'b'").ToList())
                    .IsSequenceEqualTo(new[] {"a", "b"});
            }
        }

        // see http://stackoverflow.com/questions/16726709/string-format-with-sql-wildcard-causing-dapper-query-to-break
        [Test]
        public async Task CheckComplexConcatAsync()
        {
            using (var connection = GetOpenConnection())
            {
                string end_wildcard = @"
SELECT * FROM #users16726709
WHERE (first_name LIKE @search_term+'%' OR last_name LIKE @search_term+'%');";

                string both_wildcards = @"
SELECT * FROM #users16726709
WHERE (first_name LIKE '%'+@search_term+'%' OR last_name LIKE '%'+@search_term+'%');";

                string formatted = @"
SELECT * FROM #users16726709
WHERE (first_name LIKE {0} OR last_name LIKE {0});";

                string use_end_only = @"@search_term+'%'";
                string use_both = @"'%'+@search_term+'%'";

                // if true, slower query due to not being able to use indices, but will allow searching inside strings 
                bool allow_start_wildcards = false;

                string query = String.Format(formatted, allow_start_wildcards ? use_both : use_end_only);
                string term = "F"; // the term the user searched for

                await connection.Execute(@"create table #users16726709 (first_name varchar(200), last_name varchar(200))
insert #users16726709 values ('Fred','Bloggs') insert #users16726709 values ('Tony','Farcus') insert #users16726709 values ('Albert','Tenof')");

                // Using Dapper
                (await connection.Query(end_wildcard, new {search_term = term}).Count()).IsEqualTo(2);
                (await connection.Query(both_wildcards, new {search_term = term}).Count()).IsEqualTo(3);
                (await connection.Query(query, new {search_term = term}).Count()).IsEqualTo(2);
            }
        }

        enum EnumParam : short
        {
            None, A, B
        }
        class EnumParamObject
        {
            public EnumParam A { get; set; }
            public EnumParam? B { get; set; }
            public EnumParam? C { get; set; }
        }
        class EnumParamObjectNonNullable
        {
            public EnumParam A { get; set; }
            public EnumParam? B { get; set; }
            public EnumParam? C { get; set; }
        }
        
        [Test]
        public async Task TestEnumParamsWithNullableAsync()
        {
            using (var connection = GetOpenConnection())
            {
                EnumParam a = EnumParam.A;
                EnumParam? b = EnumParam.B, c = null;
                var obj = await connection.Query<EnumParamObject>("select @a as A, @b as B, @c as C",
                                                                  new {a, b, c}).SingleAsync();
                obj.A.IsEqualTo(EnumParam.A);
                obj.B.IsEqualTo(EnumParam.B);
                obj.C.IsEqualTo(null);
            }
        }

        [Test]
        public async Task TestEnumParamsWithoutNullableAsync()
        {
            using (var connection = GetOpenConnection())
            {
                EnumParam a = EnumParam.A;
                EnumParam b = EnumParam.B, c = 0;
                var obj = await connection.Query<EnumParamObjectNonNullable>("select @a as A, @b as B, @c as C",
                                                                             new {a, b, c}).SingleAsync();
                obj.A.IsEqualTo(EnumParam.A);
                obj.B.IsEqualTo(EnumParam.B);
                obj.C.IsEqualTo((EnumParam) 0);
            }
        }
        
        public class Dog
        {
            public int? Age { get; set; }
            public Guid Id { get; set; }
            public string Name { get; set; }
            public float? Weight { get; set; }

            public int IgnoredProperty { get { return 1; } }
        }

        [Test]
        public async Task TestExtraFieldsAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var guid = Guid.NewGuid();
                var dog = await connection.Query<Dog>("select '' as Extra, 1 as Age, 0.1 as Name1 , Id = @id", new { Id = guid }).ToList();

                dog.Count()
                   .IsEqualTo(1);

                dog.First().Age
                    .IsEqualTo(1);

                dog.First().Id
                    .IsEqualTo(guid);
            }
        }

        [Test]
        public async Task TestStrongTypeAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var guid = Guid.NewGuid();
                var dog = await connection.Query<Dog>("select Age = @Age, Id = @Id", new { Age = (int?)null, Id = guid }).ToList();

                dog.Count()
                    .IsEqualTo(1);

                dog.First().Age
                    .IsNull();

                dog.First().Id
                    .IsEqualTo(guid);
            }
        }

        [Test]
        public async Task TestSimpleNullAsync()
        {
            using (var connection = GetOpenConnection())
            {
                (await connection.Query<DateTime?>("select null").FirstAsync()).IsNull();
            }
        }

        [Test]
        public async Task TestExpandoAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var rows = await connection.Query("select 1 A, 2 B union all select 3, 4").ToList();

                ((int) rows[0].A)
                    .IsEqualTo(1);

                ((int) rows[0].B)
                    .IsEqualTo(2);

                ((int) rows[1].A)
                    .IsEqualTo(3);

                ((int) rows[1].B)
                    .IsEqualTo(4);
            }
        }

        [Test]
        public async Task TestStringListAsync()
        {
            using (var connection = GetOpenConnection())
            {
                (await connection.Query<string>("select * from (select 'a' as x union all select 'b' union all select 'c') as T where x in @strings", new { strings = new[] { "a", "b", "c" } }).ToList())
                    .IsSequenceEqualTo(new[] { "a", "b", "c" });

                (await connection.Query<string>("select * from (select 'a' as x union all select 'b' union all select 'c') as T where x in @strings", new { strings = new string[0] }).ToList())
                       .IsSequenceEqualTo(new string[0]);
            }
        }

        [Test]
        public async Task TestExecuteCommandAsync()
        {
            using (var connection = GetOpenConnection())
            {
                (await connection.Execute(@"
    set nocount on 
    create table #t(i int) 
    set nocount off 
    insert #t 
    select @a a union all select @b 
    set nocount on 
    drop table #t", new {a = 1, b = 2})).IsEqualTo(2);
            }
        }
        
        [Test]
        public async Task TestExecuteCommandWithHybridParametersAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var p = new DynamicParameters(new {a = 1, b = 2});
                p.Add("c", dbType: DbType.Int32, direction: ParameterDirection.Output);
                await connection.Execute(@"set @c = @a + @b", p);
                p.Get<int>("@c").IsEqualTo(3);
            }
        }

        [Test]
        public async Task TestExecuteMultipleCommandAsync()
        {
            using (var connection = GetOpenConnection())
            {
                await connection.Execute("create table #t(i int)");
                int tally = await connection.Execute(@"insert #t (i) values(@a)", new[] { new { a = 1 }, new { a = 2 }, new { a = 3 }, new { a = 4 } });
                int sum = await connection.Query<int>("select sum(i) from #t drop table #t").FirstAsync();
                tally.IsEqualTo(4);
                sum.IsEqualTo(10);
            }
        }

        class Student
        {
            public string Name { get; set; }
            public int Age { get; set; }
        }

        [Test]
        public async Task TestExecuteMultipleCommandStrongTypeAsync()
        {
            using (var connection = GetOpenConnection())
            {
                await connection.Execute("create table #t(Name nvarchar(max), Age int)");
                int tally = await connection.Execute(@"insert #t (Name,Age) values(@Name, @Age)", new List<Student>
                    {
                        new Student {Age = 1, Name = "sam"},
                        new Student {Age = 2, Name = "bob"}
                    });
                int sum = await connection.Query<int>("select sum(Age) from #t drop table #t").FirstAsync();
                tally.IsEqualTo(2);
                sum.IsEqualTo(3);
            }
        }

        [Test]
        public async Task TestExecuteMultipleCommandObjectArrayAsync()
        {
            using (var connection = GetOpenConnection())
            {
                await connection.Execute("create table #t(i int)");
                int tally = await connection.Execute(@"insert #t (i) values(@a)", new object[] { new { a = 1 }, new { a = 2 }, new { a = 3 }, new { a = 4 } });
                int sum = await connection.Query<int>("select sum(i) from #t drop table #t").FirstAsync();
                tally.IsEqualTo(4);
                sum.IsEqualTo(10);
            }
        }

        [Test]
        public async Task TestMassiveStringsAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var str = new string('X', 20000);
                (await connection.Query<string>("select @a", new {a = str}).FirstAsync())
                    .IsEqualTo(str);
            }
        }

        class TestObj
        {
            public int _internal;
            internal int Internal { set { _internal = value; } }

            public int _priv;
            private int Priv { set { _priv = value; } }

            private int PrivGet { get { return _priv;} }
        }

        [Test]
        public async Task TestSetInternalAsync()
        {
            using (var connection = GetOpenConnection())
            {
                (await connection.Query<TestObj>("select 10 as [Internal]").FirstAsync())._internal.IsEqualTo(10);
            }
        }

        [Test]
        public async Task TestSetPrivateAsync()
        {
            using (var connection = GetOpenConnection())
            {
                (await connection.Query<TestObj>("select 10 as [Priv]").FirstAsync())._priv.IsEqualTo(10);
            }
        }

        [Test]
        public async Task TestExpandWithNullableFieldsAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var row = await connection.Query("select null A, 2 B").SingleAsync();

                ((int?) row.A)
                    .IsNull();

                ((int?) row.B)
                    .IsEqualTo(2);
            }
        }
		
		// Had to turn on MARS for async, making this test not very applicable anymore
		//public async Task TestEnumerationAsync()
		//{
		//	var en = connection.Query<int>("select 1 as one union all select 2 as one").Next();
		//	var i = en.GetEnumerator();
		//	i.MoveNext();

		//	bool gotException = false;
		//	try
		//	{
		//		var x = await connection.Query<int>("select 1 as one").FirstAsync();
		//	}
		//	catch (Exception)
		//	{
		//		gotException = true;
		//	}

		//	while (i.MoveNext())
		//	{ }

		//	// should not exception, since enumertated
		//	en = await connection.Query<int>("select 1 as one").ToList();

		//	gotException.IsTrue();
		//}

		// Had to turn on MARS for async, making this test not very applicable anymore
		//public async Task TestEnumerationDynamicAsync()
		//{
		//	var en = connection.Query("select 1 as one union all select 2 as one").Next();
		//	var i = en.GetEnumerator();
		//	i.MoveNext();

		//	bool gotException = false;
		//	try
		//	{
		//		var x = await connection.Query("select 1 as one").FirstAsync();
		//	}
		//	catch (Exception)
		//	{
		//		gotException = true;
		//	}

		//	while (i.MoveNext())
		//	{ }

		//	// should not exception, since enumertated
		//	en = await connection.Query("select 1 as one").ToList();

		//	gotException.IsTrue();
		//}

        [Test]
        public async Task TestNakedBigIntAsync()
        {
            using (var connection = GetOpenConnection())
            {
                long foo = 12345;
                var result = await connection.Query<long>("select @foo", new {foo}).SingleAsync();
                foo.IsEqualTo(result);
            }
        }

        [Test]
        public async Task TestBigIntMemberAsync()
        {
            using (var connection = GetOpenConnection())
            {
                long foo = 12345;
                var result = await connection.Query<WithBigInt>(@"
declare @bar table(Value bigint)
insert @bar values (@foo)
select * from @bar", new {foo}).SingleAsync();
                result.Value.IsEqualTo(foo);
            }
        }
        
        class WithBigInt
        {
            public long Value { get; set; }
        }

        class User
        {
            public int Id { get; set; }
            public string Name { get; set; }
        }
        class Post
        {
            public int Id { get; set; }
            public User Owner { get; set; }
            public string Content { get; set; }
            public Comment Comment { get; set; }
        }

        [Test]
        public async Task TestMultiMapAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var createSql = @"
                create table #Users (Id int, Name varchar(20))
                create table #Posts (Id int, OwnerId int, Content varchar(20))

                insert #Users values(99, 'Sam')
                insert #Users values(2, 'I am')

                insert #Posts values(1, 99, 'Sams Post1')
                insert #Posts values(2, 99, 'Sams Post2')
                insert #Posts values(3, null, 'no ones post')
";
                await connection.Execute(createSql);

                var sql =
    @"select * from #Posts p 
left join #Users u on u.Id = p.OwnerId 
Order by p.Id";

                var data = await connection.Query<Post, User, Post>(sql, (post, user) => { post.Owner = user; return post; }).ToList();
                var p = data.First();

                p.Content.IsEqualTo("Sams Post1");
                p.Id.IsEqualTo(1);
                p.Owner.Name.IsEqualTo("Sam");
                p.Owner.Id.IsEqualTo(99);

                data[2].Owner.IsNull();

                await connection.Execute("drop table #Users drop table #Posts");
            }
        }

        [Test]
        public async Task TestMultiMapGridReaderAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var createSql = @"
                create table #Users (Id int, Name varchar(20))
                create table #Posts (Id int, OwnerId int, Content varchar(20))

                insert #Users values(99, 'Sam')
                insert #Users values(2, 'I am')

                insert #Posts values(1, 99, 'Sams Post1')
                insert #Posts values(2, 99, 'Sams Post2')
                insert #Posts values(3, null, 'no ones post')
";
                await connection.Execute(createSql);

                var sql =
    @"select p.*, u.Id, u.Name + '0' Name from #Posts p 
left join #Users u on u.Id = p.OwnerId 
Order by p.Id

select p.*, u.Id, u.Name + '1' Name from #Posts p 
left join #Users u on u.Id = p.OwnerId 
Order by p.Id
";

                var grid = await connection.QueryMultipleAsync(sql);

                for (int i = 0; i < 2; i++)
                {
                    var data = await grid.Read<Post, User, Post>((post, user) => { post.Owner = user; return post; }).ToList();
                    var p = data.First();

                    p.Content.IsEqualTo("Sams Post1");
                    p.Id.IsEqualTo(1);
                    p.Owner.Name.IsEqualTo("Sam" + i);
                    p.Owner.Id.IsEqualTo(99);

                    data[2].Owner.IsNull();
                }

                await connection.Execute("drop table #Users drop table #Posts");
            }
        }

        [Test]
        public async Task TestQueryMultipleAsyncBuffered()
        {
            using (var connection = GetOpenConnection())
            {
                using (var grid = await connection.QueryMultipleAsync("select 1; select 2; select @x; select 4", new { x = 3 }))
                {
                    var a = grid.Read<int>();
                    var b = grid.Read<int>();
                    var c = grid.Read<int>();
                    var d = grid.Read<int>();

                    (await a.SingleAsync()).IsEqualTo(1);
                    (await b.SingleAsync()).IsEqualTo(2);
                    (await c.SingleAsync()).IsEqualTo(3);
                    (await d.SingleAsync()).IsEqualTo(4);
                }
            }
        }

        [Test]
        public async Task TestQueryMultipleAsyncNonBufferedIncorrectOrder()
        {
            using (var connection = GetOpenConnection())
            {
                using (var grid = await connection.QueryMultipleAsync("select 1; select 2; select @x; select 4", new { x = 3 }))
                {
                    var a = await grid.Read<int>();
                    try
                    {
                        var b = await grid.Read<int>();
                        throw new InvalidOperationException(); // should have thrown
                    }
                    catch (InvalidOperationException)
                    {
                        // that's expected
                    }
                }
            }
        }

        [Test]
        public async Task TestQueryMultipleAsyncNonBufferedCcorrectOrder()
        {
            using (var connection = GetOpenConnection())
            {
                using (var grid = await connection.QueryMultipleAsync("select 1; select 2; select @x; select 4", new { x = 3 }))
                {
                    var a = await grid.Read<int>().SingleAsync();
                    var b = await grid.Read<int>().SingleAsync();
                    var c = await grid.Read<int>().SingleAsync();
                    var d = await grid.Read<int>().SingleAsync();

                    a.IsEqualTo(1);
                    b.IsEqualTo(2);
                    c.IsEqualTo(3);
                    d.IsEqualTo(4);
                }
            }
        }

        [Test]
        public async Task TestMultiMapDynamicAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var createSql = @"
                create table #Users (Id int, Name varchar(20))
                create table #Posts (Id int, OwnerId int, Content varchar(20))

                insert #Users values(99, 'Sam')
                insert #Users values(2, 'I am')

                insert #Posts values(1, 99, 'Sams Post1')
                insert #Posts values(2, 99, 'Sams Post2')
                insert #Posts values(3, null, 'no ones post')
";
                await connection.Execute(createSql);

                var sql =
    @"select * from #Posts p 
left join #Users u on u.Id = p.OwnerId 
Order by p.Id";

                var data = await connection.Query<dynamic, dynamic, dynamic>(sql, (post, user) => { post.Owner = user; return post; }).ToList();
                var p = data.First();

                // hairy extension method support for dynamics
                ((string)p.Content).IsEqualTo("Sams Post1");
                ((int)p.Id).IsEqualTo(1);
                ((string)p.Owner.Name).IsEqualTo("Sam");
                ((int)p.Owner.Id).IsEqualTo(99);

                ((object)data[2].Owner).IsNull();

                await connection.Execute("drop table #Users drop table #Posts");
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

        [Test]
        public async Task TestMultiMapWithSplit() // http://stackoverflow.com/q/6056778/233Async54
        {
            using (var connection = GetOpenConnection())
            {
                var sql = @"select 1 as id, 'abc' as name, 2 as id, 'def' as name";
                var product = await connection.Query<Product, Category, Product>(sql, (prod, cat) =>
                    {
                        prod.Category = cat;
                        return prod;
                    }).FirstAsync();
                // assertions
                product.Id.IsEqualTo(1);
                product.Name.IsEqualTo("abc");
                product.Category.Id.IsEqualTo(2);
                product.Category.Name.IsEqualTo("def");
            }
        }

        [Test]
        public async Task TestMultiMapWithSplitWithNullValue() // http://stackoverflow.com/q/10744728/4499Async06
        {
            using (var connection = GetOpenConnection())
            {
                var sql = @"select 1 as id, 'abc' as name, NULL as description, 'def' as name";
                var product = await connection.Query<Product, Category, Product>(sql, (prod, cat) =>
                    {
                        prod.Category = cat;
                        return prod;
                    }, splitOn: "description").FirstAsync();
                // assertions
                product.Id.IsEqualTo(1);
                product.Name.IsEqualTo("abc");
                product.Category.IsNull();
            }
        }

        [Test]
        public async Task TestMultiMapWithSplitWithNullValueAndSpoofColumn() // http://stackoverflow.com/q/10744728/4499Async06
        {
            using (var connection = GetOpenConnection())
            {
                var sql = @"select 1 as id, 'abc' as name, 1 as spoof, NULL as description, 'def' as name";
                var product = await connection.Query<Product, Category, Product>(sql, (prod, cat) =>
                    {
                        prod.Category = cat;
                        return prod;
                    }, splitOn: "spoof").FirstAsync();
                // assertions
                product.Id.IsEqualTo(1);
                product.Name.IsEqualTo("abc");
                product.Category.IsNotNull();
                product.Category.Id.IsEqualTo(0);
                product.Category.Name.IsEqualTo("def");
                product.Category.Description.IsNull();
            }
        }

        [Test]
        public async Task TestFieldsAndPrivatesAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var data = await connection.Query<TestFieldCaseAndPrivatesEntity>(
                    @"select a=1,b=2,c=3,d=4,f='5'").SingleAsync();

                data.a.IsEqualTo(1);
                data.GetB().IsEqualTo(2);
                data.c.IsEqualTo(3);
                data.GetD().IsEqualTo(4);
                data.e.IsEqualTo(5);
            }
        }

        private class TestFieldCaseAndPrivatesEntity
        {
            public int a { get; set; }
            private int b { get; set; }
            public int GetB() { return b; }
            public int c = 0;
            private int d = 0;
            public int GetD() { return d; }
            public int e { get; set; }
            private string f
            {
                get { return e.ToString(); }
                set { e = int.Parse(value); }
            }
        }

        [Test]
        public async Task TestMultiReaderBasicAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var sql = @"select 1 as Id union all select 2 as Id     select 'abc' as name   select 1 as Id union all select 2 as Id";
                int i, j;
                string s;
                using (var multi = await connection.QueryMultipleAsync(sql))
                {
                    i = await multi.Read<int>().FirstAsync();
                    s = await multi.Read<string>().SingleAsync();
                    j = await multi.Read<int>().Sum().FirstAsync();
                }
                Assert.IsEqualTo(i, 1);
                Assert.IsEqualTo(s, "abc");
                Assert.IsEqualTo(j, 3);
            }
        }

        [Test]
        public async Task TestMultiMappingVariationsAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var sql = @"select 1 as Id, 'a' as Content, 2 as Id, 'b' as Content, 3 as Id, 'c' as Content, 4 as Id, 'd' as Content, 5 as Id, 'e' as Content";

                var order = await connection.Query<dynamic, dynamic, dynamic, dynamic>(sql, (o, owner, creator) => { o.Owner = owner; o.Creator = creator; return o; }).FirstAsync();

                Assert.IsEqualTo(order.Id, 1);
                Assert.IsEqualTo(order.Content, "a");
                Assert.IsEqualTo(order.Owner.Id, 2);
                Assert.IsEqualTo(order.Owner.Content, "b");
                Assert.IsEqualTo(order.Creator.Id, 3);
                Assert.IsEqualTo(order.Creator.Content, "c");

                order = await connection.Query<dynamic, dynamic, dynamic, dynamic, dynamic>(sql, (o, owner, creator, address) =>
                {
                    o.Owner = owner;
                    o.Creator = creator;
                    o.Owner.Address = address;
                    return o;
                }).FirstAsync();

                Assert.IsEqualTo(order.Id, 1);
                Assert.IsEqualTo(order.Content, "a");
                Assert.IsEqualTo(order.Owner.Id, 2);
                Assert.IsEqualTo(order.Owner.Content, "b");
                Assert.IsEqualTo(order.Creator.Id, 3);
                Assert.IsEqualTo(order.Creator.Content, "c");
                Assert.IsEqualTo(order.Owner.Address.Id, 4);
                Assert.IsEqualTo(order.Owner.Address.Content, "d");

                order = await connection.Query<dynamic, dynamic, dynamic, dynamic, dynamic, dynamic>(sql, (a, b, c, d, e) => { a.B = b; a.C = c; a.C.D = d; a.E = e; return a; }).FirstAsync();

                Assert.IsEqualTo(order.Id, 1);
                Assert.IsEqualTo(order.Content, "a");
                Assert.IsEqualTo(order.B.Id, 2);
                Assert.IsEqualTo(order.B.Content, "b");
                Assert.IsEqualTo(order.C.Id, 3);
                Assert.IsEqualTo(order.C.Content, "c");
                Assert.IsEqualTo(order.C.D.Id, 4);
                Assert.IsEqualTo(order.C.D.Content, "d");
                Assert.IsEqualTo(order.E.Id, 5);
                Assert.IsEqualTo(order.E.Content, "e");
            }
        }

        class InheritanceTest1
        {
            public string Base1 { get; set; }
            public string Base2 { get; private set; }
        }

        class InheritanceTest2 : InheritanceTest1
        {
            public string Derived1 { get; set; }
            public string Derived2 { get; private set; }
        }

        [Test]
        public async Task TestInheritanceAsync()
        {
            using (var connection = GetOpenConnection())
            {
                // Test that inheritance works.
                var list = await connection.Query<InheritanceTest2>("select 'One' as Derived1, 'Two' as Derived2, 'Three' as Base1, 'Four' as Base2").ToList();
                list.First().Derived1.IsEqualTo("One");
                list.First().Derived2.IsEqualTo("Two");
                list.First().Base1.IsEqualTo("Three");
                list.First().Base2.IsEqualTo("Four");
            }
        }


        public class PostCE
        {
            public int ID { get; set; }
            public string Title { get; set; }
            public string Body { get; set; }

            public AuthorCE Author { get; set; }
        }

        public class AuthorCE
        {
            public int ID { get; set; }
            public string Name { get; set; }
        }

        [Test]
        public async Task MultiRSSqlCEAsync()
        {
            if (File.Exists("Test.sdf"))
                File.Delete("Test.sdf");

            var cnnStr = "Data Source = Test.sdf;";
            var engine = new SqlCeEngine(cnnStr);
            engine.CreateDatabase();

            using (var cnn = new SqlCeConnection(cnnStr))
            {
                await cnn.OpenAsync();

                await cnn.Execute("create table Posts (ID int, Title nvarchar(50), Body nvarchar(50), AuthorID int)");
                await cnn.Execute("create table Authors (ID int, Name nvarchar(50))");

                await cnn.Execute("insert Posts values (1,'title','body',1)");
                await cnn.Execute("insert Posts values(2,'title2','body2',null)");
                await cnn.Execute("insert Authors values(1,'sam')");

                var data = await cnn.Query<PostCE, AuthorCE, PostCE>(@"select * from Posts p left join Authors a on a.ID = p.AuthorID", (post, author) => { post.Author = author; return post; }).ToList();
                var firstPost = data.First();
                firstPost.Title.IsEqualTo("title");
                firstPost.Author.Name.IsEqualTo("sam");
                data[1].Author.IsNull();
                cnn.Close();
            }
        }

        enum TestEnum : byte
        {
            Bla = 1
        }
        class TestEnumClass
        {
            public TestEnum? EnumEnum { get; set; }
        }
        class TestEnumClassNoNull
        {
            public TestEnum EnumEnum { get; set; }
        }

        [Test]
        public async Task TestEnumWeirdnessAsync()
        {
            using (var connection = GetOpenConnection())
            {
                (await connection.Query<TestEnumClass>("select cast(1 as tinyint) as [EnumEnum]").FirstAsync()).EnumEnum.IsEqualTo(TestEnum.Bla);
                (await connection.Query<TestEnumClass>("select null as [EnumEnum]").FirstAsync()).EnumEnum.IsEqualTo(null);
            }
        }

        [Test]
        public async Task TestEnumStringsAsync()
        {
            using (var connection = GetOpenConnection())
            {
                (await connection.Query<TestEnumClassNoNull>("select 'BLA' as [EnumEnum]").FirstAsync()).EnumEnum.IsEqualTo(TestEnum.Bla);
                (await connection.Query<TestEnumClassNoNull>("select 'bla' as [EnumEnum]").FirstAsync()).EnumEnum.IsEqualTo(TestEnum.Bla);

                (await connection.Query<TestEnumClass>("select 'BLA' as [EnumEnum]").FirstAsync()).EnumEnum.IsEqualTo(TestEnum.Bla);
                (await connection.Query<TestEnumClass>("select 'bla' as [EnumEnum]").FirstAsync()).EnumEnum.IsEqualTo(TestEnum.Bla);
            }
        }

        [Test]
        public async Task TestSupportForDynamicParametersAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var p = new DynamicParameters();
                p.Add("name", "bob");
                p.Add("age", dbType: DbType.Int32, direction: ParameterDirection.Output);

                (await connection.Query<string>("set @age = 11 select @name", p).FirstAsync()).IsEqualTo("bob");

                p.Get<int>("age").IsEqualTo(11);
            }
        }

        [Test]
        public async Task TestSupportForExpandoObjectParametersAsync()
        {
            using (var connection = GetOpenConnection())
            {
                dynamic p = new ExpandoObject();
                p.name = "bob";
                object parameters = p;
                string result = await connection.Query<string>("select @name", parameters).FirstAsync();
                result.IsEqualTo("bob");
            }
        }

        [Test]
        public async Task TestProcSupportAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var p = new DynamicParameters();
                p.Add("a", 11);
                p.Add("b", dbType: DbType.Int32, direction: ParameterDirection.Output);
                p.Add("c", dbType: DbType.Int32, direction: ParameterDirection.ReturnValue);

                await connection.Execute(@"create proc #TestProc 
	@a int,
	@b int output
as 
begin
	set @b = 999
	select 1111
	return @a
end");
                (await connection.Query<int>("#TestProc", p, commandType: CommandType.StoredProcedure).FirstAsync())
                    .IsEqualTo(1111);

                p.Get<int>("c").IsEqualTo(11);
                p.Get<int>("b").IsEqualTo(999);
            }
        }

        [Test]
        public async Task TestDbStringAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var obj =
                    await
                    connection.Query(
                        "select datalength(@a) as a, datalength(@b) as b, datalength(@c) as c, datalength(@d) as d, datalength(@e) as e, datalength(@f) as f",
                        new
                            {
                                a = new DbString {Value = "abcde", IsFixedLength = true, Length = 10, IsAnsi = true},
                                b = new DbString {Value = "abcde", IsFixedLength = true, Length = 10, IsAnsi = false},
                                c = new DbString {Value = "abcde", IsFixedLength = false, Length = 10, IsAnsi = true},
                                d = new DbString {Value = "abcde", IsFixedLength = false, Length = 10, IsAnsi = false},
                                e = new DbString {Value = "abcde", IsAnsi = true},
                                f = new DbString {Value = "abcde", IsAnsi = false},
                            }).FirstAsync();
                ((int) obj.a).IsEqualTo(10);
                ((int) obj.b).IsEqualTo(20);
                ((int) obj.c).IsEqualTo(5);
                ((int) obj.d).IsEqualTo(10);
                ((int) obj.e).IsEqualTo(5);
                ((int) obj.f).IsEqualTo(10);
            }
        }

        class Person
        {
            public int PersonId { get; set; }
            public string Name { get; set; }
        }

        class Address
        {
            public int AddressId { get; set; }
            public string Name { get; set; }
            public int PersonId { get; set; }
        }

        class Extra
        {
            public int Id { get; set; }
            public string Name { get; set; }
        }

        [Test]
        public async Task TestFlexibleMultiMappingAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var sql =
                    @"select 
    1 as PersonId, 'bob' as Name, 
    2 as AddressId, 'abc street' as Name, 1 as PersonId,
    3 as Id, 'fred' as Name
    ";
                var personWithAddress = await connection.Query<Person, Address, Extra, Tuple<Person, Address, Extra>>
                                                  (sql, (p, a, e) => Tuple.Create(p, a, e), splitOn: "AddressId,Id")
                                                        .FirstAsync();

                personWithAddress.Item1.PersonId.IsEqualTo(1);
                personWithAddress.Item1.Name.IsEqualTo("bob");
                personWithAddress.Item2.AddressId.IsEqualTo(2);
                personWithAddress.Item2.Name.IsEqualTo("abc street");
                personWithAddress.Item2.PersonId.IsEqualTo(1);
                personWithAddress.Item3.Id.IsEqualTo(3);
                personWithAddress.Item3.Name.IsEqualTo("fred");
            }
        }

        [Test]
        public async Task TestMultiMappingWithSplitOnSpaceBetweenCommasAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var sql = @"select 
                        1 as PersonId, 'bob' as Name, 
                        2 as AddressId, 'abc street' as Name, 1 as PersonId,
                        3 as Id, 'fred' as Name
                        ";
                var personWithAddress = await connection.Query<Person, Address, Extra, Tuple<Person, Address, Extra>>
                                                  (sql, (p, a, e) => Tuple.Create(p, a, e), splitOn: "AddressId, Id")
                                                        .FirstAsync();

                personWithAddress.Item1.PersonId.IsEqualTo(1);
                personWithAddress.Item1.Name.IsEqualTo("bob");
                personWithAddress.Item2.AddressId.IsEqualTo(2);
                personWithAddress.Item2.Name.IsEqualTo("abc street");
                personWithAddress.Item2.PersonId.IsEqualTo(1);
                personWithAddress.Item3.Id.IsEqualTo(3);
                personWithAddress.Item3.Name.IsEqualTo("fred");
            }
        }

        [Test]
        public async Task TestFastExpandoSupportsIDictionaryAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var row = (await connection.Query("select 1 A, 'two' B").FirstAsync()) as IDictionary<string, object>;
                row["A"].IsEqualTo(1);
                row["B"].IsEqualTo("two");
            }
        }

        class PrivateDan
        {
            public int Shadow { get; set; }
            private string ShadowInDB
            {
                set
                {
                    Shadow = value == "one" ? 1 : 0;
                }
            }
        }

        [Test]
        public async Task TestDapperSetsPrivatesAsync()
        {
            using (var connection = GetOpenConnection())
            {
                (await connection.Query<PrivateDan>("select 'one' ShadowInDB").FirstAsync()).Shadow.IsEqualTo(1);
            }
        }


        class IntDynamicParam : SqlMapper.IDynamicParameters
        {
            IEnumerable<int> numbers;
            public IntDynamicParam(IEnumerable<int> numbers)
            {
                this.numbers = numbers;
            }

            public void AddParameters(DbCommand command, SqlMapper.Identity identity)
            {
                var sqlCommand = (SqlCommand)command;
                sqlCommand.CommandType = CommandType.StoredProcedure;

                List<Microsoft.SqlServer.Server.SqlDataRecord> number_list = new List<Microsoft.SqlServer.Server.SqlDataRecord>();

                // Create an SqlMetaData object that describes our table type.
                Microsoft.SqlServer.Server.SqlMetaData[] tvp_definition = { new Microsoft.SqlServer.Server.SqlMetaData("n", SqlDbType.Int) };

                foreach (int n in numbers)
                {
                    // Create a new record, using the metadata array above.
                    Microsoft.SqlServer.Server.SqlDataRecord rec = new Microsoft.SqlServer.Server.SqlDataRecord(tvp_definition);
                    rec.SetInt32(0, n);    // Set the value.
                    number_list.Add(rec);      // Add it to the list.
                }

                // Add the table parameter.
                var p = sqlCommand.Parameters.Add("ints", SqlDbType.Structured);
                p.Direction = ParameterDirection.Input;
                p.TypeName = "int_list_type";
                p.Value = number_list;

            }
        }

        // SQL Server specific test to demonstrate TVP 
        [Test]
        public async Task TestTVPAsync()
        {
            using (var connection = GetOpenConnection())
            {
                try
                {
                    await connection.Execute("CREATE TYPE int_list_type AS TABLE (n int NOT NULL PRIMARY KEY)");
                    await connection.Execute("CREATE PROC get_ints @ints int_list_type READONLY AS select * from @ints");

                    var nums =
                        await connection.Query<int>("get_ints", new IntDynamicParam(new int[] {1, 2, 3})).ToList();
                    nums[0].IsEqualTo(1);
                    nums[1].IsEqualTo(2);
                    nums[2].IsEqualTo(3);
                    nums.Count.IsEqualTo(3);

                }
                finally
                {
                    try
                    {
                        connection.Execute("DROP PROC get_ints").Wait();
                    }
                    finally
                    {
                        connection.Execute("DROP TYPE int_list_type").Wait();
                    }
                }
            }
        }

        class DynamicParameterWithIntTVP : DynamicParameters, SqlMapper.IDynamicParameters
        {
            IEnumerable<int> numbers;
            public DynamicParameterWithIntTVP(IEnumerable<int> numbers)
            {
                this.numbers = numbers;
            }

            public new void AddParameters(DbCommand command, SqlMapper.Identity identity)
            {
                base.AddParameters(command, identity);

                var sqlCommand = (SqlCommand)command;
                sqlCommand.CommandType = CommandType.StoredProcedure;

                List<Microsoft.SqlServer.Server.SqlDataRecord> number_list = new List<Microsoft.SqlServer.Server.SqlDataRecord>();

                // Create an SqlMetaData object that describes our table type.
                Microsoft.SqlServer.Server.SqlMetaData[] tvp_definition = { new Microsoft.SqlServer.Server.SqlMetaData("n", SqlDbType.Int) };

                foreach (int n in numbers)
                {
                    // Create a new record, using the metadata array above.
                    Microsoft.SqlServer.Server.SqlDataRecord rec = new Microsoft.SqlServer.Server.SqlDataRecord(tvp_definition);
                    rec.SetInt32(0, n);    // Set the value.
                    number_list.Add(rec);      // Add it to the list.
                }

                // Add the table parameter.
                var p = sqlCommand.Parameters.Add("ints", SqlDbType.Structured);
                p.Direction = ParameterDirection.Input;
                p.TypeName = "int_list_type";
                p.Value = number_list;

            }
        }

        [Test]
        public async Task TestTVPWithAdditionalParamsAsync()
        {
            using (var connection = GetOpenConnection())
            {
                try
                {
                    await connection.Execute("CREATE TYPE int_list_type AS TABLE (n int NOT NULL PRIMARY KEY)");
                    await connection.Execute("CREATE PROC get_values @ints int_list_type READONLY, @stringParam varchar(20), @dateParam datetime AS select i.*, @stringParam as stringParam, @dateParam as dateParam from @ints i");

                    var dynamicParameters = new DynamicParameterWithIntTVP(new int[] { 1, 2, 3 });
                    dynamicParameters.AddDynamicParams(new { stringParam = "stringParam", dateParam = new DateTime(2012, 1, 1) });

                    var results = await connection.Query("get_values", dynamicParameters, commandType: CommandType.StoredProcedure).ToList();
                    results.Count.IsEqualTo(3);
                    for (int i = 0; i < results.Count; i++)
                    {
                        var result = results[i];
                        Assert.IsEqualTo(i + 1, result.n);
                        Assert.IsEqualTo("stringParam", result.stringParam);
                        Assert.IsEqualTo(new DateTime(2012, 1, 1), result.dateParam);
                    }

                }
                finally
                {
                    try
                    {
                        connection.Execute("DROP PROC get_values").Wait();
                    }
                    finally
                    {
                        connection.Execute("DROP TYPE int_list_type").Wait();
                    }
                }
            }
        }

        class IntCustomParam : SqlMapper.ICustomQueryParameter
        {
            IEnumerable<int> numbers;
            public IntCustomParam(IEnumerable<int> numbers)
            {
                this.numbers = numbers;
            }

            public void AddParameter(DbCommand command, string name)
            {
                var sqlCommand = (SqlCommand)command;
                sqlCommand.CommandType = CommandType.StoredProcedure;

                List<Microsoft.SqlServer.Server.SqlDataRecord> number_list = new List<Microsoft.SqlServer.Server.SqlDataRecord>();

                // Create an SqlMetaData object that describes our table type.
                Microsoft.SqlServer.Server.SqlMetaData[] tvp_definition = { new Microsoft.SqlServer.Server.SqlMetaData("n", SqlDbType.Int) };

                foreach (int n in numbers)
                {
                    // Create a new record, using the metadata array above.
                    Microsoft.SqlServer.Server.SqlDataRecord rec = new Microsoft.SqlServer.Server.SqlDataRecord(tvp_definition);
                    rec.SetInt32(0, n);    // Set the value.
                    number_list.Add(rec);      // Add it to the list.
                }

                // Add the table parameter.
                var p = sqlCommand.Parameters.Add(name, SqlDbType.Structured);
                p.Direction = ParameterDirection.Input;
                p.TypeName = "int_list_type";
                p.Value = number_list;
            }
        }

        [Test]
        public async Task TestTVPWithAnonymousObjectAsync()
        {
            using (var connection = GetOpenConnection())
            {
                try
                {
                    await connection.Execute("CREATE TYPE int_list_type AS TABLE (n int NOT NULL PRIMARY KEY)");
                    await connection.Execute("CREATE PROC get_ints @integers int_list_type READONLY AS select * from @integers");

                    var nums = await connection.Query<int>("get_ints", new { integers = new IntCustomParam(new int[] { 1, 2, 3 }) }, commandType: CommandType.StoredProcedure).ToList();
                    nums[0].IsEqualTo(1);
                    nums[1].IsEqualTo(2);
                    nums[2].IsEqualTo(3);
                    nums.Count.IsEqualTo(3);

                }
                finally
                {
                    try
                    {
                        connection.Execute("DROP PROC get_ints").Wait();
                    }
                    finally
                    {
                        connection.Execute("DROP TYPE int_list_type").Wait();
                    }
                }
            }
        }

        class Parent
        {
            public int Id { get; set; }
            public readonly List<Child> Children = new List<Child>();
        }
        class Child
        {
            public int Id { get; set; }
        }

        [Test]
        public async Task ParentChildIdentityAssociationsAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var lookup = new Dictionary<int, Parent>();
                var parents = await connection.Query<Parent, Child, Parent>(@"select 1 as [Id], 1 as [Id] union all select 1,2 union all select 2,3 union all select 1,4 union all select 3,5",
                    (parent, child) =>
                    {
                        Parent found;
                        if (!lookup.TryGetValue(parent.Id, out found))
                        {
                            lookup.Add(parent.Id, found = parent);
                        }
                        found.Children.Add(child);
                        return found;
                    }).Distinct().ToDictionary(p => p.Id);
                parents.Count().IsEqualTo(3);
                parents[1].Children.Select(c => c.Id).SequenceEqual(new[] { 1, 2, 4 }).IsTrue();
                parents[2].Children.Select(c => c.Id).SequenceEqual(new[] { 3 }).IsTrue();
                parents[3].Children.Select(c => c.Id).SequenceEqual(new[] { 5 }).IsTrue();
            }
        }


        /* TODO:
         * 
        public void TestMagicParam()
        {
            // magic params allow you to pass in single params without using an anon class
            // this test fails for now, but I would like to support a single param by parsing the sql with regex and remapping. 

            var first = connection.Query("select @a as a", 1).First();
            Assert.IsEqualTo(first.a, 1);
        }
         * */

        class WithBizarreData
        {
            public GenericUriParser Foo { get; set; }
            public int Bar { get; set; }
        }

        [Test]
        public async Task TestUnexpectedDataMessageAsync()
        {
            using (var connection = GetOpenConnection())
            {
                string msg = null;
                try
                {
                    await connection.Query<int>("select count(1) where 1 = @Foo", new WithBizarreData { Foo = new GenericUriParser(GenericUriParserOptions.Default), Bar = 23 }).FirstAsync();

                }
                catch (Exception ex)
                {
                    msg = ex.Message;
                }
                msg.IsEqualTo("The member Foo of type System.GenericUriParser cannot be used as a parameter value");
            }
        }

        [Test]
        public async Task TestUnexpectedButFilteredDataMessageAsync()
        {
            using (var connection = GetOpenConnection())
            {
                int i = await connection.Query<int>("select @Bar", new WithBizarreData { Foo = new GenericUriParser(GenericUriParserOptions.Default), Bar = 23 }).SingleAsync();

                i.IsEqualTo(23);
            }
        }

        class WithCharValue
        {
            public char Value { get; set; }
            public char? ValueNullable { get; set; }
        }

        [Test]
        public async Task TestCharInputAndOutputAsync()
        {
            using (var connection = GetOpenConnection())
            {
                const char test = '〠';
                char c = await connection.Query<char>("select @c", new { c = test }).SingleAsync();

                c.IsEqualTo(test);

                var obj = await connection.Query<WithCharValue>("select @Value as Value", new WithCharValue { Value = c }).SingleAsync();

                obj.Value.IsEqualTo(test);
            }
        }

        [Test]
        public async Task TestNullableCharInputAndOutputNonNullAsync()
        {
            using (var connection = GetOpenConnection())
            {
                char? test = '〠';
                char? c = await connection.Query<char?>("select @c", new { c = test }).SingleAsync();

                c.IsEqualTo(test);

                var obj = await connection.Query<WithCharValue>("select @ValueNullable as ValueNullable", new WithCharValue { ValueNullable = c }).SingleAsync();

                obj.ValueNullable.IsEqualTo(test);
            }
        }

        [Test]
        public async Task TestNullableCharInputAndOutputNullAsync()
        {
            using (var connection = GetOpenConnection())
            {
                char? test = null;
                char? c = await connection.Query<char?>("select @c", new { c = test }).SingleAsync();

                c.IsEqualTo(test);

                var obj = await connection.Query<WithCharValue>("select @ValueNullable as ValueNullable", new WithCharValue { ValueNullable = c }).SingleAsync();

                obj.ValueNullable.IsEqualTo(test);
            }
        }

        [Test]
        public async Task TestInvalidSplitCausesNiceErrorAsync()
        {
            using (var connection = GetOpenConnection())
            {
                try
                {
                    await connection.Query<User, User, User>("select 1 A, 2 B, 3 C", (x, y) => x);
                }
                catch (ArgumentException)
                {
                    // expecting an app exception due to multi mapping being bodged 
                }

                try
                {
                    await connection.Query<dynamic, dynamic, dynamic>("select 1 A, 2 B, 3 C", (x, y) => x);
                }
                catch (ArgumentException)
                {
                    // expecting an app exception due to multi mapping being bodged 
                }
            }
        }
        
        class Comment
        {
            public int Id { get; set; }
            public string CommentData { get; set; }
        }

        [Test]
        public async Task TestMultiMapThreeTypesWithGridReaderAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var createSql = @"
                create table #Users (Id int, Name varchar(20))
                create table #Posts (Id int, OwnerId int, Content varchar(20))
                create table #Comments (Id int, PostId int, CommentData varchar(20))

                insert #Users values(99, 'Sam')
                insert #Users values(2, 'I am')

                insert #Posts values(1, 99, 'Sams Post1')
                insert #Posts values(2, 99, 'Sams Post2')
                insert #Posts values(3, null, 'no ones post')

                insert #Comments values(1, 1, 'Comment 1')";
                await connection.Execute(createSql);

                var sql = @"SELECT p.* FROM #Posts p

select p.*, u.Id, u.Name + '0' Name, c.Id, c.CommentData from #Posts p 
left join #Users u on u.Id = p.OwnerId 
left join #Comments c on c.postId = p.Id
where p.Id = 1
Order by p.Id";

                var grid = await connection.QueryMultipleAsync(sql);

                var post1 = await grid.Read<Post>().ToList();

                var post2 = await grid.Read<Post, User, Comment, Post>((post, user, comment) => { post.Owner = user; post.Comment = comment; return post; }).SingleOrDefaultAsync();

                post2.Comment.Id.IsEqualTo(1);
                post2.Owner.Id.IsEqualTo(99);


                await connection.Execute("drop table #Users drop table #Posts drop table #Comments");
            }
        }

        [Test]
        public async Task TestReadDynamicWithGridReaderAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var createSql = @"
                create table #Users (Id int, Name varchar(20))
                create table #Posts (Id int, OwnerId int, Content varchar(20))

                insert #Users values(99, 'Sam')
                insert #Users values(2, 'I am')

                insert #Posts values(1, 99, 'Sams Post1')
                insert #Posts values(2, 99, 'Sams Post2')
                insert #Posts values(3, null, 'no ones post')";

                await connection.Execute(createSql);

                var sql = @"SELECT * FROM #Users ORDER BY Id
                        SELECT * FROM #Posts ORDER BY Id DESC";

                var grid = await connection.QueryMultipleAsync(sql);

                var users = await grid.Read().ToList();
                var posts = await grid.Read().ToList();

                users.Count.IsEqualTo(2);
                posts.Count.IsEqualTo(3);

                ((int) users.First().Id).IsEqualTo(2);
                ((int) posts.First().Id).IsEqualTo(3);

                await connection.Execute("drop table #Users drop table #Posts");
            }
        }

        [Test]
        public async Task TestDynamicParamNullSupportAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var p = new DynamicParameters();

                p.Add("@b", dbType: DbType.Int32, direction: ParameterDirection.Output);
                await connection.Execute("select @b = null", p);

                p.Get<int?>("@b").IsNull();
            }
        }
        class Foo1
        {
#pragma warning disable 0649
            public int Id;
#pragma warning restore 0649
            public int BarId { get; set; }
        }
        class Bar1
        {
#pragma warning disable 0649
            public int BarId;
#pragma warning restore 0649
            public string Name { get; set; }
        }

        [Test]
        public async Task TestMultiMapperIsNotConfusedWithUnorderedColsAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var result = await connection.Query<Foo1, Bar1, Tuple<Foo1, Bar1>>("select 1 as Id, 2 as BarId, 3 as BarId, 'a' as Name", (f, b) => Tuple.Create(f, b), splitOn: "BarId").FirstAsync();

                result.Item1.Id.IsEqualTo(1);
                result.Item1.BarId.IsEqualTo(2);
                result.Item2.BarId.IsEqualTo(3);
                result.Item2.Name.IsEqualTo("a");
            }
        }

        [Test]
        public async Task TestLinqBinaryToClassAsync()
        {
            using (var connection = GetOpenConnection())
            {
                byte[] orig = new byte[20];
                new Random(123456).NextBytes(orig);
                var input = new System.Data.Linq.Binary(orig);

                var output =
                    (await connection.Query<WithBinary>("select @input as [Value]", new {input}).FirstAsync()).Value;

                output.ToArray().IsSequenceEqualTo(orig);
            }
        }

        [Test]
        public async Task TestLinqBinaryRawAsync()
        {
            using (var connection = GetOpenConnection())
            {
                byte[] orig = new byte[20];
                new Random(123456).NextBytes(orig);
                var input = new System.Data.Linq.Binary(orig);

                var output = await connection.Query<System.Data.Linq.Binary>("select @input as [Value]", new { input }).FirstAsync();

                output.ToArray().IsSequenceEqualTo(orig);
            }
        }

        class WithBinary
        {
            public System.Data.Linq.Binary Value { get; set; }
        }
        
        class WithPrivateConstructor
        {
            public int Foo { get; set; }
            private WithPrivateConstructor() { }
        }

        [Test]
        public async Task TestWithNonPublicConstructorAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var output = await connection.Query<WithPrivateConstructor>("select 1 as Foo").FirstAsync();
                output.Foo.IsEqualTo(1);
            }
        }

        [Test]
        public async Task TestAppendingAnonClassesAsync()
        {
            using (var connection = GetOpenConnection())
            {
                DynamicParameters p = new DynamicParameters();
                p.AddDynamicParams(new {A = 1, B = 2});
                p.AddDynamicParams(new {C = 3, D = 4});

                var result = await connection.Query("select @A a,@B b,@C c,@D d", p).SingleAsync();

                ((int) result.a).IsEqualTo(1);
                ((int) result.b).IsEqualTo(2);
                ((int) result.c).IsEqualTo(3);
                ((int) result.d).IsEqualTo(4);
            }
        }

        [Test]
        public async Task TestAppendingADictionaryAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var dictionary = new Dictionary<string, object>();
                dictionary.Add("A", 1);
                dictionary.Add("B", "two");

                DynamicParameters p = new DynamicParameters();
                p.AddDynamicParams(dictionary);

                var result = await connection.Query("select @A a, @B b", p).SingleAsync();

                ((int) result.a).IsEqualTo(1);
                ((string) result.b).IsEqualTo("two");
            }
        }

        [Test]
        public async Task TestAppendingAnExpandoObjectAsync()
        {
            using (var connection = GetOpenConnection())
            {
                dynamic expando = new System.Dynamic.ExpandoObject();
                expando.A = 1;
                expando.B = "two";

                DynamicParameters p = new DynamicParameters();
                p.AddDynamicParams(expando);

                var result = await connection.Query("select @A a, @B b", p).SingleAsync();

                ((int) result.a).IsEqualTo(1);
                ((string) result.b).IsEqualTo("two");
            }
        }

        [Test]
        public async Task TestAppendingAListAsync()
        {
            using (var connection = GetOpenConnection())
            {
                DynamicParameters p = new DynamicParameters();
                var list = new int[] { 1, 2, 3 };
                p.AddDynamicParams(new { list });

                var result = await connection.Query<int>("select * from (select 1 A union all select 2 union all select 3) X where A in @list", p).ToList();

                result[0].IsEqualTo(1);
                result[1].IsEqualTo(2);
                result[2].IsEqualTo(3);
            }
        }

        [Test]
        public async Task TestAppendingAListAsDictionaryAsync()
        {
            using (var connection = GetOpenConnection())
            {
                DynamicParameters p = new DynamicParameters();
                var list = new int[] { 1, 2, 3 };
                var args = new Dictionary<string, object>();
                args.Add("ids", list);
                p.AddDynamicParams(args);

                var result = await connection.Query<int>("select * from (select 1 A union all select 2 union all select 3) X where A in @ids", p).ToList();

                result[0].IsEqualTo(1);
                result[1].IsEqualTo(2);
                result[2].IsEqualTo(3);
            }
        }

        [Test]
        public async Task TestAppendingAListByNameAsync()
        {
            using (var connection = GetOpenConnection())
            {
                DynamicParameters p = new DynamicParameters();
                var list = new int[] { 1, 2, 3 };
                p.Add("ids", list);

                var result = await connection.Query<int>("select * from (select 1 A union all select 2 union all select 3) X where A in @ids", p).ToList();

                result[0].IsEqualTo(1);
                result[1].IsEqualTo(2);
                result[2].IsEqualTo(3);
            }
        }

        [Test]
        public async Task TestUniqueIdentifierAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var guid = Guid.NewGuid();
                var result = await connection.Query<Guid>("declare @foo uniqueidentifier set @foo = @guid select @foo", new { guid }).SingleAsync();
                result.IsEqualTo(guid);
            }
        }

        [Test]
        public async Task TestNullableUniqueIdentifierNonNullAsync()
        {
            using (var connection = GetOpenConnection())
            {
                Guid? guid = Guid.NewGuid();
                var result = await connection.Query<Guid?>("declare @foo uniqueidentifier set @foo = @guid select @foo", new { guid }).SingleAsync();
                result.IsEqualTo(guid);
            }
        }

        [Test]
        public async Task TestNullableUniqueIdentifierNullAsync()
        {
            using (var connection = GetOpenConnection())
            {
                Guid? guid = null;
                var result = await connection.Query<Guid?>("declare @foo uniqueidentifier set @foo = @guid select @foo", new { guid }).SingleAsync();
                result.IsEqualTo(guid);
            }
        }

        [Test]
        public async Task WorkDespiteHavingWrongStructColumnTypesAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var hazInt = await connection.Query<CanHazInt>("select cast(1 as bigint) Value").SingleAsync();
                hazInt.Value.IsEqualTo(1);
            }
        }

        [Test]
        public async Task TestProcWithOutParameterAsync()
        {
            using (var connection = GetOpenConnection())
            {
                await connection.Execute(
                    @"CREATE PROCEDURE #TestProcWithOutParameter
        @ID int output,
        @Foo varchar(100),
        @Bar int
        AS
        SET @ID = @Bar + LEN(@Foo)");
                var obj = new
                    {
                        ID = 0,
                        Foo = "abc",
                        Bar = 4
                    };
                var args = new DynamicParameters(obj);
                args.Add("ID", 0, direction: ParameterDirection.Output);
                await connection.Execute("#TestProcWithOutParameter", args, commandType: CommandType.StoredProcedure);
                args.Get<int>("ID").IsEqualTo(7);
            }
        }

        [Test]
        public async Task TestProcWithOutAndReturnParameterAsync()
        {
            using (var connection = GetOpenConnection())
            {
                await connection.Execute(
                    @"CREATE PROCEDURE #TestProcWithOutAndReturnParameter
        @ID int output,
        @Foo varchar(100),
        @Bar int
        AS
        SET @ID = @Bar + LEN(@Foo)
        RETURN 42");
                var obj = new
                {
                    ID = 0,
                    Foo = "abc",
                    Bar = 4
                };
                var args = new DynamicParameters(obj);
                args.Add("ID", 0, direction: ParameterDirection.Output);
                args.Add("result", 0, direction: ParameterDirection.ReturnValue);
                await connection.Execute("#TestProcWithOutAndReturnParameter", args, commandType: CommandType.StoredProcedure);
                args.Get<int>("ID").IsEqualTo(7);
                args.Get<int>("result").IsEqualTo(42);
            }
        }
        struct CanHazInt
        {
            public int Value { get; set; }
        }

        [Test]
        public async Task TestInt16UsageAsync()
        {
            using (var connection = GetOpenConnection())
            {
                (await connection.Query<short>("select cast(42 as smallint)").SingleAsync()).IsEqualTo((short)42);
                (await connection.Query<short?>("select cast(42 as smallint)").SingleAsync()).IsEqualTo((short?)42);
                (await connection.Query<short?>("select cast(null as smallint)").SingleAsync()).IsEqualTo((short?)null);

                (await connection.Query<ShortEnum>("select cast(42 as smallint)").SingleAsync()).IsEqualTo((ShortEnum)42);
                (await connection.Query<ShortEnum?>("select cast(42 as smallint)").SingleAsync()).IsEqualTo((ShortEnum?)42);
                (await connection.Query<ShortEnum?>("select cast(null as smallint)").SingleAsync()).IsEqualTo((ShortEnum?)null);

                var row =
                    await connection.Query<WithInt16Values>(
                        "select cast(1 as smallint) as NonNullableInt16, cast(2 as smallint) as NullableInt16, cast(3 as smallint) as NonNullableInt16Enum, cast(4 as smallint) as NullableInt16Enum")
                        .SingleAsync();
                row.NonNullableInt16.IsEqualTo((short)1);
                row.NullableInt16.IsEqualTo((short)2);
                row.NonNullableInt16Enum.IsEqualTo(ShortEnum.Three);
                row.NullableInt16Enum.IsEqualTo(ShortEnum.Four);

                row =
        await connection.Query<WithInt16Values>(
            "select cast(5 as smallint) as NonNullableInt16, cast(null as smallint) as NullableInt16, cast(6 as smallint) as NonNullableInt16Enum, cast(null as smallint) as NullableInt16Enum")
            .SingleAsync();
                row.NonNullableInt16.IsEqualTo((short)5);
                row.NullableInt16.IsEqualTo((short?)null);
                row.NonNullableInt16Enum.IsEqualTo(ShortEnum.Six);
                row.NullableInt16Enum.IsEqualTo((ShortEnum?)null);
            }
        }

        [Test]
        public async Task TestInt32UsageAsync()
        {
            using (var connection = GetOpenConnection())
            {
                (await connection.Query<int>("select cast(42 as int)").SingleAsync()).IsEqualTo((int) 42);
                (await connection.Query<int?>("select cast(42 as int)").SingleAsync()).IsEqualTo((int?) 42);
                (await connection.Query<int?>("select cast(null as int)").SingleAsync()).IsEqualTo((int?) null);

                (await connection.Query<IntEnum>("select cast(42 as int)").SingleAsync()).IsEqualTo((IntEnum) 42);
                (await connection.Query<IntEnum?>("select cast(42 as int)").SingleAsync()).IsEqualTo((IntEnum?) 42);
                (await connection.Query<IntEnum?>("select cast(null as int)").SingleAsync()).IsEqualTo((IntEnum?) null);

                var row =
                    await connection.Query<WithInt32Values>(
                        "select cast(1 as int) as NonNullableInt32, cast(2 as int) as NullableInt32, cast(3 as int) as NonNullableInt32Enum, cast(4 as int) as NullableInt32Enum")
                                    .SingleAsync();
                row.NonNullableInt32.IsEqualTo((int) 1);
                row.NullableInt32.IsEqualTo((int) 2);
                row.NonNullableInt32Enum.IsEqualTo(IntEnum.Three);
                row.NullableInt32Enum.IsEqualTo(IntEnum.Four);

                row =
                    await connection.Query<WithInt32Values>(
                        "select cast(5 as int) as NonNullableInt32, cast(null as int) as NullableInt32, cast(6 as int) as NonNullableInt32Enum, cast(null as int) as NullableInt32Enum")
                                    .SingleAsync();
                row.NonNullableInt32.IsEqualTo((int) 5);
                row.NullableInt32.IsEqualTo((int?) null);
                row.NonNullableInt32Enum.IsEqualTo(IntEnum.Six);
                row.NullableInt32Enum.IsEqualTo((IntEnum?) null);
            }
        }

        public class WithInt16Values
        {
            public short NonNullableInt16 { get; set; }
            public short? NullableInt16 { get; set; }
            public ShortEnum NonNullableInt16Enum { get; set; }
            public ShortEnum? NullableInt16Enum { get; set; }
        }
        public enum ShortEnum : short
        {
            Zero = 0, One = 1, Two = 2, Three = 3, Four = 4, Five = 5, Six = 6
        }
        public class WithInt32Values
        {
            public int NonNullableInt32 { get; set; }
            public int? NullableInt32 { get; set; }
            public IntEnum NonNullableInt32Enum { get; set; }
            public IntEnum? NullableInt32Enum { get; set; }
        }
        public enum IntEnum : int
        {
            Zero = 0, One = 1, Two = 2, Three = 3, Four = 4, Five = 5, Six = 6
        }

        [Test]
        public async Task TestTransactionCommitAsync()
        {
            using (var connection = GetOpenConnection())
            {
                try
                {
                    await connection.Execute("create table #TransactionTest ([ID] int, [Value] varchar(32));");

                    using (var transaction = connection.BeginTransaction())
                    {
                        await connection.Execute("insert into #TransactionTest ([ID], [Value]) values (1, 'ABC');", transaction: transaction);

                        transaction.Commit();
                    }

                    (await connection.Query<int>("select count(*) from #TransactionTest;").SingleAsync()).IsEqualTo(1);
                }
                finally
                {
                    connection.Execute("drop table #TransactionTest;").Wait();
                }
            }
        }

        [Test]
        public async Task TestTransactionRollbackAsync()
        {
            using (var connection = GetOpenConnection())
            {
                await connection.Execute("create table #TransactionTest ([ID] int, [Value] varchar(32));");

                try
                {
                    using (var transaction = connection.BeginTransaction())
                    {
                        await connection.Execute("insert into #TransactionTest ([ID], [Value]) values (1, 'ABC');", transaction: transaction);

                        transaction.Rollback();
                    }

                    (await connection.Query<int>("select count(*) from #TransactionTest;").SingleAsync()).IsEqualTo(0);
                }
                finally
                {
                    connection.Execute("drop table #TransactionTest;").Wait();
                }
            }
        }

        [Test]
        public async Task TestReaderWhenResultsChangeAsync()
        {
            using (var connection = GetOpenConnection())
            {
                try
                {
                    await
                        connection.Execute(
                            "create table #ResultsChange (X int);create table #ResultsChange2 (Y int);insert #ResultsChange (X) values(1);insert #ResultsChange2 (Y) values(1);");

                    var obj1 = await connection.Query<ResultsChangeType>("select * from #ResultsChange").SingleAsync();
                    obj1.X.IsEqualTo(1);
                    obj1.Y.IsEqualTo(0);
                    obj1.Z.IsEqualTo(0);

                    var obj2 =
                        await
                        connection.Query<ResultsChangeType>(
                            "select * from #ResultsChange rc inner join #ResultsChange2 rc2 on rc2.Y=rc.X")
                                  .SingleAsync();
                    obj2.X.IsEqualTo(1);
                    obj2.Y.IsEqualTo(1);
                    obj2.Z.IsEqualTo(0);

                    await connection.Execute("alter table #ResultsChange add Z int null");
                    await connection.Execute("update #ResultsChange set Z = 2");

                    var obj3 = await connection.Query<ResultsChangeType>("select * from #ResultsChange").SingleAsync();
                    obj3.X.IsEqualTo(1);
                    obj3.Y.IsEqualTo(0);
                    obj3.Z.IsEqualTo(2);

                    var obj4 =
                        await
                        connection.Query<ResultsChangeType>(
                            "select * from #ResultsChange rc inner join #ResultsChange2 rc2 on rc2.Y=rc.X")
                                  .SingleAsync();
                    obj4.X.IsEqualTo(1);
                    obj4.Y.IsEqualTo(1);
                    obj4.Z.IsEqualTo(2);
                }
                finally
                {
                    connection.Execute("drop table #ResultsChange;drop table #ResultsChange2;").Wait();
                }
            }
        }
        class ResultsChangeType
        {
            public int X { get; set; }
            public int Y { get; set; }
            public int Z { get; set; }
        }

        [Test]
        public async Task TestCustomTypeMapAsync()
        {
            using (var connection = GetOpenConnection())
            {
                // default mapping
                var item = await connection.Query<TypeWithMapping>("Select 'AVal' as A, 'BVal' as B").SingleAsync();
                item.A.IsEqualTo("AVal");
                item.B.IsEqualTo("BVal");

                // custom mapping
                var map = new CustomPropertyTypeMap(typeof(TypeWithMapping),
                    (type, columnName) => type.GetProperties().Where(prop => prop.GetCustomAttributes(false).OfType<DescriptionAttribute>().Any(attr => attr.Description == columnName)).FirstOrDefault());
                SqlMapper.SetTypeMap(typeof(TypeWithMapping), map);

                item = await connection.Query<TypeWithMapping>("Select 'AVal' as A, 'BVal' as B").SingleAsync();
                item.A.IsEqualTo("BVal");
                item.B.IsEqualTo("AVal");

                // reset to default
                SqlMapper.SetTypeMap(typeof(TypeWithMapping), null);
                item = await connection.Query<TypeWithMapping>("Select 'AVal' as A, 'BVal' as B").SingleAsync();
                item.A.IsEqualTo("AVal");
                item.B.IsEqualTo("BVal");
            }
        }

        public class TypeWithMapping
        {
            [Description("B")]
            public string A { get; set; }

            [Description("A")]
            public string B { get; set; }
        }

        public class WrongTypes
        {
            public int A { get; set; }
            public double B { get; set; }
            public long C { get; set; }
            public bool D { get; set; }
        }

        [Test]
        public async Task TestWrongTypes_WithRightTypesAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var item = await connection.Query<WrongTypes>("select 1 as A, cast(2.0 as float) as B, cast(3 as bigint) as C, cast(1 as bit) as D").SingleAsync();
                item.A.IsEqualTo(1);
                item.B.IsEqualTo(2.0);
                item.C.IsEqualTo(3L);
                item.D.IsEqualTo(true);
            }
        }

        [Test]
        public async Task TestWrongTypes_WithWrongTypesAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var item = await connection.Query<WrongTypes>("select cast(1.0 as float) as A, 2 as B, 3 as C, cast(1 as bigint) as D").SingleAsync();
                item.A.IsEqualTo(1);
                item.B.IsEqualTo(2.0);
                item.C.IsEqualTo(3L);
                item.D.IsEqualTo(true);
            }
        }

        [Test]
        public async Task Test_AddDynamicParametersRepeatedShouldWorkAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var args = new DynamicParameters();
                args.AddDynamicParams(new {Foo = 123});
                args.AddDynamicParams(new {Foo = 123});
                int i = await connection.Query<int>("select @Foo", args).SingleAsync();
                i.IsEqualTo(123);
            }
        }

        public class ParameterWithIndexer
        {
            public int A { get; set; }
            public virtual string this[string columnName]
            {
                get { return null; }
                set { }
            }
        }

        [Test]
        public async Task TestParameterWithIndexerAsync()
        {
            using (var connection = GetOpenConnection())
            {
                await connection.Execute(@"create proc #TestProcWithIndexer 
	@A int
as 
begin
	select @A
end");
                var item = await connection.Query<int>("#TestProcWithIndexer", new ParameterWithIndexer(), commandType: CommandType.StoredProcedure).SingleAsync();
            }
        }

        [Test]
        public async Task Issue_40_AutomaticBoolConversionAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var user = await connection.Query<Issue40_User>("select UserId=1,Email='abc',Password='changeme',Active=cast(1 as tinyint)").SingleAsync();
                user.Active.IsTrue();
                user.UserID.IsEqualTo(1);
                user.Email.IsEqualTo("abc");
                user.Password.IsEqualTo("changeme");
            }
        }

        public class Issue40_User
        {
          public Issue40_User()
          {
             Email = Password = String.Empty;
          }
          public int UserID { get; set; }
          public string Email { get; set; }
          public string Password { get; set; }
          public bool Active { get; set; }
        }

        [Test]
        public async Task ExecuteFromClosedAsync()
        {
            using (var conn = GetClosedConnection())
            {
                await conn.Execute("-- nop");
                conn.State.IsEqualTo(ConnectionState.Closed);
            }
        }
        
        class Multi1
        {
            public int Id { get; set; }
        }
        
        class Multi2
        {
            public int Id { get; set; }
        }

        [Test]
        public async Task QueryMultimapFromClosedAsync()
        {
            using (var conn = GetClosedConnection())
            {
                conn.State.IsEqualTo(ConnectionState.Closed);
                var i = await conn.Query<Multi1, Multi2, int>("select 2 as [Id], 3 as [Id]", (x, y) => x.Id + y.Id).SingleAsync();
                conn.State.IsEqualTo(ConnectionState.Closed);
                i.IsEqualTo(5);
            }
        }

        [Test]
        public async Task QueryMultipleAsync2FromClosed()
        {
            using (var conn = GetClosedConnection())
            {
                conn.State.IsEqualTo(ConnectionState.Closed);
                using (var multi = await conn.QueryMultipleAsync("select 1 select 2 select 3"))
                {
                    (await multi.Read<int>().SingleAsync()).IsEqualTo(1);
                    (await multi.Read<int>().SingleAsync()).IsEqualTo(2);
                    // not reading 3 is intentional here
                }
                conn.State.IsEqualTo(ConnectionState.Closed);
            }
        }

        [Test]
        public async Task ExecuteInvalidFromClosedAsync()
        {
            using (var conn = GetClosedConnection())
            {
                try
                {
                    await conn.Execute("nop");
                    false.IsEqualTo(true); // shouldn't have got here
                }
                catch
                {
                    conn.State.IsEqualTo(ConnectionState.Closed);
                }
            }
        }

        [Test]
        public async Task QueryFromClosedAsync()
        {
            using (var conn = GetClosedConnection())
            {
                var i = await conn.Query<int>("select 1").SingleAsync();
                conn.State.IsEqualTo(ConnectionState.Closed);
                i.IsEqualTo(1);
            }
        }

        [Test]
        public async Task QueryInvalidFromClosedAsync()
        {
            using (var conn = GetClosedConnection())
            {
                try
                {
                    await conn.Query<int>("select gibberish").SingleAsync();
                    false.IsEqualTo(true); // shouldn't have got here
                }
                catch
                {
                    conn.State.IsEqualTo(ConnectionState.Closed);
                }
            }
        }

        [Test]
        public async Task QueryMultipleAsyncFromClosed()
        {
            using (var conn = GetClosedConnection())
            {
                using (var multi = await conn.QueryMultipleAsync("select 1; select 'abc';"))
                {
                    (await multi.Read<int>().SingleAsync()).IsEqualTo(1);
                    (await multi.Read<string>().SingleAsync()).IsEqualTo("abc");
                }
                conn.State.IsEqualTo(ConnectionState.Closed);
            }
        }

        [Test]
        public async Task QueryMultipleAsyncInvalidFromClosed()
        {
            using (var conn = GetClosedConnection())
            {
                try
                {
                    await conn.QueryMultipleAsync("select gibberish");
                    false.IsEqualTo(true); // shouldn't have got here
                }
                catch
                {
                    conn.State.IsEqualTo(ConnectionState.Closed);
                }
            }
        }

        [Test]
        public async Task TestMultiSelectAsyncWithSomeEmptyGrids()
        {
            using (var connection = GetOpenConnection())
            {
                using (var reader = await connection.QueryMultipleAsync("select 1; select 2 where 1 = 0; select 3 where 1 = 0; select 4;"))
                {
                    var one = await reader.Read<int>().ToArray();
                    var two = await reader.Read<int>().ToArray();
                    var three = await reader.Read<int>().ToArray();
                    var four = await reader.Read<int>().ToArray();
                    try
                    { // only returned four grids; expect a fifth read to fail
                        await reader.Read<int>();
                        throw new InvalidOperationException("this should not have worked!");
                    }
                    catch (ObjectDisposedException ex)
                    { // expected; success
                        ex.Message.IsEqualTo("The reader has been disposed; this can happen after all data has been consumed\r\nObject name: 'SqlChic.SqlMapper+GridReader'.");
                    }

                    one.Length.IsEqualTo(1);
                    one[0].IsEqualTo(1);
                    two.Length.IsEqualTo(0);
                    three.Length.IsEqualTo(0);
                    four.Length.IsEqualTo(1);
                    four[0].IsEqualTo(4);
                }
            }
        }

        [Test]
        public async Task TestDynamicMutation()
        {
            using (var connection = GetOpenConnection())
            {
                var obj = await connection.Query("select 1 as [a], 2 as [b], 3 as [c]").SingleAsync();
                ((int) obj.a).IsEqualTo(1);
                IDictionary<string, object> dict = obj;
                Assert.Equals(3, dict.Count);
                Assert.IsTrue(dict.Remove("a"));
                Assert.IsFalse(dict.Remove("d"));
                Assert.Equals(2, dict.Count);
                dict.Add("d", 4);
                Assert.Equals(3, dict.Count);
                Assert.Equals("b,c,d", string.Join(",", dict.Keys.OrderBy(x => x)));
                Assert.Equals("2,3,4", string.Join(",", dict.OrderBy(x => x.Key).Select(x => x.Value)));

                Assert.Equals(2, (int) obj.b);
                Assert.Equals(3, (int) obj.c);
                Assert.Equals(4, (int) obj.d);
                try
                {
                    ((int) obj.a).IsEqualTo(1);
                    throw new InvalidOperationException("should have thrown");
                }
                catch (RuntimeBinderException)
                {
                    // pass
                }
            }
        }

        [Test]
        public async Task TestIssue131Async()
        {
            using (var connection = GetOpenConnection())
            {
                var results = await connection.Query<dynamic, int, dynamic>(
                    "SELECT 1 Id, 'Mr' Title, 'John' Surname, 4 AddressCount",
                    (person, addressCount) =>
                        {
                            return person;
                        },
                    splitOn: "AddressCount"
                                        ).FirstOrDefaultAsync();

                var asDict = (IDictionary<string, object>) results;

                asDict.ContainsKey("Id").IsEqualTo(true);
                asDict.ContainsKey("Title").IsEqualTo(true);
                asDict.ContainsKey("Surname").IsEqualTo(true);
                asDict.ContainsKey("AddressCount").IsEqualTo(false);
            }
        }

        // see http://stackoverflow.com/questions/13127886/dapper-returns-null-for-singleordefaultdatediff
        [Test]
        public async Task TestNullFromInt_NoRowsAsync()
        {
            using (var connection = GetOpenConnection())
            {
                var result = await connection.Query<int>( // case with rows
                    "select DATEDIFF(day, GETUTCDATE(), @date)", new {date = DateTime.UtcNow.AddDays(20)})
                                             .SingleOrDefaultAsync();
                result.IsEqualTo(20);

                result = await connection.Query<int>( // case without rows
                    "select DATEDIFF(day, GETUTCDATE(), @date) where 1 = 0", new {date = DateTime.UtcNow.AddDays(20)})
                                         .SingleOrDefaultAsync();
                result.IsEqualTo(0); // zero rows; default of int over zero rows is zero
            }
        }

        [Test]
        public async Task TestDapperTableMetadataRetrievalAsync()
		{
            using (var connection = GetOpenConnection())
            {
                // Test for a bug found in CS 51509960 where the following sequence would result in an InvalidOperationException being
                // thrown due to an attempt to access a disposed of DataReader:
                //
                // - Perform a dynamic query that yields no results
                // - Add data to the source of that query
                // - Perform a the same query again
                await connection.Execute("CREATE TABLE #sut (value varchar(10) NOT NULL PRIMARY KEY)");
                (await connection.Query("SELECT value FROM #sut").ToList()).IsSequenceEqualTo(
                    Enumerable.Empty<dynamic>());

                (await connection.Execute("INSERT INTO #sut (value) VALUES ('test')")).IsEqualTo(1);
                var result = connection.Query("SELECT value FROM #sut");

                var first = await result.FirstAsync();
                ((string) first.value).IsEqualTo("test");
            }
		}

#if POSTGRESQL

        class Cat
        {
            public int Id { get; set; }
            public string Breed { get; set; }
            public string Name { get; set; }
        }

        Cat[] Cats = {
                                new Cat() { Breed = "Abyssinian", Name="KACTUS"},
                                new Cat() { Breed = "Aegean cat", Name="KADAFFI"},
                                new Cat() { Breed = "American Bobtail", Name="KANJI"},
                                new Cat() { Breed = "Balinese", Name="MACARONI"},
                                new Cat() { Breed = "Bombay", Name="MACAULAY"},
                                new Cat() { Breed = "Burmese", Name="MACBETH"},
                                new Cat() { Breed = "Chartreux", Name="MACGYVER"},
                                new Cat() { Breed = "German Rex", Name="MACKENZIE"},
                                new Cat() { Breed = "Javanese", Name="MADISON"},
                                new Cat() { Breed = "Persian", Name="MAGNA"}
                            };

        public void TestPostresqlArrayParameters()
        {
            using (var conn = new NpgsqlConnection("Server=localhost;Port=5432;User Id=dappertest;Password=dapperpass;Database=dappertest;Encoding=UNICODE"))
            {
                conn.Open();
                IDbTransaction transaction = conn.BeginTransaction();
                conn.Execute("create table tcat ( id serial not null, breed character varying(20) not null, name character varying (20) not null);");
                conn.Execute("insert tcat(breed, name) values(:breed, :name) ", Cats);

                var r = conn.Query<Cat>("select * from tcat where id=any(:catids)", new { catids = new[] { 1, 3, 5 } });
                r.Count().IsEqualTo(3);
                r.Count(c => c.Id == 1).IsEqualTo(1);
                r.Count(c => c.Id == 3).IsEqualTo(1);
                r.Count(c => c.Id == 5).IsEqualTo(1);
                transaction.Rollback();
            }
        }
#endif
    }
}
