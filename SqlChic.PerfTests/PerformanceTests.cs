using System;
using System.Data;
using System.Data.Linq;
using System.Data.SqlClient;
using System.Linq;
using System.Reactive.Linq;
using BLToolkit.Data;
using NHibernate.Criterion;
using NHibernate.Linq;
using ServiceStack.OrmLite;
using ServiceStack.OrmLite.SqlServer;
using SqlChic.PerfTests.Linq2Sql;
using SqlChic.PerfTests.NHibernate;

namespace SqlChic.PerfTests
{
    internal static class PerformanceTests
    {
		public static void Run(int iterations, int concurrency, Action<string, TimeSpan> resultLogger)
		{
			if (iterations < 1)
				return;
			if (concurrency < 1)
				throw new ArgumentOutOfRangeException("concurrency");
			if (resultLogger == null)
				throw new ArgumentNullException("resultLogger");

			var tests = new Tests(resultLogger);

			AddTests_Linq2Sql(tests, concurrency, iterations);

			AddTests_EntityFramework(tests, concurrency);

			AddTests_SqlChic(tests);

			AddTests_Dapper(tests);

			AddTests_PetaPoco(tests, concurrency);

			AddTests_Subsonic(tests, concurrency);

			// NHibernate

			AddTests_NHibernate(tests, concurrency);

			// bltoolkit
			AddTest_BlToolkit(tests, concurrency);

			// Simple.Data
			AddTest_SimpleData(tests, concurrency);

			// Soma
			var somadb = new Soma.Core.Db(new SomaConfig());
			tests.Add(id => { somadb.Find<Post>(id); }, "Soma");

			//ServiceStack's OrmLite:
			AddTest_OrmLite(tests, concurrency);

			AddTests_HandCoded(tests, concurrency);

			tests.Run(iterations, concurrency);
		}
		
		private static Func<EntityFramework.tempdbEntities1, int, EntityFramework.Post> entityFrameworkCompiled = System.Data.Objects.CompiledQuery.Compile<EntityFramework.tempdbEntities1, int, EntityFramework.Post>((db, id) => db.Posts.First(p => p.Id == id));    
		
	    private static void AddTest_OrmLite(Tests tests, int concurrency)
	    {
		    //var connections = Enumerable.Range(0, concurrency).Select(x => Program.GetOpenConnection()).ToArray();
		    OrmLiteConfig.DialectProvider = SqlServerOrmLiteDialectProvider.Instance; //Using SQL Server
		    //var ormLiteCmds = Enumerable.Range(0, concurrency).Select(i => connections[i].CreateCommand()).ToArray();
		    tests.Add(
				id =>
					{
						using(var connection = Program.GetOpenConnection())
						using(var command = connection.CreateCommand())
						command.QueryById<Post>(id);
					},
				//()=>ormLiteCmds.Cast<IDisposable>().Union(connections).ToList().ForEach(x=>x.Dispose()),
				"OrmLite (QueryById)");
	    }

	    private static void AddTest_SimpleData(Tests tests, int concurrency)
	    {
		    var sdb = Simple.Data.Database.OpenConnection(Program.connectionString);
		    tests.Add(
				id => { sdb.Posts.FindById(id); },
				"Simple.Data");
	    }

	    private static void AddTest_BlToolkit(Tests tests, int concurrency)
	    {
		    //var connections = Enumerable.Range(0, concurrency).Select(x=>Program.GetOpenConnection()).ToArray();
			//var dbManagers = Enumerable.Range(0, concurrency).Select((x,i) => new DbManager(connections[x])).ToArray();
			tests.Add(
				id =>
					{
						using(var connection = Program.GetOpenConnection())
						using(var dbManager = new DbManager(connection))
						dbManager.SetCommand("select * from Posts where Id = @id",
											dbManager.Parameter("id", id)
											).ExecuteList<Post>();
					},
				//() => dbManagers.Cast<IDisposable>().Union(connections).ToList().ForEach(x=>x.Dispose()),
				"BLToolkit");
	    }

	    private static void AddTests_HandCoded(Tests tests, int concurrency)
	    {
		    Func<SqlConnection, int, SqlCommand> createCommand = (connection, id) =>
			    {
					var command = new SqlCommand();
					command.Connection = connection;
					command.CommandText = @"select Id, [Text], [CreationDate], LastChangeDate, 
                Counter1,Counter2,Counter3,Counter4,Counter5,Counter6,Counter7,Counter8,Counter9 from Posts where Id = @Id";
					var idParam = command.Parameters.Add("@Id", System.Data.SqlDbType.Int);
					idParam.Value = id;
					return command;
			    };

		    //var connections1 = Enumerable.Range(0, concurrency).Select(x => Program.GetOpenConnection()).ToArray();
			tests.Add(id =>
			    {
					using(var connection = Program.GetOpenConnection())
					using(var postCommand = createCommand(connection, id))
					using (var reader = postCommand.ExecuteReader())
					{
						reader.Read();
						var post = new Post();
						post.Id = reader.GetInt32(0);
						post.Text = reader.GetNullableString(1);
						post.CreationDate = reader.GetDateTime(2);
						post.LastChangeDate = reader.GetDateTime(3);

						post.Counter1 = reader.GetNullableValue<int>(4);
						post.Counter2 = reader.GetNullableValue<int>(5);
						post.Counter3 = reader.GetNullableValue<int>(6);
						post.Counter4 = reader.GetNullableValue<int>(7);
						post.Counter5 = reader.GetNullableValue<int>(8);
						post.Counter6 = reader.GetNullableValue<int>(9);
						post.Counter7 = reader.GetNullableValue<int>(10);
						post.Counter8 = reader.GetNullableValue<int>(11);
						post.Counter9 = reader.GetNullableValue<int>(12);
					}
			    }, 
				//()=>connections1.Cast<IDisposable>().ToList().ForEach(x=>x.Dispose()),
				"Hand Coded (DataReader, Sync)");

			//var connections2 = Enumerable.Range(0, concurrency).Select(x => Program.GetOpenConnection()).ToArray();
		    tests.Add(async id =>
			    {
					using (var connection = Program.GetOpenConnection())
					using (var postCommand = createCommand(connection,id))
					using (var reader = await postCommand.ExecuteReaderAsync())
					{
						await reader.ReadAsync();
						var post = new Post();
						post.Id = await reader.GetFieldValueAsync<int>(0);
						post.Text = reader.GetNullableString(1);
						post.CreationDate = await reader.GetFieldValueAsync<DateTime>(2);
						post.LastChangeDate = await reader.GetFieldValueAsync<DateTime>(3);

						post.Counter1 = await reader.GetNullableValueAsync<int>(4);
						post.Counter2 = await reader.GetNullableValueAsync<int>(5);
						post.Counter3 = await reader.GetNullableValueAsync<int>(6);
						post.Counter4 = await reader.GetNullableValueAsync<int>(7);
						post.Counter5 = await reader.GetNullableValueAsync<int>(8);
						post.Counter6 = await reader.GetNullableValueAsync<int>(9);
						post.Counter7 = await reader.GetNullableValueAsync<int>(10);
						post.Counter8 = await reader.GetNullableValueAsync<int>(11);
						post.Counter9 = await reader.GetNullableValueAsync<int>(12);
					}
			    }, 
				//()=>connections2.Cast<IDisposable>().ToList().ForEach(x=>x.Dispose()),
				"Hand Coded (DataReader, Async)");

		    DataTable table = new DataTable
			    {
				    Columns =
					    {
						    {"Id", typeof (int)},
						    {"Text", typeof (string)},
						    {"CreationDate", typeof (DateTime)},
						    {"LastChangeDate", typeof (DateTime)},
						    {"Counter1", typeof (int)},
						    {"Counter2", typeof (int)},
						    {"Counter3", typeof (int)},
						    {"Counter4", typeof (int)},
						    {"Counter5", typeof (int)},
						    {"Counter6", typeof (int)},
						    {"Counter7", typeof (int)},
						    {"Counter8", typeof (int)},
						    {"Counter9", typeof (int)},
					    }
			    };

			////var connections3 = Enumerable.Range(0, concurrency).Select(x => Program.GetOpenConnection()).ToArray();
			//tests.Add(id =>
			//	{
			//		using (var connection = Program.GetOpenConnection())
			//		using (var postCommand = createCommand(connection, id))
			//		{
			//			object[] values = new object[13];
			//			using (var reader = postCommand.ExecuteReader())
			//			{
			//				reader.Read();
			//				reader.GetValues(values);
			//				table.Rows.Add(values);
			//			}
			//		}
			//	},
			//	//()=>connections3.Cast<IDisposable>().ToList().ForEach(x=>x.Dispose()),
			//	"Hand Coded (DataTable via IDataReader.GetValues, Sync)");

			////var connections4 = Enumerable.Range(0, concurrency).Select(x => Program.GetOpenConnection()).ToArray();
			//tests.Add(async id =>
			//{
			//	using (var connection = Program.GetOpenConnection())
			//	using (var postCommand = createCommand(connection, id))
			//	{
			//		object[] values = new object[13];
			//		using (var reader = await postCommand.ExecuteReaderAsync())
			//		{
			//			await reader.ReadAsync();
			//			reader.GetValues(values);
			//			table.Rows.Add(values);
			//		}
			//	}
			//},
			//	//()=>connections4.Cast<IDisposable>().ToList().ForEach(x=>x.Dispose()),
			//"Hand Coded (DataTable via IDataReader.GetValues, Async)");
	    }

	    private static void AddTests_NHibernate(Tests tests, int concurrency)
	    {
		    var nhSessions1 = Enumerable.Range(0, concurrency).Select(x => NHibernateHelper.OpenSession()).ToArray();
		    tests.Add(id => {nhSessions1[id % concurrency].CreateSQLQuery(@"select * from Posts where Id = :id")
									  .SetInt32("id", id)
									  .List();}, 
									  ()=>nhSessions1.ToList().ForEach(x=>x.Dispose()),
									  "NHibernate (SQL)");

			var nhSessions2 = Enumerable.Range(0, concurrency).Select(x => NHibernateHelper.OpenSession()).ToArray();
		    tests.Add(id => {nhSessions2[id % concurrency].CreateQuery(@"from Post as p where p.Id = :id")
									  .SetInt32("id", id)
									  .List();},
									  () => nhSessions2.ToList().ForEach(x => x.Dispose()),
									  "NHibernate (HQL)");

			var nhSessions3 = Enumerable.Range(0, concurrency).Select(x => NHibernateHelper.OpenSession()).ToArray();
		    tests.Add(id => {nhSessions3[id % concurrency].CreateCriteria<Post>()
									  .Add(Restrictions.IdEq(id))
									  .List();},
									  () => nhSessions3.ToList().ForEach(x => x.Dispose()),
									  "NHibernate (Criteria)");

			var nhSessions4 = Enumerable.Range(0, concurrency).Select(x => NHibernateHelper.OpenSession()).ToArray();
		    tests.Add(id => {nhSessions4[id % concurrency]
								.Query<Post>()
								.Where(p => p.Id == id).First();},
								() => nhSessions4.ToList().ForEach(x => x.Dispose()),
								"NHibernate (LINQ)");

			var nhSessions5 = Enumerable.Range(0, concurrency).Select(x => NHibernateHelper.OpenSession()).ToArray();
			tests.Add(id => { nhSessions5[id % concurrency].Get<Post>(id); },
				() => nhSessions5.ToList().ForEach(x => x.Dispose()),
				"NHibernate (Session.Get)");
	    }

	    private static void AddTests_Subsonic(Tests tests, int concurrency)
	    {
			// Subsonic ActiveRecord 
		    tests.Add(id => SubSonic.Post.SingleOrDefault(x => x.Id == id), "SubSonic (ActiveRecord.SingleOrDefault)");

		    // Subsonic coding horror
		    SubSonic.tempdbDB db = new SubSonic.tempdbDB();
		    tests.Add(
			    id => new global::SubSonic.Query.CodingHorror(db.Provider, "select * from Posts where Id = @0", id).ExecuteTypedList<Post>(),
				"SubSonic (Coding Horror)");
	    }

	    private static void AddTests_PetaPoco(Tests tests, int concurrency)
	    {
			// PetaPoco test with all default options
		    var petaPocos = Enumerable.Range(0, concurrency).Select(x =>
			    {
					var r = new PetaPoco.Database(Program.connectionString, "System.Data.SqlClient");
					r.OpenSharedConnection();
					return r;
			    }).ToArray();
			tests.Add(id => petaPocos[id%concurrency].Fetch<Post>("SELECT * from Posts where Id=@0", id),
				()=>petaPocos.ToList().ForEach(x=>x.Dispose()),
				"PetaPoco (Normal)");

		    // PetaPoco with some "smart" functionality disabled
		    var petapocoFasts = Enumerable.Range(0, concurrency).Select(x =>
			    {
					var r = new PetaPoco.Database(Program.connectionString, "System.Data.SqlClient");
					r.OpenSharedConnection();
					r.EnableAutoSelect = false;
					r.EnableNamedParams = false;
					r.ForceDateTimesToUtc = false;
					return r;
			    }).ToArray();	    
			tests.Add(id => { petapocoFasts[id%concurrency].Fetch<Post>("SELECT * from Posts where Id=@0", id); }, 
				()=>petapocoFasts.ToList().ForEach(x=>x.Dispose()),
				"PetaPoco (Fast)");
	    }

	    private static void AddTests_SqlChic(Tests tests)
	    {
		    tests.Add(id =>
			    {
					using(var connection = Program.GetClosedConnection())
					connection.Query<Post>("select * from Posts where Id = @Id", new {Id = id}).FirstAsync();
			    },
				"SqlChic (Buffered)");

		    tests.Add(id =>
			    {
					using(var connection = Program.GetOpenConnection())
					connection.Query<Post>("select * from Posts where Id = @Id", new {Id = id}).FirstAsync();
			    },
				"SqlChic (Non-buffered)");

		    tests.Add(id =>
			    {
					using(var connection = Program.GetClosedConnection())
					connection.Query("select * from Posts where Id = @Id", new {Id = id}).FirstAsync();
			    },
				"SqlChic (Dynamic, Buffered)");

			tests.Add(id =>
				{
					using(var connection = Program.GetOpenConnection())
					connection.Query("select * from Posts where Id = @Id", new {Id = id}).FirstAsync();
				},
				"SqlChic (Dynamic, Non-buffered)");
	    }

		private static void AddTests_Dapper(Tests tests)
		{
			tests.Add(id =>
				{
					using (var connection = Program.GetClosedConnection())
					{
						global::Dapper.SqlMapper.Query<Post>(connection, "select * from Posts where Id = @Id", new {Id = id}).First();
					}
				},
				"Dapper (Buffered, Sync)");

			tests.Add(id =>
				{
					using (var connection = Program.GetOpenConnection())
					{
						global::Dapper.SqlMapper.Query<Post>(connection, "select * from Posts where Id = @Id", new {Id = id}).First();
					}
				},
				"Dapper (Non-buffered, Sync)");

			tests.Add(id =>
				{
					using (var connection = Program.GetClosedConnection())
					{
						global::Dapper.SqlMapper.Query<dynamic>(connection, "select * from Posts where Id = @Id", new {Id = id}).First();
					}
				},
				"Dapper (Dynamic, Buffered, Sync)");

			tests.Add((Action<int>)(id =>
				{
					using (var connection = Program.GetOpenConnection())
					{
						global::Dapper.SqlMapper.Query<dynamic>(connection, "select * from Posts where Id = @Id", new {Id = id}).First();
					}
				}),
				"Dapper (Dynamic, Non-buffered, Sync)");

			//tests.Add(async id => (await global::Dapper.SqlMapper.QueryAsync<Post>(mapperConnectionClosed3, "select * from Posts where Id = @Id", new { Id = id })).First(),
			//	() => mapperConnectionClosed3.Dispose(),
			//	"Dapper (Buffered, Async)");
			
			tests.Add(async (int id) =>
				{
					using (var connection = Program.GetOpenConnection())
					{
						(await global::Dapper.SqlMapper.QueryAsync<Post>(connection, "select * from Posts where Id = @Id", new {Id = id})).First();
					}
				},
				"Dapper (Non-buffered, Async)");

			//tests.Add((Func<int,Task>)(async id => (await global::Dapper.SqlMapper.QueryAsync<dynamic>(mapperConnectionClosed4, "select * from Posts where Id = @Id", new { Id = id })).First()),
			//	() => mapperConnectionClosed4.Dispose(),
			//	"Dapper (Dynamic, Buffered, Async)");

			tests.Add(async id =>
				{
					using (var connection = Program.GetOpenConnection())
					{
						(await global::Dapper.SqlMapper.QueryAsync<dynamic>(connection, "select * from Posts where Id = @Id", new {Id = id})).First();
					}
				},
				"Dapper (Dynamic, Non-buffered, Async)");
		}

	    private static void AddTests_EntityFramework(Tests tests, int concurrency)
	    {
		    var entityContext = new EntityFramework.tempdbEntities1();
		    entityContext.Connection.Open();
		    tests.Add(id => entityContext.Posts.First(p => p.Id == id),
				()=>entityContext.Dispose(),
				"Entity Framework (Normal)");

		    var entityContext2 = new EntityFramework.tempdbEntities1();
		    entityContext2.Connection.Open();
		    tests.Add(id => entityContext2.ExecuteStoreQuery<Post>("select * from Posts where Id = {0}", id).First(),
					  () => entityContext2.Dispose(),
					  "Entity Framework (ExecuteStoreQuery)");

		    var entityContext3 = new EntityFramework.tempdbEntities1();
		    entityContext3.Connection.Open();
		    tests.Add(id => entityFrameworkCompiled(entityContext3, id),
				()=>entityContext3.Dispose(),
				"Entity Framework (CompiledQuery)");

		    var entityContext4 = new EntityFramework.tempdbEntities1();
		    entityContext4.Connection.Open();
		    tests.Add(id => entityContext4.Posts.Where("it.Id = @id", new System.Data.Objects.ObjectParameter("id", id)).First(),
					()=>entityContext4.Dispose(),
					  "Entity Framework (ESQL)");

		    var entityContext5 = new EntityFramework.tempdbEntities1();
		    entityContext5.Connection.Open();
		    entityContext5.Posts.MergeOption = System.Data.Objects.MergeOption.NoTracking;
		    tests.Add(id => entityContext5.Posts.First(p => p.Id == id),
				()=>entityContext5.Dispose(),
				"Entity Framework (No Tracking)");
	    }

	    private static void AddTests_Linq2Sql(Tests tests, int concurrency, int iterations)
	    {
			var connections1 = Enumerable.Range(0, concurrency).Select(x => Program.GetClosedConnection()).ToArray();
			var l2scontexts1 = Enumerable.Range(0, concurrency).Select(x => new DataClassesDataContext(connections1[x])).ToArray();
			tests.Add(id => { l2scontexts1[id % concurrency].Posts.First(p => p.Id == id); },
				() => l2scontexts1.Cast<IDisposable>().Union(connections1).ToList().ForEach(x => x.Dispose()),
				"Linq2Sql (Normal)");

			var connections2 = Enumerable.Range(0, concurrency).Select(x => Program.GetClosedConnection()).ToArray();
			var l2scontexts2 = Enumerable.Range(0, concurrency).Select(x => new DataClassesDataContext(connections2[x])).ToArray();
			var compiledGetPost =
			    CompiledQuery.Compile((Linq2Sql.DataClassesDataContext ctx, int id) => ctx.Posts.First(p => p.Id == id));
			tests.Add(id => { compiledGetPost(l2scontexts2[id % concurrency], id); },
				() => l2scontexts2.Cast<IDisposable>().Union(connections2).ToList().ForEach(x => x.Dispose()),
				"Linq2Sql (Compiled)");

			var connections3 = Enumerable.Range(0, concurrency).Select(x => Program.GetClosedConnection()).ToArray();
			var l2scontexts3 = Enumerable.Range(0, concurrency).Select(x => new DataClassesDataContext(connections3[x])).ToArray();
			tests.Add(id => { l2scontexts3[id % concurrency].ExecuteQuery<Post>("select * from Posts where Id = {0}", id).First(); },
				()=>l2scontexts3.Cast<IDisposable>().Union(connections3).ToList().ForEach(x=>x.Dispose()),
					  "Linq2Sql (ExecuteQuery)");
	    }
    }
}