using System;
using System.Data.SqlClient;
using System.Reflection;
using System.Linq;
using System.Threading.Tasks;

namespace SqlChic.PerfTests
{
    [ServiceStack.DataAnnotations.Alias("Posts")]
	[Soma.Core.Table(Name = "Posts")]
    class Post
    {
		[Soma.Core.Id(Soma.Core.IdKind.Identity)]
        public int Id { get; set; }
        public string Text { get; set; }
        public DateTime CreationDate { get; set; }
        public DateTime LastChangeDate { get; set; }
        public int? Counter1 { get; set; }
        public int? Counter2 { get; set; }
        public int? Counter3 { get; set; }
        public int? Counter4 { get; set; }
        public int? Counter5 { get; set; }
        public int? Counter6 { get; set; }
        public int? Counter7 { get; set; }
        public int? Counter8 { get; set; }
        public int? Counter9 { get; set; }

    }

    class Program
    {

		public static readonly string connectionString = "Data Source=.;Initial Catalog=tempdb;Integrated Security=True;MultipleActiveResultSets=True";

        public static SqlConnection GetOpenConnection()
        {
            var connection = new SqlConnection(connectionString);
            connection.Open();
            return connection;
        }

        public static SqlConnection GetClosedConnection()
        {
            var connection = new SqlConnection(connectionString);
            return connection;
        }

        static void RunPerformanceTests()
        {
            var test = new PerformanceTests();

            int baseConcurrency = System.Environment.ProcessorCount;

            const int warmupIterations = 2000;
            Console.WriteLine("Warming up by running {0} iterations that load up a post entity", warmupIterations);
            test.Run(warmupIterations, 1, (tn, tt) => { });

            RunGcCollect();

            const int iterations = 2000;
            Console.WriteLine("Running {0} iterations that load up a post entity, no concurrency", iterations);
            test.Run(iterations, 1, LogTestToConsole);

			test = new PerformanceTests();
            RunGcCollect();

            if (baseConcurrency > 1)
            {
                Console.WriteLine("Running {0} iterations that load up a post entity, concurrency @ 1xCPU ({1})", iterations, baseConcurrency);
                test.Run(iterations, baseConcurrency, LogTestToConsole);

				test = new PerformanceTests();
				RunGcCollect();

                Console.WriteLine("Running {0} iterations that load up a post entity, concurrency @ 2xCPU ({1})", iterations, 2 * baseConcurrency);
                test.Run(iterations, 2 * baseConcurrency, LogTestToConsole);

				test = new PerformanceTests();
				RunGcCollect();

                Console.WriteLine("Running {0} iterations that load up a post entity, concurrency @ 4xCPU ({1})", iterations, 4 * baseConcurrency);
                test.Run(iterations, 4 * baseConcurrency, LogTestToConsole);

                if ((baseConcurrency ^ 2) > (baseConcurrency*4))
                {
					test = new PerformanceTests();
					RunGcCollect();

                    Console.WriteLine("Running {0} iterations that load up a post entity, concurrency @ CPU^2 ({1})", iterations, baseConcurrency ^ 2);
                    test.Run(iterations, baseConcurrency ^ 2, LogTestToConsole);                    
                }
            }
            else
            {
                Console.Error.WriteLine("Unable to test concurrency due to lack of CPUs");
            }
        }

        private static void RunGcCollect()
        {
            Console.WriteLine();
            Console.WriteLine("Running GC Collect");
            GC.Collect(1, GCCollectionMode.Forced, true);
            Console.WriteLine();
        }

        private static void LogTestToConsole(string testName, TimeSpan totalTestTime)
        {
            Console.WriteLine("{0} \t{1}ms", testName, totalTestTime.TotalMilliseconds);
        }

        static void Main()
        {

#if DEBUG
            throw new InvalidOperationException("Performance tests should not be run in DEBUG.");
#else
			EnsureDBSetup();
			RunPerformanceTests();
#endif

            if (System.Diagnostics.Debugger.IsAttached)
            {
                Console.WriteLine("(end of tests; press any key)");
                Console.ReadKey();
            }
        }

        private static void EnsureDBSetup()
        {
            using (var cnn = GetOpenConnection())
            {
                var cmd = cnn.CreateCommand();
                cmd.CommandText = @"
if (OBJECT_ID('Posts') is null)
begin
	create table Posts
	(
		Id int identity primary key, 
		[Text] varchar(max) not null, 
		CreationDate datetime not null, 
		LastChangeDate datetime not null,
		Counter1 int,
		Counter2 int,
		Counter3 int,
		Counter4 int,
		Counter5 int,
		Counter6 int,
		Counter7 int,
		Counter8 int,
		Counter9 int
	)
	   
	set nocount on 

	declare @i int
	declare @c int

	declare @id int

	set @i = 0

	while @i <= 5001
	begin 
		
		insert Posts ([Text],CreationDate, LastChangeDate) values (replicate('x', 2000), GETDATE(), GETDATE())
		set @id = @@IDENTITY
		
		set @i = @i + 1
	end
end
";
                cmd.Connection = cnn;
                cmd.ExecuteNonQuery();
            }
        }

    }
}
