using System;
using System.Data.SqlClient;

namespace SqlChic.PerfTests
{
	class Program
    {
		public static readonly string connectionString = "Data Source=.;Initial Catalog=tempdb;Integrated Security=True;MultipleActiveResultSets=True";

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
	        bool teamCityDetected = DetectTeamCity();

            int baseConcurrency = System.Environment.ProcessorCount;

            const int warmupIterations = 2000;
			const int iterations = 2000;

			using (StartLogSection(String.Format("PerfTests warming up", warmupIterations), teamCityDetected))
			{
				PerformanceTests.Run(warmupIterations, 1, (tn, tt) => { });

				RunGcCollect();				
			}
			Console.WriteLine();

			using (var testSuiteLogger = new TestSuiteLogger("perftests.csv", iterations))
			{
				using (StartLogSection(String.Format("PerfTests @ {0} iterations, no concurrency", iterations), teamCityDetected))
				{
					using (var testResultLogger = testSuiteLogger.BeginLoggingRun(1))
					{
						PerformanceTests.Run(iterations, 1, testResultLogger.LogTestResult);
						RunGcCollect();
					}
				}

				if (baseConcurrency > 1)
				{
					using (StartLogSection(String.Format("PerfTests @ {0} iterations, concurrency @ 1xCPU ({1})", iterations, baseConcurrency), teamCityDetected))
					{
						using (var testResultLogger = testSuiteLogger.BeginLoggingRun(baseConcurrency))
						{
							PerformanceTests.Run(iterations, baseConcurrency, testResultLogger.LogTestResult);
							RunGcCollect();
						}
					}

					using (StartLogSection(String.Format("PerfTests @ {0} iterations, concurrency @ 2xCPU ({1})", iterations, 2 * baseConcurrency), teamCityDetected))
					{
						using (var testResultLogger = testSuiteLogger.BeginLoggingRun(2 * baseConcurrency))
						{
							PerformanceTests.Run(iterations, 2 * baseConcurrency, testResultLogger.LogTestResult);
							RunGcCollect();
						}
					}

					using (StartLogSection(String.Format("PerfTests @ {0} iterations, concurrency @ 4xCPU ({1})", iterations, 4 * baseConcurrency), teamCityDetected))
					{
						using (var testResultLogger = testSuiteLogger.BeginLoggingRun(4 * baseConcurrency))
						{
							PerformanceTests.Run(iterations, 4 * baseConcurrency, testResultLogger.LogTestResult);
						}
					}

					//int baseConcurrencySquared = (int) Math.Pow(baseConcurrency, 2);
					//if (baseConcurrencySquared > (baseConcurrency * 4))
					//{
					//	using (StartLogSection(String.Format("PerfTests @ {0} iterations, concurrency @ CPU^2 ({1})", iterations, baseConcurrencySquared), teamCityDetected))
					//	{
					//		RunGcCollect();
					//		PerformanceTests.Run(iterations, baseConcurrencySquared, LogTestToConsole);
					//	}
					//}
				}
				else
				{
					Console.Error.WriteLine("Unable to test concurrency due to lack of CPUs");
				}
			}
        }

        private static void RunGcCollect()
        {
            Console.WriteLine();
            Console.WriteLine("Running GC Collect");
            GC.Collect(1, GCCollectionMode.Forced, true);
            Console.WriteLine();
        }

        public static void LogTestToConsole(string testName, TimeSpan totalTestTime)
        {
            Console.WriteLine("{0} \t\t{1}ms", testName, totalTestTime.TotalMilliseconds);
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

		private static bool DetectTeamCity()
		{
			return !String.IsNullOrWhiteSpace(System.Environment.GetEnvironmentVariable("TEAMCITY_VERSION"));
		}

		private static IDisposable StartLogSection(string message, bool useTeamCity)
		{
			if(!useTeamCity)
			{
				Console.Out.WriteLine(message);
				return OnDispose.DoNothing;
			}
			else
			{
				Console.WriteLine("##teamcity[progressStart '{0}']", message);
				return OnDispose.Do(() => Console.WriteLine("##teamcity[progressFinish '{0}']", message));
			}
		}
    }
}
