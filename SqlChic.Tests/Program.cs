using System;
using System.Data.SqlClient;
using System.Reflection;
using System.Linq;
using System.Threading.Tasks;

namespace SqlChic.Tests
{
    class Program
    {

		public static readonly string connectionString = "Data Source=.;Initial Catalog=tempdb;Integrated Security=True;MultipleActiveResultSets=True";

        public static SqlConnection GetOpenConnection()
        {
            var connection = new SqlConnection(connectionString);
            connection.Open();
            return connection;
        }

        static void Main()
        {
            RunTests();

            Console.WriteLine("(end of tests; press any key)");

            Console.ReadKey();
        }

        private static void RunTests()
        {
            var tester = new Tests();
            int fail = 0;
            MethodInfo[] methods = typeof(Tests).GetMethods(BindingFlags.Public | BindingFlags.Instance | BindingFlags.DeclaredOnly);
            var activeTests = methods.Where(m => Attribute.IsDefined(m, typeof(ActiveTestAttribute))).ToArray();
            if (activeTests.Length != 0) methods = activeTests;
            foreach (var method in methods)
            {
                Console.Write("Running " + method.Name);
				try
				{
					var methodResult = method.Invoke(tester, null);
					if (methodResult is Task)
					{
						((Task) methodResult).Wait();
					}
					Console.WriteLine(" - OK!");
				}
				catch (TargetInvocationException tie)
				{
					fail++;
					Console.WriteLine(" - " + tie.InnerException.Message);

				}
				catch (AggregateException ae)
				{
					fail++;
					if (ae.InnerExceptions.Count == 1)
					{
						Console.WriteLine(" - " + ae.InnerException.Message);
					}
					else
					{
						Console.WriteLine(" - " + ae.Message);
					}
				}
				catch (Exception ex)
				{
					fail++;
					Console.WriteLine(" - " + ex.Message);
				}
            }
            Console.WriteLine();
            if(fail == 0)
            {
                Console.WriteLine("(all tests successful)");
            }
            else
            {
                Console.WriteLine("#### FAILED: {0}", fail);
            }
        }
    }

    [AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
    public sealed class ActiveTestAttribute : Attribute {}

}
