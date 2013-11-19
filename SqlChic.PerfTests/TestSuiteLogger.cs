using System;
using System.IO;

namespace SqlChic.PerfTests
{
	public class TestSuiteLogger
		: IDisposable
	{
		private readonly int _iterations;
		private TextWriter _Output = null;

		public TestSuiteLogger(string outputFilePath, int iterations)
		{
			_iterations = iterations;
			if (!String.IsNullOrWhiteSpace(outputFilePath))
			{
				if(System.IO.File.Exists(outputFilePath))
					System.IO.File.Delete(outputFilePath);
				_Output = new StreamWriter(outputFilePath);
				_Output.WriteLine("Test,Iterations,Concurrency,\"Total Time (ms)\"");
			}
			else
			{
				_Output = TextWriter.Null;
			}
		}

		public TestRunLogger BeginLoggingRun(int concurrency)
		{
			return new TestRunLogger(_Output, _iterations, concurrency);
		}

		public void Dispose()
		{
			if (_Output != TextWriter.Null)
			{
				_Output.Flush();
				_Output.Dispose();
			}
		}

		public class TestRunLogger
			: IDisposable
		{
			private readonly TextWriter _csvOutput;
			private readonly int _iterations;
			private readonly int _concurrency;

			public TestRunLogger(TextWriter csvOutput, int iterations, int concurrency)
			{
				_csvOutput = csvOutput;
				_iterations = iterations;
				_concurrency = concurrency;
			}

			public void LogTestResult(string testName, TimeSpan totalTestTime)
			{
				Program.LogTestToConsole(testName, totalTestTime);
				if (_csvOutput != TextWriter.Null)
				{
					_csvOutput.WriteLine("\"{0}\",{1},{2},{3}", testName, _iterations, _concurrency, totalTestTime.TotalMilliseconds);
				}
			}

			void IDisposable.Dispose()
			{
				_csvOutput.Flush();
			}
		}
	}
}