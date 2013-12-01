using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace SqlChic.PerfTests
{
	internal class Tests : List<Test>
	{
		private readonly Action<string, TimeSpan, double> _resultLogger;

		public Tests(Action<string,TimeSpan,double> resultLogger)
		{
			_resultLogger = resultLogger;
		}

		public void Add(Action<int> iteration, string name)
		{
			Add(Test.Create(iteration, name));
		}

		public void Add(Func<int, Task> iteration, string name)
		{
			Add(Test.Create(iteration, name));
		}

		public void Add(Action<int> iteration, Action teardown, string name)
		{
			Add(Test.Create(iteration, teardown, name));
		}

		public void Add(Func<int, Task> iteration, Action teardown, string name)
		{
			Add(Test.Create(iteration, teardown, name));
		}

		public void Run(int iterations, int concurrency)
		{ 
			// warmup 
			foreach (var test in this)
			{
				var task = test.Iteration(iterations + 1);
				task.Wait();
			}

			//System.Threading.ThreadPool.SetMaxThreads(concurrency*2, concurrency*2);
			//System.Threading.ThreadPool.SetMinThreads(concurrency, concurrency);
			//System.Threading.ThreadPool.SetMaxThreads(concurrency * 2, concurrency * 2);

			if (concurrency == 1)
			{
				var rand = new Random();
				foreach (var test in this.OrderBy(ignore => rand.Next()))
				{
					var watch = new Stopwatch();
					for (int i = 1; i <= iterations; i++)
					{
						watch.Reset();
						watch.Start();
						var task = test.Iteration(i);
						task.Wait();
						watch.Stop();
						test.Timings.Add(watch.Elapsed);
					}
					test.Teardown();
				}    
			}
			else if (concurrency > iterations)
			{
				throw new InvalidOperationException(String.Format("Concurrency ({0}) exceeds iterations ({1})", concurrency, iterations));
			}
			else
			{
				Func<Test, int, Task> createTask = async (test, concurrencyIndex) =>
				{
					var watch = new Stopwatch();
					for (var i = concurrencyIndex; i < iterations; i += concurrency)
					{
						watch.Reset();
						watch.Start();
						var work = test.Iteration(i);
						await work;
						watch.Stop();
						test.Timings.Add(watch.Elapsed);
					}
				};
				foreach (var test in this)
				{
					var tasksToWaitOn = Enumerable.Range(1, concurrency).Select(i => createTask(test, i)).ToArray();
					Task.WaitAll(tasksToWaitOn);
					test.Teardown();
				}
			}

			// Remove outliers so that we get the best representation
			var testTimingsPruned = this.ToDictionary(x => x, x =>
				{
					var timingsAverage = x.Timings.Average();
					var timingsStdDev = x.Timings.StdDevFrom(timingsAverage);
					return Tuple.Create(x.Timings.Where(y => (timingsAverage - y).Abs() <= timingsStdDev), x.Timings);
				});
			var testTimingsPrunedResults = testTimingsPruned.ToDictionary(x => x.Key, x =>
				{
					var prunedMeasurementsCount = x.Value.Item1.Count();
					var originalMeasurementsCount = x.Value.Item2.Count();
					var error = (double) (originalMeasurementsCount - prunedMeasurementsCount)/(double)originalMeasurementsCount;
					return Tuple.Create(x.Value.Item1.Average(), error); 
				});

			foreach (var kvp in testTimingsPrunedResults.OrderBy(x=>x.Value.Item1))
			{
				_resultLogger(kvp.Key.Name, kvp.Value.Item1, kvp.Value.Item2);
			}
		}
	}
}