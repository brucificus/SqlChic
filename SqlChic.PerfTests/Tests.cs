using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;

namespace SqlChic.PerfTests
{
	internal class Tests : List<Test>
	{
		private readonly Action<string, TimeSpan> _resultLogger;

		public Tests(Action<string,TimeSpan> resultLogger)
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
				test.Watch = new Stopwatch();
				test.Watch.Reset();
			}

			//System.Threading.ThreadPool.SetMaxThreads(concurrency*2, concurrency*2);
			//System.Threading.ThreadPool.SetMinThreads(concurrency, concurrency);
			//System.Threading.ThreadPool.SetMaxThreads(concurrency * 2, concurrency * 2);

			if (concurrency == 1)
			{
				var rand = new Random();
				foreach (var test in this.OrderBy(ignore => rand.Next()))
				{
					test.Watch.Start();
					for (int i = 1; i <= iterations; i++)
					{
						var task = test.Iteration(i);
						task.Wait();
					}
					test.Watch.Stop();
					test.Teardown();
				}    
			}
			else if (concurrency > iterations)
			{
				throw new InvalidOperationException(String.Format("Concurrency ({0}) exceeds iterations ({1})", concurrency, iterations));
			}
			else
			{
				foreach (var test in this)
				{
					Func<int, Task> createTask = async (concurrencyIndex) =>
						{
							for (int i = concurrencyIndex; i < iterations; i += concurrency)
							{
								var work = test.Iteration(i);
								await work;
							}
						};

					test.Watch.Start();
					var tasks = new List<Task>();
					foreach (var i in Enumerable.Range(1, concurrency))
					{
						tasks.Add(createTask(i));
					}
					var tasksToWaitOn = tasks.ToArray();
					Task.WaitAll(tasksToWaitOn);
					test.Watch.Stop();
					test.Teardown();
				}
			}

			foreach (var test in this.OrderBy(t => t.Watch.ElapsedMilliseconds))
			{
				_resultLogger(test.Name, test.Watch.Elapsed);
			}
		}
	}
}