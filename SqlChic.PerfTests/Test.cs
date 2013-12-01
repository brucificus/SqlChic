using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;

namespace SqlChic.PerfTests
{
	internal class Test
	{
		public static Test Create(Action<int> iteration, string name)
		{
			return Create(iteration, () => { }, name);
		}

		public static Test Create(Func<int, Task> iteration, string name)
		{
			return Create(iteration, () => { }, name);
		}

		public static Test Create(Action<int> iteration, Action teardown, string name)
		{
			return Create((i) =>
				{
					var t = Task.Factory.StartNew(() => iteration(i));
					return t;
				}, teardown, name);
		}

		public static Test Create(Func<int, Task> iteration, Action teardown, string name)
		{
			return new Test { Iteration = iteration, Name = name, Teardown = teardown };
		}

		private Test()
		{
			Timings = new ConcurrentBag<TimeSpan>();
		}

		public Func<int,Task> Iteration { get; private set; }
		public string Name { get; private set; }
		public System.Collections.Concurrent.ConcurrentBag<TimeSpan> Timings { get; private set; }
		public Action Teardown { get; private set; }
	}
}