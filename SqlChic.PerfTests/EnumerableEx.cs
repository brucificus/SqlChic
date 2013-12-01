using System;
using System.Collections.Generic;
using System.Linq;

namespace SqlChic.PerfTests
{
	public static class EnumerableEx
	{
		public static double GeoAverage(this IEnumerable<double> values)
		{
			return values.Aggregate(Tuple.Create(0.0, 0), (p, c) => Tuple.Create<double, int>(p.Item1 * c, p.Item2 + 1), p => Math.Pow(p.Item1, 1.0 / (double)p.Item2));
		}

		public static TimeSpan Sum(this IEnumerable<TimeSpan> values)
		{
			return values.Aggregate(TimeSpan.Zero, (p, c) => p + c);
		}

		public static TimeSpan Average(this IEnumerable<TimeSpan> values)
		{
			return values.Aggregate(Tuple.Create(TimeSpan.Zero, 0), (p, c) => Tuple.Create(p.Item1 + c, p.Item2 + 1), x => new TimeSpan(x.Item1.Ticks / x.Item2));
		}

		public static TimeSpan StdDevFrom(this IEnumerable<TimeSpan> values, TimeSpan average)
		{
			return values.Aggregate(Tuple.Create(0.0, 0.0), (p, c) => Tuple.Create(p.Item1 + Math.Pow(c.Ticks - average.Ticks, 2), p.Item2 + 1), x => new TimeSpan((long)Math.Sqrt(x.Item1 / x.Item2)));
		}
	}

	public static class TimeSpanEx
	{
		public static TimeSpan Abs(this TimeSpan self)
		{
			if (self.Ticks < 0)
				return self.Negate();
			return self;
		}
	}
}