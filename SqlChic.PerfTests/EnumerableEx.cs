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
	}
}