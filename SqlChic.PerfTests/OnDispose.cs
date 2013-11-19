using System;

namespace SqlChic.PerfTests
{
	internal class OnDispose
		: IDisposable
	{
		private readonly Action _action;

		private OnDispose(Action action)
		{
			if (action == null) throw new ArgumentNullException("action");
			_action = action;
		}

		internal static IDisposable Do(Action action)
		{
			return new OnDispose(action);
		}

		internal static readonly IDisposable DoNothing = Do(() => { });

		public void Dispose()
		{
			_action();
		}
	}
}