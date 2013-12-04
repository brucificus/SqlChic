using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;
using NUnit.Framework;

namespace SqlChic.Tests
{
	[AttributeUsage(AttributeTargets.Method)]
	public class TransactionRollbackAttribute
		: System.Attribute, NUnit.Framework.ITestAction
	{
		private readonly object _SyncLock = new object();
		private TransactionScope _TransactionScope = null;

		public void BeforeTest(TestDetails testDetails)
		{
			lock (_SyncLock)
			{
				if (_TransactionScope != null)
					throw new InvalidOperationException();
				var transactionScopeOptions = new System.Transactions.TransactionOptions()
					{
						IsolationLevel = IsolationLevel.ReadCommitted,
						Timeout = TimeSpan.FromSeconds(30)
					};
				this._TransactionScope = new TransactionScope(TransactionScopeOption.Required, transactionScopeOptions);
			}
		}

		public void AfterTest(TestDetails testDetails)
		{
			lock (_SyncLock)
			{
				if (_TransactionScope == null)
					throw new InvalidOperationException();
				_TransactionScope.Dispose();
				_TransactionScope = null;
			}
		}

		public ActionTargets Targets { get {return ActionTargets.Test;} }
	}
}
