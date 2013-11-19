namespace SqlChic.PerfTests
{
	internal class SomaConfig : Soma.Core.MsSqlConfig
	{
		public override string ConnectionString
		{
			get { return Program.connectionString; }
		}

		public override void Log(Soma.Core.PreparedStatement preparedStatement)
		{
			// no op
		}
	}
}