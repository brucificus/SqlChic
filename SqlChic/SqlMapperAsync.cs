using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SqlChic
{
	public static partial class SqlMapper
	{
		private struct AsyncExecState
		{
			public readonly DbCommand Command;
			public readonly Task<int> Task;
			public AsyncExecState(DbCommand command, Task<int> task)
			{
				this.Command = command;
				this.Task = task;
			}
		}
		private static async Task<int> ExecuteMultiImplAsync(DbConnection cnn, CommandDefinition command, IEnumerable multiExec)
		{
			bool isFirst = true;
			int total = 0;
			bool wasClosed = cnn.State == ConnectionState.Closed;
			try
			{
				if (wasClosed) await ((DbConnection)cnn).OpenAsync().ConfigureAwait(false);

				CacheInfo info = null;
				string masterSql = null;
				if ((command.Flags & CommandFlags.Pipelined) != 0)
				{
					const int MAX_PENDING = 100;
					var pending = new Queue<AsyncExecState>(MAX_PENDING);
					DbCommand cmd = null;
					try
					{
						foreach (var obj in multiExec)
						{
							if (isFirst)
							{
								isFirst = false;
								cmd = (DbCommand)command.SetupCommand(cnn, null);
								masterSql = cmd.CommandText;
								var identity = new Identity(command.CommandText, cmd.CommandType, cnn, null, obj.GetType(), null);
								info = GetCacheInfo(identity, obj);
							}
							else if (pending.Count >= MAX_PENDING)
							{
								var recycled = pending.Dequeue();
								total += await recycled.Task.ConfigureAwait(false);
								cmd = recycled.Command;
								cmd.CommandText = masterSql; // because we do magic replaces on "in" etc
								cmd.Parameters.Clear(); // current code is Add-tastic
							}
							else
							{
								cmd = (DbCommand)command.SetupCommand(cnn, null);
							}
							info.ParamReader(cmd, obj);

							var task = cmd.ExecuteNonQueryAsync(command.CancellationToken);
							pending.Enqueue(new AsyncExecState(cmd, task));
							cmd = null; // note the using in the finally: this avoids a double-dispose
						}
						while (pending.Count != 0)
						{
							var pair = pending.Dequeue();
							using (pair.Command) { } // dispose commands
							total += await pair.Task.ConfigureAwait(false);
						}
					}
					finally
					{
						// this only has interesting work to do if there are failures
						using (cmd) { } // dispose commands
						while (pending.Count != 0)
						{ // dispose tasks even in failure
							using (pending.Dequeue().Command) { } // dispose commands
						}
					}
					return total;
				}
				else
				{
					using (var cmd = (DbCommand)command.SetupCommand(cnn, null))
					{
						foreach (var obj in multiExec)
						{
							if (isFirst)
							{
								masterSql = cmd.CommandText;
								isFirst = false;
								var identity = new Identity(command.CommandText, cmd.CommandType, cnn, null, obj.GetType(), null);
								info = GetCacheInfo(identity, obj);
							}
							else
							{
								cmd.CommandText = masterSql; // because we do magic replaces on "in" etc
								cmd.Parameters.Clear(); // current code is Add-tastic
							}
							info.ParamReader(cmd, obj);
							total += await cmd.ExecuteNonQueryAsync(command.CancellationToken).ConfigureAwait(false);
						}
					}
				}
			}
			finally
			{
				if (wasClosed) cnn.Close();
			}
			return total;
		}

		/// <summary>
		/// Maps a query to objects
		/// </summary>
		/// <typeparam name="TFirst">The first type in the recordset</typeparam>
		/// <typeparam name="TSecond">The second type in the recordset</typeparam>
		/// <typeparam name="TReturn">The return type</typeparam>
		/// <param name="cnn"></param>
		/// <param name="sql"></param>
		/// <param name="map"></param>
		/// <param name="param"></param>
		/// <param name="transaction"></param>
		/// <param name="buffered"></param>
		/// <param name="splitOn">The field we should split and read the second object from (default: id)</param>
		/// <param name="commandTimeout">Number of seconds before command execution timeout</param>
		/// <param name="commandType">Is it a stored proc or a batch?</param>
		/// <returns></returns>
		public static Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TReturn>(this DbConnection cnn, string sql, Func<TFirst, TSecond, TReturn> map, dynamic param = null, DbTransaction transaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
		{
			return MultiMapAsync<TFirst, TSecond, DontMap, DontMap, DontMap, DontMap, DontMap, TReturn>(cnn,
				new CommandDefinition(sql, (object)param, transaction, commandTimeout, commandType, buffered ? CommandFlags.Buffered : CommandFlags.None, default(CancellationToken)), map, splitOn);
		}

		/// <summary>
		/// Maps a query to objects
		/// </summary>
		/// <typeparam name="TFirst">The first type in the recordset</typeparam>
		/// <typeparam name="TSecond">The second type in the recordset</typeparam>
		/// <typeparam name="TReturn">The return type</typeparam>
		/// <param name="cnn"></param>
		/// <param name="splitOn">The field we should split and read the second object from (default: id)</param>
		/// <param name="command">The command to execute</param>
		/// <param name="map"></param>
		/// <returns></returns>
		public static Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TReturn>(this DbConnection cnn, CommandDefinition command, Func<TFirst, TSecond, TReturn> map, string splitOn = "Id")
		{
			return MultiMapAsync<TFirst, TSecond, DontMap, DontMap, DontMap, DontMap, DontMap, TReturn>(cnn, command, map, splitOn);
		}

		/// <summary>
		/// Maps a query to objects
		/// </summary>
		/// <typeparam name="TFirst"></typeparam>
		/// <typeparam name="TSecond"></typeparam>
		/// <typeparam name="TThird"></typeparam>
		/// <typeparam name="TReturn"></typeparam>
		/// <param name="cnn"></param>
		/// <param name="sql"></param>
		/// <param name="map"></param>
		/// <param name="param"></param>
		/// <param name="transaction"></param>
		/// <param name="buffered"></param>
		/// <param name="splitOn">The Field we should split and read the second object from (default: id)</param>
		/// <param name="commandTimeout">Number of seconds before command execution timeout</param>
		/// <param name="commandType"></param>
		/// <returns></returns>
		public static Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TReturn>(this DbConnection cnn, string sql, Func<TFirst, TSecond, TThird, TReturn> map, dynamic param = null, DbTransaction transaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
		{
			return MultiMapAsync<TFirst, TSecond, TThird, DontMap, DontMap, DontMap, DontMap, TReturn>(cnn,
				new CommandDefinition(sql, (object)param, transaction, commandTimeout, commandType, buffered ? CommandFlags.Buffered : CommandFlags.None, default(CancellationToken)), map, splitOn);
		}

		/// <summary>
		/// Maps a query to objects
		/// </summary>
		/// <typeparam name="TFirst"></typeparam>
		/// <typeparam name="TSecond"></typeparam>
		/// <typeparam name="TThird"></typeparam>
		/// <typeparam name="TReturn"></typeparam>
		/// <param name="cnn"></param>
		/// <param name="splitOn">The field we should split and read the second object from (default: id)</param>
		/// <param name="command">The command to execute</param>
		/// <param name="map"></param>
		/// <returns></returns>
		public static Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TReturn>(this DbConnection cnn, CommandDefinition command, Func<TFirst, TSecond, TThird, TReturn> map, string splitOn = "Id")
		{
			return MultiMapAsync<TFirst, TSecond, TThird, DontMap, DontMap, DontMap, DontMap, TReturn>(cnn, command, map, splitOn);
		}

		/// <summary>
		/// Perform a multi mapping query with 4 input parameters
		/// </summary>
		/// <typeparam name="TFirst"></typeparam>
		/// <typeparam name="TSecond"></typeparam>
		/// <typeparam name="TThird"></typeparam>
		/// <typeparam name="TFourth"></typeparam>
		/// <typeparam name="TReturn"></typeparam>
		/// <param name="cnn"></param>
		/// <param name="sql"></param>
		/// <param name="map"></param>
		/// <param name="param"></param>
		/// <param name="transaction"></param>
		/// <param name="buffered"></param>
		/// <param name="splitOn"></param>
		/// <param name="commandTimeout"></param>
		/// <param name="commandType"></param>
		/// <returns></returns>
		public static Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TReturn>(this DbConnection cnn, string sql, Func<TFirst, TSecond, TThird, TFourth, TReturn> map, dynamic param = null, DbTransaction transaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
		{
			return MultiMapAsync<TFirst, TSecond, TThird, TFourth, DontMap, DontMap, DontMap, TReturn>(cnn,
				new CommandDefinition(sql, (object)param, transaction, commandTimeout, commandType, buffered ? CommandFlags.Buffered : CommandFlags.None, default(CancellationToken)), map, splitOn);
		}

		/// <summary>
		/// Perform a multi mapping query with 4 input parameters
		/// </summary>
		/// <typeparam name="TFirst"></typeparam>
		/// <typeparam name="TSecond"></typeparam>
		/// <typeparam name="TThird"></typeparam>
		/// <typeparam name="TFourth"></typeparam>
		/// <typeparam name="TReturn"></typeparam>
		/// <param name="cnn"></param>
		/// <param name="splitOn">The field we should split and read the second object from (default: id)</param>
		/// <param name="command">The command to execute</param>
		/// <param name="map"></param>
		/// <returns></returns>
		public static Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TReturn>(this DbConnection cnn, CommandDefinition command, Func<TFirst, TSecond, TThird, TFourth, TReturn> map, string splitOn = "Id")
		{
			return MultiMapAsync<TFirst, TSecond, TThird, TFourth, DontMap, DontMap, DontMap, TReturn>(cnn, command, map, splitOn);
		}

		/// <summary>
		/// Perform a multi mapping query with 5 input parameters
		/// </summary>
		public static Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TReturn>(this DbConnection cnn, string sql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TReturn> map, dynamic param = null, DbTransaction transaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
		{
			return MultiMapAsync<TFirst, TSecond, TThird, TFourth, TFifth, DontMap, DontMap, TReturn>(cnn,
				new CommandDefinition(sql, (object)param, transaction, commandTimeout, commandType, buffered ? CommandFlags.Buffered : CommandFlags.None, default(CancellationToken)), map, splitOn);
		}

		/// <summary>
		/// Perform a multi mapping query with 5 input parameters
		/// </summary>
		public static Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TReturn>(this DbConnection cnn, CommandDefinition command, Func<TFirst, TSecond, TThird, TFourth, TFifth, TReturn> map, string splitOn = "Id")
		{
			return MultiMapAsync<TFirst, TSecond, TThird, TFourth, TFifth, DontMap, DontMap, TReturn>(cnn, command, map, splitOn);
		}

		/// <summary>
		/// Perform a multi mapping query with 6 input parameters
		/// </summary>
		public static Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn>(this DbConnection cnn, string sql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn> map, dynamic param = null, DbTransaction transaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
		{
			return MultiMapAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, DontMap, TReturn>(cnn,
				new CommandDefinition(sql, (object)param, transaction, commandTimeout, commandType, buffered ? CommandFlags.Buffered : CommandFlags.None, default(CancellationToken)), map, splitOn);
		}

		/// <summary>
		/// Perform a multi mapping query with 6 input parameters
		/// </summary>
		public static Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn>(this DbConnection cnn, CommandDefinition command, Func<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn> map, string splitOn = "Id")
		{
			return MultiMapAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, DontMap, TReturn>(cnn, command, map, splitOn);
		}

		/// <summary>
		/// Perform a multi mapping query with 7 input parameters
		/// </summary>
		public static Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn>(this DbConnection cnn, string sql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn> map, dynamic param = null, DbTransaction transaction = null, bool buffered = true, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
		{
			return MultiMapAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn>(cnn,
				new CommandDefinition(sql, (object)param, transaction, commandTimeout, commandType, buffered ? CommandFlags.Buffered : CommandFlags.None, default(CancellationToken)), map, splitOn);
		}

		/// <summary>
		/// Perform a multi mapping query with 7 input parameters
		/// </summary>
		public static Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn>(this DbConnection cnn, CommandDefinition command, Func<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn> map, string splitOn = "Id")
		{
			return MultiMapAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn>(cnn, command, map, splitOn);
		}

		private static async Task<IEnumerable<TReturn>> MultiMapAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn>(this DbConnection cnn, CommandDefinition command, Delegate map, string splitOn)
		{
			object param = command.Parameters;
			var identity = new Identity(command.CommandText, command.CommandType, cnn, typeof(TFirst), param == null ? null : param.GetType(), new[] { typeof(TFirst), typeof(TSecond), typeof(TThird), typeof(TFourth), typeof(TFifth), typeof(TSixth), typeof(TSeventh) });
			var info = GetCacheInfo(identity, param);
			bool wasClosed = cnn.State == ConnectionState.Closed;
			try
			{
				if (wasClosed) await ((DbConnection)cnn).OpenAsync().ConfigureAwait(false);
				using (var cmd = (DbCommand)command.SetupCommand(cnn, info.ParamReader))
				using (var reader = await cmd.ExecuteReaderAsync(command.CancellationToken).ConfigureAwait(false))
				{
					var results = MultiMapImpl<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn>(null, default(CommandDefinition), map, splitOn, reader, identity);
					return await results.ToList();
				}
			}
			finally
			{
				if (wasClosed) cnn.Close();
			}
		}

		private static IEnumerable<T> ExecuteReader<T>(DbDataReader reader, Type effectiveType, Identity identity, CacheInfo info)
		{
			var tuple = info.Deserializer;
			int hash = GetColumnHash(reader);
			if (tuple.Func == null || tuple.Hash != hash)
			{
				tuple = info.Deserializer = new DeserializerState(hash, GetDeserializer(effectiveType, reader, 0, -1, false));
				SetQueryCache(identity, info);
			}

			var func = tuple.Func;

			while (reader.Read())
			{
				yield return (T)func(reader);
			}
		}
	}
}