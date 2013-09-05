using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reactive.Linq;
using System.Threading.Tasks;

namespace Dapper
{
    public static partial class SqlMapper
    {
        /// <summary>
        /// Execute a query asynchronously using .NET 4.5 Task.
        /// </summary>
        public static async Task<IEnumerable<T>> QueryAsync<T>(this DbConnection cnn, string sql, dynamic param = null, DbTransaction transaction = null, int? commandTimeout = null, CommandType? commandType = null)
        {
            var identity = new Identity(sql, commandType, cnn, typeof(T), param == null ? null : param.GetType(), null);
            var info = GetCacheInfo(identity);
            var cmd = (DbCommand)SetupCommand(cnn, transaction, sql, info.ParamReader, param, commandTimeout, commandType);

            using (var reader = await cmd.ExecuteReaderAsync())
            {
                return await ExecuteReader<T>(reader, identity, info).ToList().FirstAsync();
            }
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
	    /// <param name="splitOn">The Field we should split and read the second object from (default: id)</param>
	    /// <param name="commandTimeout">Number of seconds before command execution timeout</param>
	    /// <param name="commandType">Is it a stored proc or a batch?</param>
	    /// <returns></returns>
	    public static Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TReturn>(this DbConnection cnn, string sql, Func<TFirst, TSecond, TReturn> map, dynamic param = null, DbTransaction transaction = null, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
        {
            return MultiMapAsync<TFirst, TSecond, DontMap, DontMap, DontMap, DontMap, DontMap, TReturn>(cnn, sql, map, param as object, transaction, splitOn, commandTimeout, commandType);
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
	    /// <param name="splitOn">The Field we should split and read the second object from (default: id)</param>
	    /// <param name="commandTimeout">Number of seconds before command execution timeout</param>
	    /// <param name="commandType"></param>
	    /// <returns></returns>
	    public static async Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TReturn>(this DbConnection cnn, string sql, Func<TFirst, TSecond, TThird, TReturn> map, dynamic param = null, DbTransaction transaction = null, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
        {
            return await MultiMapAsync<TFirst, TSecond, TThird, DontMap, DontMap, DontMap, DontMap, TReturn>(cnn, sql, map, param as object, transaction, splitOn, commandTimeout, commandType);
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
	    /// <param name="splitOn"></param>
	    /// <param name="commandTimeout"></param>
	    /// <param name="commandType"></param>
	    /// <returns></returns>
	    public static Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TReturn>(this DbConnection cnn, string sql, Func<TFirst, TSecond, TThird, TFourth, TReturn> map, dynamic param = null, DbTransaction transaction = null, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
        {
            return MultiMapAsync<TFirst, TSecond, TThird, TFourth, DontMap, DontMap, DontMap, TReturn>(cnn, sql, map, param as object, transaction, splitOn, commandTimeout, commandType);
        }

	    /// <summary>
	    /// Perform a multi mapping query with 5 input parameters
	    /// </summary>
	    /// <typeparam name="TFirst"></typeparam>
	    /// <typeparam name="TSecond"></typeparam>
	    /// <typeparam name="TThird"></typeparam>
	    /// <typeparam name="TFourth"></typeparam>
	    /// <typeparam name="TFifth"></typeparam>
	    /// <typeparam name="TReturn"></typeparam>
	    /// <param name="cnn"></param>
	    /// <param name="sql"></param>
	    /// <param name="map"></param>
	    /// <param name="param"></param>
	    /// <param name="transaction"></param>
	    /// <param name="splitOn"></param>
	    /// <param name="commandTimeout"></param>
	    /// <param name="commandType"></param>
	    /// <returns></returns>
	    public static async Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TReturn>(this DbConnection cnn, string sql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TReturn> map, dynamic param = null, DbTransaction transaction = null, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
        {
            return await MultiMapAsync<TFirst, TSecond, TThird, TFourth, TFifth, DontMap, DontMap, TReturn>(cnn, sql, map, param as object, transaction, splitOn, commandTimeout, commandType);
        }

	    /// <summary>
	    /// Perform a multi mapping query with 6 input parameters
	    /// </summary>
	    /// <typeparam name="TFirst"></typeparam>
	    /// <typeparam name="TSecond"></typeparam>
	    /// <typeparam name="TThird"></typeparam>
	    /// <typeparam name="TFourth"></typeparam>
	    /// <typeparam name="TFifth"></typeparam>
	    /// <typeparam name="TSixth"></typeparam>
	    /// <typeparam name="TReturn"></typeparam>
	    /// <param name="cnn"></param>
	    /// <param name="sql"></param>
	    /// <param name="map"></param>
	    /// <param name="param"></param>
	    /// <param name="transaction"></param>
	    /// <param name="splitOn"></param>
	    /// <param name="commandTimeout"></param>
	    /// <param name="commandType"></param>
	    /// <returns></returns>
	    public static Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn>(this DbConnection cnn, string sql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TReturn> map, dynamic param = null, DbTransaction transaction = null, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
        {
            return MultiMapAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, DontMap, TReturn>(cnn, sql, map, param as object, transaction, splitOn, commandTimeout, commandType);
        }

	    /// <summary>
	    /// Perform a multi mapping query with 7 input parameters
	    /// </summary>
	    /// <typeparam name="TFirst"></typeparam>
	    /// <typeparam name="TSecond"></typeparam>
	    /// <typeparam name="TThird"></typeparam>
	    /// <typeparam name="TFourth"></typeparam>
	    /// <typeparam name="TFifth"></typeparam>
	    /// <typeparam name="TSixth"></typeparam>
	    /// <typeparam name="TSeventh"></typeparam>
	    /// <typeparam name="TReturn"></typeparam>
	    /// <param name="cnn"></param>
	    /// <param name="sql"></param>
	    /// <param name="map"></param>
	    /// <param name="param"></param>
	    /// <param name="transaction"></param>
	    /// <param name="splitOn"></param>
	    /// <param name="commandTimeout"></param>
	    /// <param name="commandType"></param>
	    /// <returns></returns>
	    public static Task<IEnumerable<TReturn>> QueryAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn>(this DbConnection cnn, string sql, Func<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn> map, dynamic param = null, DbTransaction transaction = null, string splitOn = "Id", int? commandTimeout = null, CommandType? commandType = null)
        {
            return MultiMapAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn>(cnn, sql, map, param as object, transaction, splitOn, commandTimeout, commandType);
        }

        static async Task<IEnumerable<TReturn>> MultiMapAsync<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn>(this DbConnection cnn, string sql, object map, object param, DbTransaction transaction, string splitOn, int? commandTimeout, CommandType? commandType)
        {
            var identity = new Identity(sql, commandType, cnn, typeof(TFirst), (object)param == null ? null : ((object)param).GetType(), new[] { typeof(TFirst), typeof(TSecond), typeof(TThird), typeof(TFourth), typeof(TFifth), typeof(TSixth), typeof(TSeventh) });
            var info = GetCacheInfo(identity);
            var cmd = (DbCommand)SetupCommand(cnn, transaction, sql, info.ParamReader, param, commandTimeout, commandType);
            using (var reader = await cmd.ExecuteReaderAsync())
            {
                var results = MultiMapImpl<TFirst, TSecond, TThird, TFourth, TFifth, TSixth, TSeventh, TReturn>(null, null, map, null, null, splitOn, null, null, reader, identity);
	            return await results.ToList().FirstAsync();
            }
        }

        private static IObservable<T> ExecuteReader<T>(DbDataReader reader, Identity identity, CacheInfo info)
        {
			var tuple = info.Deserializer;
			int hash = GetColumnHash(reader);
			if (tuple.Func == null || tuple.Hash != hash)
			{
				tuple = info.Deserializer = new DeserializerState(hash, GetDeserializer(typeof(T), reader, 0, -1, false));
				SetQueryCache(identity, info);
			}

			var func = tuple.Func;

	        return Observable.Create<T>(async observer =>
		        {
					while (await reader.ReadAsync())
					{
						observer.OnNext((T)func(reader));
					}
				}).Publish().RefCount();
        }
    }
}