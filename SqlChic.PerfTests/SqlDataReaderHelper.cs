using System;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace SqlChic.PerfTests
{
	static class SqlDataReaderHelper
	{
		public static string GetNullableString(this SqlDataReader reader, int index) 
		{
			object tmp = reader.GetValue(index);
			if (tmp != DBNull.Value)
			{
				return (string)tmp;
			}
			return null;
		}

		public static Nullable<T> GetNullableValue<T>(this SqlDataReader reader, int index) where T : struct
		{
			object tmp = reader.GetValue(index);
			if (tmp != DBNull.Value)
			{
				return (T)tmp;
			}
			return null;
		}

		public static async Task<string> GetNullableStringAsync(this SqlDataReader reader, int index)
		{
			object tmp = await reader.GetFieldValueAsync<object>(index);
			if (tmp != DBNull.Value)
			{
				return (string)tmp;
			}
			return null;
		}

		public static async Task<Nullable<T>> GetNullableValueAsync<T>(this SqlDataReader reader, int index) where T : struct
		{
			object tmp = await reader.GetFieldValueAsync<object>(index);
			if (tmp != DBNull.Value)
			{
				return (T)tmp;
			}
			return null;
		}
	}
}