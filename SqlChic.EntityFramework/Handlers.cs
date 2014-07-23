using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlChic.EntityFramework
{
	/// <summary>
	/// Acts on behalf of all type-handlers in this package
	/// </summary>
	public static class Handlers
	{
		/// <summary>
		/// Register all type-handlers in this package
		/// </summary>
		public static void Register()
		{
			SqlMapper.AddTypeHandler(DbGeographyHandler.Default);
		}
	}
}
