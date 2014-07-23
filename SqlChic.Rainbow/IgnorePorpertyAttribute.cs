using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlChic.Rainbow
{
	[AttributeUsage(AttributeTargets.Property, AllowMultiple = true)]
	public class IgnorePropertyAttribute : Attribute
	{
		public IgnorePropertyAttribute(bool ignore)
		{
			Value = ignore;
		}

		public bool Value { get; set; }
	}
}
