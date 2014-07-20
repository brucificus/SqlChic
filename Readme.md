SqlChic - a simple *async* object mapper for .Net
===============================================

Features
--------
SqlChic extends your DbConnection with extension methods to make fully async database queries a breeze.

Execute a query and map the results to a strongly typed IObservable
-------------------------------------------------------------------

Note: all extension methods leave the connection in the same open/closed state that it started in.

```csharp
public static IObservable<T> Query<T>(this DbConnection cnn, string sql, object param = null, DbTransaction transaction = null)
```
Example usage:

```csharp
public class Dog
{
    public int? Age { get; set; }
    public Guid Id { get; set; }
    public string Name { get; set; }
    public float? Weight { get; set; }

    public int IgnoredProperty { get { return 1; } }
}            
            
var guid = Guid.NewGuid();
var dogs = await connection.Query<Dog>("select Age = @Age, Id = @Id", new { Age = (int?)null, Id = guid }).ToArray();
            
dogs.Count()
    .IsEqualTo(1);

dogs.First().Age
    .IsNull();

dogs.First().Id
    .IsEqualTo(guid);
```

Execute a query and map it to an IObservable of dynamic objects
---------------------------------------------------------------

```csharp
public static IObservable<dynamic> Query(this DbConnection cnn, string sql, object param = null, DbTransaction transaction = null)
```
This method will execute SQL and return a dynamic list.

Example usage:

```csharp
var rows = await connection.Query("select 1 A, 2 B union all select 3, 4").ToArray();

((int)rows[0].A)
   .IsEqualTo(1);

((int)rows[0].B)
   .IsEqualTo(2);

((int)rows[1].A)
   .IsEqualTo(3);

((int)rows[1].B)
    .IsEqualTo(4);
```

Execute a Command that returns no results
-----------------------------------------

```csharp
public static async Task<int> ExecuteAsync(this DbConnection cnn, string sql, object param = null, DbTransaction transaction = null)
```

Example usage:

```csharp
await connection.ExecuteAsync(@"
  set nocount on 
  create table #t(i int) 
  set nocount off 
  insert #t 
  select @a a union all select @b 
  set nocount on 
  drop table #t", new {a=1, b=2 })
   .IsEqualTo(2);
```

Execute a Command multiple times
--------------------------------

The same signature also allows you to conveniently and efficiently execute a command multiple times (for example to bulk-load data)

Example usage:

```csharp
await connection.ExecuteAsync(@"insert MyTable(colA, colB) values (@a, @b)",
    new[] { new { a=1, b=1 }, new { a=2, b=2 }, new { a=3, b=3 } }
  ).IsEqualTo(3); // 3 rows inserted: "1,1", "2,2" and "3,3"
```
This works for any parameter that implements IEnumerable<T> for some T.

Performance
-----------

The key feature of SqlChic is performance. The following metrics show how long it takes to execute SELECT statements against a DB and map the data returned to objects.

### Performance of SELECT mapping averaged over 5000 iterations

<table>
	<tr>
  		<th>Method</th>
		<th>Average Duration</th>		
		<th>Times Slower than SqlChic</th>
		<th>Remarks</th>
	</tr>
	<tr>
		<td>SqlChic (Buffered)</td>
		<td>0.0102ms</td>
		<td>1x</td>
	</tr>
	<tr>
		<td><a href="http://www.toptensoftware.com/petapoco/">PetaPoco</a></td>
		<td>0.1911ms</td>
		<td>19x</td>
	</tr>
	<tr>
		<td>Dapper (Non-buffered, Async)</td>
		<td>0.2663ms</td>
		<td>26x</td>
	</tr>
	<tr>
		<td>Hand coded (using a <code>SqlDataReader</code>, Async)</td>
		<td>0.2691ms</td>
		<td>26x</td>
	</tr>
	<tr>
		<td>Entity Framework (LINQ)</td>
		<td>0.3786ms</td>
		<td>37x</td>
	</tr>
	<tr>
		<td>NHibernate SQL</td>
		<td>0.3836ms</td>
		<td>38x</td>
	</tr>
	<tr>
		<td>Linq2Sql (CompiledQuery)</td>
		<td>0.3866ms</td>
		<td>38x</td>
		<td>Not super typical involves complex code</td>
	</tr>
	<tr>
		<td>SubSonic CodingHorror</td>
		<td>0.3866ms</td>
		<td>38x</td>
	</tr>
	<tr>
		<td>NHibernate HQL</td>
		<td>0.3876ms</td>
		<td>38x</td>
	</tr>
	<tr>
		<td>BLToolkit</td>
		<td>0.3914ms</td>
		<td>38x</td>
	</tr>
	<tr>
		<td>Linq2Sql <code>ExecuteQuery</code></td>
		<td>0.5058ms</td>
		<td>50x</td>
	</tr>
	<tr>
		<td>Linq2Sql (LINQ)</td>
		<td>1.1453ms</td>
		<td>112x</td>
	</tr>
	<tr>
		<td>Entity Framework (<code>ExecuteStoreQuery</code>)</td>
		<td>1.3787ms</td>
		<td>135x</td>
	</tr>
	<tr>
		<td>SubSonic ActiveRecord.SingleOrDefault</td>
		<td>7.3637ms</td>
		<td>722x</td>
	</tr>
</table>

Performance benchmarks are available as part of the source code.

Feel free to submit patches that include other ORMs - when running benchmarks, be sure to compile in Release and not attach a debugger (ctrl F5)

Parameterized queries
---------------------

Parameters are passed in as anonymous classes. This allow you to name your parameters easily and gives you the ability to simply cut-and-paste SQL snippets and run them in Query analyzer.

```csharp
new {A = 1, B = "b"} // A will be mapped to the param @A, B to the param @B 
```

List Support
------------
SqlChic allows you to pass in IEnumerable<int> and will automatically parameterize your query.

For example:

```csharp
connection.Query<int>("select * from (select 1 as Id union all select 2 union all select 3) as X where Id in @Ids", new { Ids = new int[] { 1, 2, 3 });
```

Will be translated to:

```csharp
select * from (select 1 as Id union all select 2 union all select 3) as X where Id in (@Ids1, @Ids2, @Ids3)" // @Ids1 = 1 , @Ids2 = 2 , @Ids2 = 3
```

Buffered vs Unbuffered readers
---------------------
SqlChic's default behavior is to execute your SQL and buffer the entire reader on return. This is ideal in most cases as it minimizes shared locks in the db and cuts down on db network time.

However when executing huge queries you may need to minimize memory footprint and only load objects as needed.

SqlChich will use buffered readers if the db connection it receives is closed. If the db connection is already open, SqlChic will not use buffered readers.

Multi Mapping
---------------------
SqlChic allows you to map a single row to multiple objects. This is a key feature if you want to avoid extraneous querying and eager load associations.

Example:

```csharp
var sql = 
@"select * from #Posts p 
left join #Users u on u.Id = p.OwnerId 
Order by p.Id";
 
var data = connection.QueryAsync<Post, User, Post>(sql, (post, user) => { post.Owner = user; return post;});
var post = await data.FirstAsync();
 
post.Content.IsEqualTo("Sams Post1");
post.Id.IsEqualTo(1);
post.Owner.Name.IsEqualTo("Sam");
post.Owner.Id.IsEqualTo(99);
```

**important note** SqlChic assumes your Id columns are named "Id" or "id", if your primary key is different or you would like to split the wide row at point other than "Id", use the optional 'splitOn' parameter.

Multiple Results
---------------------
SqlChic allows you to process multiple result grids in a single query.

Example:

```csharp
var sql = 
@"
select * from Customers where CustomerId = @id
select * from Orders where CustomerId = @id
select * from Returns where CustomerId = @id";
 
using (var multi = await connection.QueryMultipleAsync(sql, new {id=selectedId}))
{
   var customer = await multi.Read<Customer>().SingleAsync();
   var orders = await multi.Read<Order>().ToList();
   var returns = await multi.Read<Return>().ToList();
   ...
} 
```

Stored Procedures
---------------------
SqlChic supports stored procs:

```csharp
var user = await cnn.Query<User>("spGetUser", new {Id = 1}, 
        commandType: CommandType.StoredProcedure).FirstAsync();}}}
```

...though output parameters are still a work in progress.

Ansi Strings and varchar
---------------------
SqlChic supports varchar params, if you are executing a where clause on a varchar column using a param be sure to pass it in this way:

```csharp
Query<Thing>("select * from Thing where Name = @Name", new {Name = new DbString { Value = "abcde", IsFixedLength = true, Length = 10, IsAnsi = true });
```

On Sql Server it is crucial to use the unicode when querying unicode and ansi when querying non unicode.

Limitations and caveats
---------------------
SqlChic caches information about every query it runs, this allow it to materialize objects quickly and process parameters quickly. The current implementation caches this information in a ConcurrentDictionary object. The objects it stores are never flushed. If you are generating SQL strings on the fly without using parameters it is possible you will hit memory issues. We may convert the dictionaries to an LRU Cache.

SqlChic's simplicity means that many feature that ORMs ship with are stripped out, there is no identity map, there are no helpers for update / select and so on.

SqlChic does not manage your connection's lifecycle, it assumes the connection it gets is open (or open-able) AND has no existing datareaders enumerating (unless MARS is enabled)

Will SqlChic work with my db provider?
---------------------
SqlChic currently only supports MS SQL, to ensure full availability of async functionality.

Do you have a comprehensive list of examples?
---------------------
SqlChic has a comprehensive test suite in the source code.

Is this a Dapper fork?
----------------------
It is! Except it is fully and thoroughly async, down to the minutiae of using IObservable to represent data streaming back out of the database.