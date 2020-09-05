using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;

using static System.Console;

namespace CosmosDb.ClientDemos.Demos
{
	public static class DatabasesDemo
	{
		public static async Task Run()
		{
			Debugger.Break();

			await ViewDatabases();

			await CreateDatabase();
			await ViewDatabases();

			await DeleteDatabase();
			await ViewDatabases();
		}

		private static async Task ViewDatabases()
		{
			WriteLine("\n>>> View Databases <<<");

			FeedIterator<DatabaseProperties> iterator = Shared.Client.GetDatabaseQueryIterator<DatabaseProperties>();
			FeedResponse<DatabaseProperties> databases = await iterator.ReadNextAsync();

            if (databases.Count == 0)
            {
				WriteLine("No databases");
                return;
            }

            var count = 0;

			foreach (DatabaseProperties database in databases)
			{
				count++;
				WriteLine($" Database Id: {database.Id}; Modified: {database.LastModified}");
			}

			WriteLine($"\nTotal databases: {count}");
		}

		private static async Task CreateDatabase()
		{
			WriteLine("\n>>> Create Database <<<");

			var result = await Shared.Client.CreateDatabaseAsync("MyNewDatabase");
			var database = result.Resource;

			WriteLine($" Database Id: {database.Id}; Modified: {database.LastModified}");
		}

		private static async Task DeleteDatabase()
		{
			WriteLine("\n>>> Delete Database <<<");

			await Shared.Client.GetDatabase("MyNewDatabase").DeleteAsync();
		}
    }
}
