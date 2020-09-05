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
            const string databaseName = "Database1";

            Debugger.Break();

			await ViewDatabases();

			await CreateDatabase(databaseName);
			await ViewDatabases();

			await DeleteDatabase(databaseName);
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

		public static async Task CreateDatabase(string databaseName)
		{
			WriteLine($"\n>>> Create Database {databaseName} <<<");

			DatabaseResponse result = await Shared.Client.CreateDatabaseIfNotExistsAsync(databaseName);
			DatabaseProperties database = result.Resource;
            // todo check StatusCode

			WriteLine($" Database Id: {database.Id}; Modified: {database.LastModified}");
		}

		private static async Task DeleteDatabase(string databaseName)
		{
			WriteLine($"\n>>> Delete Database {databaseName} <<<");

			DatabaseResponse result = await Shared.Client.GetDatabase(databaseName).DeleteAsync();
			// todo check result.StatusCode
		}
    }
}
