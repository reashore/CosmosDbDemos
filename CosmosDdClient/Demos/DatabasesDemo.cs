using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;

using static System.Console;

namespace CosmosDb.ClientDemos.Demos
{
	public class DatabasesDemo
	{
        private readonly string _databaseName;

        public DatabasesDemo(string databaseName)
        {
            _databaseName = databaseName;
        }

		public async Task Run()
		{
            Debugger.Break();

			await ViewDatabases();

			await CreateDatabase(_databaseName);
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

		public static async Task CreateDatabase(string databaseName)
		{
			WriteLine($"\n>>> Create Database {databaseName} <<<");

			DatabaseResponse result = await Shared.Client.CreateDatabaseIfNotExistsAsync(databaseName);
			DatabaseProperties database = result.Resource;
            // todo check StatusCode

			WriteLine($" Database Id: {database.Id}; Modified: {database.LastModified}");
		}

		private async Task DeleteDatabase()
		{
			WriteLine($"\n>>> Delete Database {_databaseName} <<<");

			DatabaseResponse result = await Shared.Client.GetDatabase(_databaseName).DeleteAsync();
			// todo check result.StatusCode
		}
    }
}
