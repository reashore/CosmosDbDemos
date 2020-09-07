using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;
using Microsoft.Extensions.Configuration;

using static System.Console;

namespace CosmosDb.ClientDemos.Demos
{
	public static class Cleanup
	{
        public static async Task Run()
        {
            WriteLine();
            WriteLine(">>> Cleanup <<<");

            // todo replace with Shared
            var config = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();

            var endpoint = config["CosmosEndpoint"];
            var masterKey = config["CosmosMasterKey"];

            using (CosmosClient client = new CosmosClient(endpoint, masterKey))
            {
                // Delete documents created by demos
                WriteLine("Deleting documents created by demos...");
                const string sql = @"
					SELECT c._self, c.address.postalCode
					FROM c
					WHERE
						STARTSWITH(c.name, 'New Customer') OR
						STARTSWITH(c.id, '_meta') OR
						IS_DEFINED(c.weekdayOff) OR
						STARTSWITH(c.id, 'DUPEJ') OR
						c.lastName = 'Einstein'
				";

                var container = Shared.Client.GetContainer("mydb", "mystore");

                List<dynamic> documentKeys = (await container.GetItemQueryIterator<dynamic>(sql).ReadNextAsync()).ToList();

                foreach (var documentKey in documentKeys)
                {
                    string id = documentKey.id;
                    string pk = documentKey.postalCode;
                    await container.DeleteItemAsync<dynamic>(id, new PartitionKey(pk));
                }

                // Delete all stored procedures
                WriteLine("Deleting all stored procedures...");
                FeedIterator<StoredProcedureProperties> sprocIterator = container.Scripts.GetStoredProcedureQueryIterator<StoredProcedureProperties>();
                FeedResponse<StoredProcedureProperties> sprocs = await sprocIterator.ReadNextAsync();

                foreach (var sproc in sprocs)
                {
                    await container.Scripts.DeleteStoredProcedureAsync(sproc.Id);
                }

                // Delete all triggers
                WriteLine("Deleting all triggers...");
                FeedIterator<TriggerProperties> triggerIterator = container.Scripts.GetTriggerQueryIterator<TriggerProperties>();
                FeedResponse<TriggerProperties> triggers = await triggerIterator.ReadNextAsync();

                foreach (var trigger in triggers)
                {
                    await container.Scripts.DeleteStoredProcedureAsync(trigger.Id);
                }

                // Delete all user defined functions
                WriteLine("Deleting all user defined functions...");
                FeedIterator<UserDefinedFunctionProperties> udfIterator = container.Scripts.GetUserDefinedFunctionQueryIterator<UserDefinedFunctionProperties>();
                FeedResponse<StoredProcedureProperties> udfs = await sprocIterator.ReadNextAsync();

                foreach (var udf in udfs)
                {
                    await container.Scripts.DeleteUserDefinedFunctionAsync(udf.Id);
                }

                //	// Delete all users
                //	Console.WriteLine("Deleting all users...");
                //	var databaseUri = UriFactory.CreateDatabaseUri("mydb");
                //	var users = client.CreateUserQuery(databaseUri).AsEnumerable();
                //	foreach (var user in users)
                //	{
                //		await client.DeleteUserAsync(user.SelfLink);
                //	}
            }
        }
    }
}
