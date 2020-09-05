using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;

using static System.Console;

namespace CosmosDb.ClientDemos.Demos
{
	public static class ContainersDemo
	{
		public static async Task Run()
		{
			// todo create instance variable
            const string databaseName = "Database1";
            const string container1Id = "Container1";
            const string container2Id = "Container2";
            const string partitionKey = "/state";
            const int throughput = 400;

            Debugger.Break();

			await DatabasesDemo.CreateDatabase(databaseName);

			await ViewContainers(databaseName);

            await CreateContainer(databaseName, container1Id, throughput, partitionKey);
			await CreateContainer(databaseName, container2Id, throughput, partitionKey);
			await ViewContainers(databaseName);

			await DeleteContainer(databaseName, container1Id);
			await DeleteContainer(databaseName, container2Id);
			await ViewContainers(databaseName);
		}

		private static async Task ViewContainers(string databaseName)
		{
			WriteLine($"\n>>> View Containers in {databaseName} <<<");

			var database = Shared.Client.GetDatabase(databaseName);
			FeedIterator<ContainerProperties> iterator = database.GetContainerQueryIterator<ContainerProperties>();
			FeedResponse<ContainerProperties> containers = await iterator.ReadNextAsync();

            if (containers.Count == 0)
            {
				WriteLine($"No containers in database {databaseName}");
            }

			var count = 0;

			foreach (var container in containers)
			{
				count++;
				WriteLine($"\n Container #{count}");
				await ViewContainer(databaseName, container);
			}

			WriteLine($"\nTotal containers in mydb database: {count}");
		}

		private static async Task ViewContainer(string databaseName, ContainerProperties containerProperties)
		{
			WriteLine($"    Container ID: {containerProperties.Id}");
			WriteLine($"    Last Modified: {containerProperties.LastModified}");
			WriteLine($"    Partition Key: {containerProperties.PartitionKeyPath}");

			var container = Shared.Client.GetContainer(databaseName, containerProperties.Id);
			int? throughput = await container.ReadThroughputAsync();

			WriteLine($"       Throughput: {throughput}");
		}

		private static async Task CreateContainer(string databaseName, string containerId, int throughput, string partitionKey)
		{
			WriteLine($"\n>>> Create Container {containerId} in {databaseName} <<<");
			WriteLine($"\n Throughput: {throughput} RU/sec");
			WriteLine($" Partition key: {partitionKey}\n");

			ContainerProperties containerDef = new ContainerProperties
			{
				Id = containerId,
				PartitionKeyPath = partitionKey,
			};

			Database database = Shared.Client.GetDatabase(databaseName);
			ContainerResponse result = await database.CreateContainerIfNotExistsAsync(containerDef, throughput);
			ContainerProperties containerProperties = result.Resource;

			WriteLine("Created new container");
			await ViewContainer(databaseName, containerProperties);	// Intermittent failures!
		}

		private static async Task DeleteContainer(string databaseName, string containerId)
		{
			WriteLine($"\n>>> Delete Container {containerId} in {databaseName} <<<");

			var container = Shared.Client.GetContainer(databaseName, containerId);
			await container.DeleteContainerAsync();

			WriteLine($"Deleted container {containerId} from database {databaseName}");
		}

	}
}
