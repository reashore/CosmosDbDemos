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
			Debugger.Break();

			await ViewContainers();

			await CreateContainer("MyContainer1");
			await CreateContainer("MyContainer2", 1000, "/state");
			await ViewContainers();

			await DeleteContainer("MyContainer1");
			await DeleteContainer("MyContainer2");
			await ViewContainers();
		}

		private static async Task ViewContainers()
		{
			WriteLine();
			WriteLine(">>> View Containers in mydb <<<");

			var database = Shared.Client.GetDatabase("mydb");
			FeedIterator<ContainerProperties> iterator = database.GetContainerQueryIterator<ContainerProperties>();
			FeedResponse<ContainerProperties> containers = await iterator.ReadNextAsync();

			var count = 0;
			foreach (var container in containers)
			{
				count++;
				WriteLine();
				WriteLine($" Container #{count}");
				await ViewContainer(container);
			}

			WriteLine();
			WriteLine($"Total containers in mydb database: {count}");
		}

		private static async Task ViewContainer(ContainerProperties containerProperties)
		{
			WriteLine($"     Container ID: {containerProperties.Id}");
			WriteLine($"    Last Modified: {containerProperties.LastModified}");
			WriteLine($"    Partition Key: {containerProperties.PartitionKeyPath}");

			var container = Shared.Client.GetContainer("mydb", containerProperties.Id);
			int? throughput = await container.ReadThroughputAsync();

			WriteLine($"       Throughput: {throughput}");
		}

		private static async Task CreateContainer(
			string containerId,
			int throughput = 400,
			string partitionKey = "/partitionKey")
		{
			WriteLine();
			WriteLine($">>> Create Container {containerId} in mydb <<<");
			WriteLine();
			WriteLine($" Throughput: {throughput} RU/sec");
			WriteLine($" Partition key: {partitionKey}");
			WriteLine();

			var containerDef = new ContainerProperties
			{
				Id = containerId,
				PartitionKeyPath = partitionKey,
			};

			var database = Shared.Client.GetDatabase("mydb");
			var result = await database.CreateContainerAsync(containerDef, throughput);
			var container = result.Resource;

			WriteLine("Created new container");
			await ViewContainer(container);	// Intermittent failures!
		}

		private static async Task DeleteContainer(string containerId)
		{
			WriteLine();
			WriteLine($">>> Delete Container {containerId} in mydb <<<");

			var container = Shared.Client.GetContainer("mydb", containerId);
			await container.DeleteContainerAsync();

			WriteLine($"Deleted container {containerId} from database mydb");
		}

	}
}
