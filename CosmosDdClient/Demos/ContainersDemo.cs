using System.Diagnostics;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;

using static System.Console;

namespace CosmosDb.ClientDemos.Demos
{
	public class ContainersDemo
    {
        private readonly string _databaseName;

        public ContainersDemo(string databaseName)
        {
            _databaseName = databaseName;
        }

		public async Task Run()
		{
            const string container1Id = "Container1";
            const string container2Id = "Container2";
            const string partitionKey = "/state";
            const int throughput = 400;

            Debugger.Break();

			await DatabasesDemo.CreateDatabase(_databaseName);
            await ViewContainers();

            await CreateContainer(_databaseName, container1Id);
            await ViewContainers();
			await CreateContainer(_databaseName, container2Id);
			await ViewContainers();

			await DeleteContainer(container1Id);
			await DeleteContainer(container2Id);
			await ViewContainers();
		}

		private async Task ViewContainers()
		{
			WriteLine($"\n>>> View Containers in {_databaseName} <<<");

			var database = Shared.Client.GetDatabase(_databaseName);
			FeedIterator<ContainerProperties> iterator = database.GetContainerQueryIterator<ContainerProperties>();
			FeedResponse<ContainerProperties> containers = await iterator.ReadNextAsync();

            if (containers.Count == 0)
            {
				WriteLine($"No containers in database {_databaseName}");
            }

			var count = 0;

			foreach (var container in containers)
			{
				count++;
				WriteLine($"\n Container #{count}");
				await ViewContainer(container);
			}

			WriteLine($"\nTotal containers in mydb database: {count}");
		}

		private async Task ViewContainer(ContainerProperties containerProperties)
		{
			WriteLine($"    Container ID: {containerProperties.Id}");
			WriteLine($"    Last Modified: {containerProperties.LastModified}");
			WriteLine($"    Partition Key: {containerProperties.PartitionKeyPath}");

			var container = Shared.Client.GetContainer(_databaseName, containerProperties.Id);
			int? throughput = await container.ReadThroughputAsync();

			WriteLine($"       Throughput: {throughput}");
		}

		public static async Task CreateContainer(string databaseName, string containerId, int throughput = 400, string partitionKey = "/state")
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
		}

		private async Task DeleteContainer(string containerId)
		{
			WriteLine($"\n>>> Delete Container {containerId} in {_databaseName} <<<");

			var container = Shared.Client.GetContainer(_databaseName, containerId);
			await container.DeleteContainerAsync();

			WriteLine($"Deleted container {containerId} from database {_databaseName}");
		}
    }
}
