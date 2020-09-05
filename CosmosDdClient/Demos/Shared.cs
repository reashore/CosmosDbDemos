using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;

namespace CosmosDb.ClientDemos.Demos
{
	public static class Shared
	{
		public static CosmosClient Client { get; }

		static Shared()
		{
			IConfigurationRoot configurationRoot = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();
            string endpoint = configurationRoot["CosmosEndpoint"];
            string masterKey = configurationRoot["CosmosMasterKey"];

            CosmosClientOptions cosmosClientOptions = new CosmosClientOptions()
            {
                ApplicationName = "Cosmos DB Demos"
            };

			Client = new CosmosClient(endpoint, masterKey, cosmosClientOptions);
		}
	}
}
