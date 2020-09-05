using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;

namespace CosmosDb.ClientDemos.Demos
{
	public static class Shared
	{
		public static CosmosClient Client { get; }

		static Shared()
		{
			IConfigurationRoot config = new ConfigurationBuilder().AddJsonFile("appsettings.json").Build();
            string endpoint = config["CosmosEndpoint"];
            string masterKey = config["CosmosMasterKey"];

			Client = new CosmosClient(endpoint, masterKey);
		}
	}
}
