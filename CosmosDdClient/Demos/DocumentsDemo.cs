using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

using static System.Console;

namespace CosmosDb.ClientDemos.Demos
{
	public class DocumentsDemo
	{
        private readonly string _databaseName;
        private readonly string _containerName;

        public DocumentsDemo(string databaseName, string containerName)
        {
            _databaseName = databaseName;
            _containerName = containerName;
        }

		public async Task Run()
        {
            const string partitionKey = "/address/postalCode";

			Debugger.Break();

            await DatabasesDemo.CreateDatabase(_databaseName);
            await CreateContainer(_containerName, partitionKey);

            CreateDocuments();
            QueryDocuments();

            QueryWithUnstreamedStatefulPaging();
			await QueryWithUnstreamedStatelessPaging();

            await QueryWithStreamedStatefulPaging();
            await QueryWithStreamedStatelessPaging();

			QueryWithLinq();

			await ReplaceDocuments();

			await DeleteDocuments();
		}

        private async Task CreateContainer(string containerId, string partitionKey, int throughput = 400)
        {
            ContainerProperties containerDef = new ContainerProperties
            {
                Id = containerId,
                PartitionKeyPath = partitionKey,
            };

            Database database = Shared.Client.GetDatabase(_databaseName);
            ContainerResponse result = await database.CreateContainerIfNotExistsAsync(containerDef, throughput);

            // todo check status code
            //HttpStatusCode foo = result.StatusCode;
        }

		#region Create Documents

		private void CreateDocuments()
        {
            Clear();
            WriteLine(">>> Create Documents <<<\n");
            
            Container container = Shared.Client.GetContainer(_databaseName, _containerName);
            PartitionKey partitionKey = new PartitionKey("11229");

			CreateDocumentFromDynamicType(container, partitionKey);
            CreateDocumentsFromJson(container, partitionKey);
            CreateDocumentFromPoco(container, partitionKey);
        }

        private static async void CreateDocumentFromDynamicType(Container container, PartitionKey partitionKey)
        {
			dynamic customerDocument = new
            {
                id = Guid.NewGuid(),
                name = "New Customer 1",
                address = new
                {
                    addressType = "Main Office",
                    addressLine1 = "123 Main Street",
                    location = new
                    {
                        city = "Brooklyn",
                        stateProvinceName = "New York"
                    },
                    postalCode = "11229",
                    countryRegionName = "United States"
                },
            };


            await container.CreateItemAsync(customerDocument, partitionKey);
            WriteLine($"Created new document {customerDocument.id} from dynamic object");
        }

		private static async void CreateDocumentsFromJson(Container container, PartitionKey partitionKey)
        {
			string customerJson = $@"
				{{
					""id"": ""{Guid.NewGuid()}"",
					""name"": ""New Customer 2"",
					""address"": {{
						""addressType"": ""Main Office"",
						""addressLine1"": ""123 Main Street"",
						""location"": {{
							""city"": ""Brooklyn"",
							""stateProvinceName"": ""New York""
						}},
						""postalCode"": ""11229"",
						""countryRegionName"": ""United States""
					}}
				}}";

            JObject customerJObject = JsonConvert.DeserializeObject<JObject>(customerJson);
            await container.CreateItemAsync(customerJObject, partitionKey);
            WriteLine($"Created new document {customerJObject["id"].Value<string>()} from JSON string");
        }

		private static async void CreateDocumentFromPoco(Container container, PartitionKey partitionKey)
        {
			var customer = new Customer
            {
                Id = Guid.NewGuid().ToString(),
                Name = "New Customer 3",
                Address = new Address
                {
                    AddressType = "Main Office",
                    AddressLine1 = "123 Main Street",
                    Location = new Location
                    {
                        City = "Brooklyn",
                        StateProvinceName = "New York"
                    },
                    PostalCode = "11229",
                    CountryRegionName = "United States"
                },
            };

            await container.CreateItemAsync(customer, partitionKey);
            WriteLine($"Created new document {customer.Id} from typed object");
		}

		#endregion

		#region Query Documents

		private void QueryDocuments()
		{
			Clear();
			WriteLine(">>> Query Documents (SQL) <<</n");
            WriteLine("Querying for new customer documents (SQL)");

            var container = Shared.Client.GetContainer(_databaseName, _containerName);
            const string sql = "SELECT * FROM c WHERE STARTSWITH(c.name, 'New Customer') = true";

            QueryUsingDynamicType(container, sql);
            QueryUsingPoco(container, sql);
		}

        private async void QueryUsingDynamicType(Container container, string sql)
        {
            FeedIterator<dynamic> iterator = container.GetItemQueryIterator<dynamic>(sql);
            FeedResponse<dynamic> documents = await iterator.ReadNextAsync();
            int count = 0;

            foreach (var document in documents)
            {
                WriteLine($" ({++count}) Id: {document.id}; Name: {document.name};");

                // Convert dynamic object to poco
                Customer customer = JsonConvert.DeserializeObject<Customer>(document.ToString());
                WriteLine($"     City: {customer.Address.Location.City}");
            }

            WriteLine($"Retrieved {count} new documents as dynamic\n");
        }

		private async void QueryUsingPoco(Container container, string sql)
        {
            FeedIterator<Customer> iterator = container.GetItemQueryIterator<Customer>(sql);
            FeedResponse<Customer> customers = await iterator.ReadNextAsync();
            int count = 0;

            foreach (Customer customer in customers)
            {
                WriteLine($" ({++count}) Id: {customer.Id}; Name: {customer.Name};");
                WriteLine($"     City: {customer.Address.Location.City}");
            }

            WriteLine($"Retrieved {count} new documents as Customer\n");
        }

        #endregion

        #region Unstreamed Statefull Paging

        private void QueryWithUnstreamedStatefulPaging()
		{
			Clear();
			WriteLine(">>> Query Documents (paged results, stateful) <<<\n");

			Container container = Shared.Client.GetContainer(_databaseName, _containerName);
            const string sql = "SELECT * FROM c";

            GetFirstPageFromResultSet(container, sql);
            GetAllPagesFromResultSet(container, sql);
		}

        private static async void GetFirstPageFromResultSet(Container container, string sql)
        {
            FeedIterator<Customer> feedIterator = container.GetItemQueryIterator<Customer>(sql);
            FeedResponse<Customer> customers = await feedIterator.ReadNextAsync();
            int itemCount = 0;

            foreach (Customer customer in customers)
            {
                WriteLine($" ({++itemCount}) Id: {customer.Id}; Name: {customer.Name};");
            }

            WriteLine($"Retrieved {itemCount} documents in first page\n");
        }

        private static async void GetAllPagesFromResultSet(Container container, string sql)
        {
            var feedIterator = container.GetItemQueryIterator<Customer>(sql);
            int itemCount = 0;
            int pageCount = 0;

            while (feedIterator.HasMoreResults)
            {
                pageCount++;
                FeedResponse<Customer> customers = await feedIterator.ReadNextAsync();

                foreach (var customer in customers)
                {
                    WriteLine($" ({pageCount}.{++itemCount}) Id: {customer.Id}; Name: {customer.Name};");
                }
            }

            WriteLine($"Retrieved {itemCount} documents from multi-page result set\n");
        }

        #endregion

        #region Unstreamed Stateless Paging

        private async Task QueryWithUnstreamedStatelessPaging()
		{
			string continuationToken = default;
            const string sql = "SELECT * FROM c";
            Container container = Shared.Client.GetContainer(_databaseName, _containerName);

            do
            {
				continuationToken = await QueryFetchNextPage(container, sql, continuationToken);
			} 
            while (continuationToken != null);
		}

		private static async Task<string> QueryFetchNextPage(Container container, string sql, string continuationToken)
		{
            FeedIterator<Customer> feedIterator = container.GetItemQueryIterator<Customer>(sql, continuationToken);
			FeedResponse<Customer> customers = await feedIterator.ReadNextAsync();
			int itemCount = 0;

            foreach (Customer customer in customers)
			{
				WriteLine($" ({++itemCount}) Id: {customer.Id}; Name: {customer.Name};");
			}

			continuationToken = customers.ContinuationToken;

			return continuationToken;
		}

        #endregion

        #region Steamed Stateful Paging

        private async Task QueryWithStreamedStatefulPaging()
        {
            Container container = Shared.Client.GetContainer(_databaseName, _containerName);

            const string sql = "SELECT * FROM c";
            FeedIterator feedIterator = container.GetItemQueryStreamIterator(sql);
            int itemCount = 0;
            int pageCount = 0;

            while (feedIterator.HasMoreResults)
            {
                pageCount++;
                ResponseMessage results = await feedIterator.ReadNextAsync();
                Stream stream = results.Content;

                using (StreamReader streamReader = new StreamReader(stream))
                {
                    string json = await streamReader.ReadToEndAsync();
                    JObject deserializeObject = JsonConvert.DeserializeObject<JObject>(json);
                    JArray jArray = (JArray)deserializeObject["Documents"];

                    foreach (JToken item in jArray)
                    {
                        Customer customer = JsonConvert.DeserializeObject<Customer>(item.ToString());
                        WriteLine($" ({pageCount}.{++itemCount}) Id: {customer.Id}; Name: {customer.Name};");
                    }
                }
            }

            WriteLine($"Retrieved {itemCount} documents\n");
        }

        private async Task QueryWithStreamedStatelessPaging()
        {
            string continuationToken = default;

            do
            {
                continuationToken = await QueryFetchNextPageStreamed(continuationToken);
            } 
            while (continuationToken != null);

            WriteLine("Retrieved all documents\n");
        }

        private async Task<string> QueryFetchNextPageStreamed(string continuationToken)
        {
            var container = Shared.Client.GetContainer(_databaseName, _containerName);

            const string sql = "SELECT * FROM c";
            FeedIterator streamIterator = container.GetItemQueryStreamIterator(sql, continuationToken);
            ResponseMessage response = await streamIterator.ReadNextAsync();
            Stream stream = response.Content;
            int itemCount = 0;

            using (StreamReader streamReader = new StreamReader(stream))
            {
                string json = await streamReader.ReadToEndAsync();
                JObject jObject = JsonConvert.DeserializeObject<JObject>(json);
                JArray jArray = (JArray)jObject["Documents"];

                foreach (JToken item in jArray)
                {
                    var customer = JsonConvert.DeserializeObject<Customer>(item.ToString());
                    WriteLine($" ({++itemCount}) Id: {customer.Id}; Name: {customer.Name};");
                }
            }

            continuationToken = response.Headers.ContinuationToken;
            return continuationToken;
        }

        #endregion

        private void QueryWithLinq()
		{
			Container container = Shared.Client.GetContainer(_databaseName, _containerName);
            const string regionName = "United Kingdom";
            IOrderedQueryable<Customer> customers = container.GetItemLinqQueryable<Customer>(true);

            var query = 
                from d in customers
				where d.Address.CountryRegionName == regionName
                select new
				{
					d.Id,
					d.Name,
					d.Address.Location.City
				};

			var documents = query.ToList();

			WriteLine($"Found {documents.Count} UK customers");

			foreach (var document in documents)
			{
				dynamic d = document;
				WriteLine($" Id: {d.Id}; Name: {d.Name}; City: {d.City}");
			}
		}

		private async Task ReplaceDocuments()
		{
			Container container = Shared.Client.GetContainer(_databaseName, _containerName);

			string sql = "SELECT VALUE COUNT(c) FROM c WHERE c.isNew = true";
            FeedIterator<int> iterator1 = container.GetItemQueryIterator<int>(sql);
            FeedResponse<int> feedResponse1 = await iterator1.ReadNextAsync();
			int count = feedResponse1.First();

			WriteLine($"Documents with 'isNew' flag: {count}\n");
            WriteLine("Querying for documents to be updated");

			sql = "SELECT * FROM c WHERE STARTSWITH(c.name, 'New Customer') = true";
            FeedIterator<dynamic> feedIterator2 = container.GetItemQueryIterator<dynamic>(sql);
            FeedResponse<dynamic> feedResponse2 = await feedIterator2.ReadNextAsync();
			List<dynamic> documentsList = feedResponse2.ToList();

			WriteLine($"Found {documentsList.Count} documents to be updated");

			foreach (var document in documentsList)
			{
				document.isNew = true;
				dynamic result = await container.ReplaceItemAsync<dynamic>(document, (string)document.id);
				dynamic updatedDocument = result.Resource;
				WriteLine($"Updated document 'isNew' flag: {updatedDocument.isNew}");
			}

			sql = "SELECT VALUE COUNT(c) FROM c WHERE c.isNew = true";
            FeedIterator<int> feedIterator3 = container.GetItemQueryIterator<int>(sql);
            FeedResponse<int> feedResponse3 = await feedIterator3.ReadNextAsync();
			count = feedResponse3.First();

			WriteLine($"Documents with 'isNew' flag: {count}\n");
		}

		private async Task DeleteDocuments()
		{
			var container = Shared.Client.GetContainer(_databaseName, _containerName);

			const string sql = "SELECT c.id, c.address.postalCode FROM c WHERE STARTSWITH(c.name, 'New Customer') = true";
			FeedIterator<dynamic> iterator = container.GetItemQueryIterator<dynamic>(sql);
            FeedResponse<dynamic> feedResponse = await iterator.ReadNextAsync();
			List<dynamic> documents = feedResponse.ToList();

			WriteLine($"Found {documents.Count} documents to be deleted");

			foreach (var document in documents)
			{
				string id = document.id;
				string pk = document.postalCode;

				await container.DeleteItemAsync<dynamic>(id, new PartitionKey(pk));
			}

			WriteLine($"Deleted {documents.Count} new customer documents\n");
		}
    }
}
