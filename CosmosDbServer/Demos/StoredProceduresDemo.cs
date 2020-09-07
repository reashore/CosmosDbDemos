using Microsoft.Azure.Cosmos;
using Microsoft.Azure.Cosmos.Scripts;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;

namespace CosmosDb.ServerSide.Demos
{
	public static class StoredProceduresDemo
	{
		public static async Task Run()
		{
			Debugger.Break();

			await CreateStoredProcedures();

			await ViewStoredProcedures();

			await ExecuteStoredProcedures();

			await DeleteStoredProcedures();
		}

		private static async Task CreateStoredProcedures()
		{
			Console.WriteLine("\n>>> Create Stored Procedures <<<\n");

			await CreateStoredProcedure("spHelloWorld");
			await CreateStoredProcedure("spSetNorthAmerica");
			await CreateStoredProcedure("spGenerateId");
			await CreateStoredProcedure("spBulkInsert");
			await CreateStoredProcedure("spBulkDelete");
		}

		private static async Task CreateStoredProcedure(string sprocId)
		{
			var sprocBody = await File.ReadAllTextAsync($@"Server\{sprocId}.js");

			var sprocProps = new StoredProcedureProperties
			{
				Id = sprocId,
				Body = sprocBody
			};

			var container = Shared.Client.GetContainer("mydb", "mystore");
			var result = await container.Scripts.CreateStoredProcedureAsync(sprocProps);

			Console.WriteLine($"Created stored procedure {sprocId} ({result.RequestCharge} RUs);");
		}

		private static async Task ViewStoredProcedures()
		{
			Console.WriteLine("\n>>> View Stored Procedures <<<\n");

			var container = Shared.Client.GetContainer("mydb", "mystore");

			FeedIterator<StoredProcedureProperties> iterator = container.Scripts.GetStoredProcedureQueryIterator<StoredProcedureProperties>();
			FeedResponse<StoredProcedureProperties> sprocs = await iterator.ReadNextAsync();
            var count = 0;

			foreach (var sproc in sprocs)
			{
				count++;
				Console.WriteLine($" Stored procedure Id: {sproc.Id}; Modified: {sproc.LastModified}");
			}

			Console.WriteLine($"\nTotal stored procedures: {count}");
		}

		private static async Task ExecuteStoredProcedures()
		{
			Console.Clear();
			await Execute_spHelloWorld();

			Console.Clear();
			await Execute_spSetNorthAmerica1();
			await Execute_spSetNorthAmerica2();
			await Execute_spSetNorthAmerica3();

			Console.Clear();
			await Execute_spGenerateId();

			Console.Clear();
			await Execute_spBulkInsert();

			Console.Clear();
			await Execute_spBulkDelete();
		}

		private static async Task Execute_spHelloWorld()
		{
			Console.WriteLine("\nExecute spHelloWorld stored procedure");

			Scripts scripts = Shared.Client.GetContainer("mydb", "mystore").Scripts;
			PartitionKey partitionKey = new PartitionKey(string.Empty);
			StoredProcedureExecuteResponse<string> result = await scripts.ExecuteStoredProcedureAsync<string>("spHelloWorld", partitionKey, null);
			string message = result.Resource;

			Console.WriteLine($"Result: {message}");
		}

		private static async Task Execute_spSetNorthAmerica1()
		{
			Console.WriteLine("\nExecute spSetNorthAmerica (country = United States)\n");

			// Should succeed with isNorthAmerica = true
			var id = Guid.NewGuid().ToString();
			dynamic documentDefinition = new
			{
				id,
				name = "John Doe",
				address = new
				{
					countryRegionName = "United States",
					postalCode = "12345"
				}
			};

			Container container = Shared.Client.GetContainer("mydb", "mystore");
			PartitionKey partitionKey = new PartitionKey(documentDefinition.address.postalCode);
			StoredProcedureExecuteResponse<dynamic> result = await container.Scripts.ExecuteStoredProcedureAsync<dynamic>("spSetNorthAmerica", partitionKey, new[] { documentDefinition, true });
			var document = result.Resource;

			var country = document.address.countryRegionName;
			var isNorthAmerica = document.address.isNorthAmerica;

			Console.WriteLine("Result:");
			Console.WriteLine($" Country = {country}");
			Console.WriteLine($" Is North America = {isNorthAmerica}");

			await container.DeleteItemAsync<dynamic>(id, partitionKey);
		}

		private static async Task Execute_spSetNorthAmerica2()
		{
			Console.WriteLine();
			Console.WriteLine("Execute spSetNorthAmerica (country = United Kingdom)");

			// Should succeed with isNorthAmerica = false
			var id = Guid.NewGuid().ToString();
			dynamic documentDefinition = new
			{
				id,
				name = "John Doe",
				address = new
				{
					countryRegionName = "United Kingdom",
					postalCode = "RG41 1QW"
				}
			};

			var container = Shared.Client.GetContainer("mydb", "mystore");
			var pk = new PartitionKey(documentDefinition.address.postalCode);
			StoredProcedureExecuteResponse<dynamic> result = await container.Scripts.ExecuteStoredProcedureAsync<dynamic>("spSetNorthAmerica", pk, new[] { documentDefinition, true });
			var document = result.Resource;

			// Deserialize new document as JObject (use dictionary-style indexers to access dynamic properties)
			var documentObject = JsonConvert.DeserializeObject(document.ToString());

			var country = documentObject["address"]["countryRegionName"];
			var isNorthAmerica = documentObject["address"]["isNorthAmerica"];

			Console.WriteLine("Result:");
			Console.WriteLine($" Country = {country}");
			Console.WriteLine($" Is North America = {isNorthAmerica}");

			await container.DeleteItemAsync<dynamic>(id, pk);
		}

		private static async Task Execute_spSetNorthAmerica3()
		{
			Console.WriteLine();
			Console.WriteLine("Execute spSetNorthAmerica (no country)");

			var id = Guid.NewGuid().ToString();
			dynamic documentDefinition = new
			{
				id,
				name = "James Smith",
				address = new
				{
					postalCode = "12345"
				}
			};

			var container = Shared.Client.GetContainer("mydb", "mystore");
			var pk = new PartitionKey(documentDefinition.address.postalCode);

			try
			{
				// Should fail with no country and enforceSchema = true
				StoredProcedureExecuteResponse<dynamic> result = await container.Scripts.ExecuteStoredProcedureAsync<dynamic>("spSetNorthAmerica", pk, new[] { documentDefinition, true });
			}
			catch (CosmosException exception)
			{
				Console.WriteLine($"Error: {exception.Message}");
			}
		}

		private static async Task Execute_spGenerateId()
		{
			Console.WriteLine();
			Console.WriteLine("Execute spGenerateId");

			dynamic docDef1 = new { firstName = "Albert", lastName = "Einstein", address = new { postalCode = "12345" } };
			dynamic docDef2 = new { firstName = "Alfred", lastName = "Einstein", address = new { postalCode = "12345" } };
			dynamic docDef3 = new { firstName = "Ashton", lastName = "Einstein", address = new { postalCode = "12345" } };
			dynamic docDef4 = new { firstName = "Albert", lastName = "Einstein", address = new { postalCode = "54321" } };

			var container = Shared.Client.GetContainer("mydb", "mystore");
			var pk12345 = new PartitionKey("12345");
			var pk54321 = new PartitionKey("54321");

			StoredProcedureExecuteResponse<dynamic> result1 = await container.Scripts.ExecuteStoredProcedureAsync<dynamic>("spGenerateId", pk12345, new[] { docDef1 });
			var doc1 = result1.Resource;
			Console.WriteLine($"New document in PK '{doc1.address.postalCode}', generated ID '{doc1.id}' for '{doc1.firstName} {doc1.lastName}'");

			StoredProcedureExecuteResponse<dynamic> result2 = await container.Scripts.ExecuteStoredProcedureAsync<dynamic>("spGenerateId", pk12345, new[] { docDef2 });
			var doc2 = result2.Resource;
			Console.WriteLine($"New document in PK '{doc2.address.postalCode}', generated ID '{doc2.id}' for '{doc2.firstName} {doc2.lastName}'");

			StoredProcedureExecuteResponse<dynamic> result3 = await container.Scripts.ExecuteStoredProcedureAsync<dynamic>("spGenerateId", pk12345, new[] { docDef3 });
			var doc3 = result3.Resource;
			Console.WriteLine($"New document in PK '{doc3.address.postalCode}', generated ID '{doc3.id}' for '{doc3.firstName} {doc3.lastName}'");

			StoredProcedureExecuteResponse<dynamic> result4 = await container.Scripts.ExecuteStoredProcedureAsync<dynamic>("spGenerateId", pk54321, new[] { docDef4 });
			var doc4 = result4.Resource;
			Console.WriteLine($"New document in PK '{doc4.address.postalCode}', generated ID '{doc4.id}' for '{doc4.firstName} {doc4.lastName}'");

			await container.DeleteItemAsync<dynamic>(doc1.id.ToString(), pk12345);
			await container.DeleteItemAsync<dynamic>(doc2.id.ToString(), pk12345);
			await container.DeleteItemAsync<dynamic>(doc3.id.ToString(), pk12345);
			await container.DeleteItemAsync<dynamic>(doc4.id.ToString(), pk54321);
		}

		private static async Task Execute_spBulkInsert()
		{
			Console.WriteLine();
			Console.WriteLine("Execute spBulkInsert");

			List<dynamic> docs = new List<dynamic>();
			const int total = 5000;
			for (var i = 1; i <= total; i++)
			{
				dynamic doc = new
				{
					name = $"Bulk inserted doc {i}",
					address = new
					{
						postalCode = "12345"
					}
				};
				docs.Add(doc);
			}

			var container = Shared.Client.GetContainer("mydb", "mystore");
			var pk = new PartitionKey("12345");
			var totalInserted = 0;
			while (totalInserted < total)
			{
				StoredProcedureExecuteResponse<int> result = await container.Scripts.ExecuteStoredProcedureAsync<int>("spBulkInsert", pk, new[] { docs });
				var inserted = result.Resource;
				totalInserted += inserted;
				var remaining = total - totalInserted;
				Console.WriteLine($"Inserted {inserted} documents ({totalInserted} total, {remaining} remaining)");
				docs = docs.GetRange(inserted, docs.Count - inserted);
			}
			Console.WriteLine();
		}

		private static async Task Execute_spBulkDelete()
		{
			Console.WriteLine();
			Console.WriteLine("Execute spBulkDelete");

			// query retrieves self-links for documents to bulk-delete
			const string whereClause = "STARTSWITH(c.name, 'Bulk inserted doc ') = true";
			var count = await Execute_spBulkDelete(whereClause);

			Console.WriteLine($"Deleted bulk inserted documents; count: {count}");
			Console.WriteLine();
		}

		private static async Task<int> Execute_spBulkDelete(string sql)
		{
			var container = Shared.Client.GetContainer("mydb", "mystore");
			var pk = new PartitionKey("12345");
			var continuationFlag = true;
			var totalDeleted = 0;
			while (continuationFlag)
			{
				StoredProcedureExecuteResponse<BulkDeleteResponse> result = await container.Scripts.ExecuteStoredProcedureAsync<BulkDeleteResponse>("spBulkDelete", pk, new[] { sql });
				var response = result.Resource;
				continuationFlag = response.ContinuationFlag;
				var deleted = response.Count;
				totalDeleted += deleted;
				Console.WriteLine($"Deleted {deleted} documents ({totalDeleted} total, more: {continuationFlag})");
			}
			Console.WriteLine();

			return totalDeleted;
		}

		// Delete stored procedures

		private static async Task DeleteStoredProcedures()
		{
			Console.WriteLine();
			Console.WriteLine(">>> Delete Stored Procedures <<<");
			Console.WriteLine();

			await DeleteStoredProcedure("spHelloWorld");
			await DeleteStoredProcedure("spSetNorthAmerica");
			await DeleteStoredProcedure("spGenerateId");
			await DeleteStoredProcedure("spBulkInsert");
			await DeleteStoredProcedure("spBulkDelete");
		}

		private static async Task DeleteStoredProcedure(string sprocId)
		{
			var container = Shared.Client.GetContainer("mydb", "mystore");
			await container.Scripts.DeleteStoredProcedureAsync(sprocId);

			Console.WriteLine($"Deleted stored procedure: {sprocId}");
		}

	}

	public class BulkDeleteResponse
	{
		[JsonProperty(PropertyName = "count")]
		public int Count { get; set; }

		[JsonProperty(PropertyName = "continuationFlag")]
		public bool ContinuationFlag { get; set; }
	}

}
