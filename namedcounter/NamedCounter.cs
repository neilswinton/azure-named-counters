using System.Collections.Generic;
using System.IO;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.WebJobs.Host;
using Newtonsoft.Json;
using System.Threading.Tasks;

using Microsoft.Extensions.Configuration;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;


namespace namedcounter
{
    public static class NamedCounter
    {
        //# static private readonly string storageAccount = "devstoreaccount1";
        // # static private readonly string storageAccountKey = "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

        
        public class CounterEntity : TableEntity
        {
            public CounterEntity(string userId, string name, System.Int64 counter)
            {
                this.PartitionKey = userId;
                this.RowKey = name;
                this.CurrentCount = counter;
            }

            public CounterEntity() { }

            public System.Int64 CurrentCount { get; set; }
        }

        private static CloudTable getTableClient(ExecutionContext context)
        {
            var config = new ConfigurationBuilder()
                .SetBasePath(context.FunctionAppDirectory)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            var storageConnectionString = config.GetConnectionString("TableStorageConnection");


            CloudStorageAccount storageAccount = CloudStorageAccount.Parse(storageConnectionString);
            CloudTableClient tableClient = storageAccount.CreateCloudTableClient();
            CloudTable table = tableClient.GetTableReference("namedCounters");

            return table;
        }

        [FunctionName("Next")]
        public static async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)]HttpRequest req, TraceWriter log, ExecutionContext context)
        {
            log.Info(string.Format("C# HTTP trigger function {0} processed a request.", req.Method));

            string nameParameter = req.Query["name"];
            string countParameter = req.Query["count"];
            string resetParameter = req.Query["reset"];
            string userId = req.Query["code"];

            if (req.Headers.ContainsKey("x-functions-key"))
                userId = req.Headers["x-functions-key"].ToString();

            if (userId == null)
                 userId = "Joe User";


            string requestBody = new StreamReader(req.Body).ReadToEnd();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            nameParameter = nameParameter ?? data?.name;
            countParameter = countParameter ?? data?.count ?? "1";
            resetParameter = resetParameter ?? data?.reset ?? "False";

            if (countParameter == string.Empty)
                countParameter = "1";

            if (resetParameter == string.Empty)
                resetParameter = true.ToString();

            if (!System.Int64.TryParse(countParameter, out System.Int64 counterIncrement) || counterIncrement < 0)
                return new BadRequestObjectResult($"count value must be a positive integer: \"{countParameter}\"");

            if (!bool.TryParse(resetParameter, out bool counterReset))
                return new BadRequestObjectResult($"reset value must be true/false: \"{resetParameter}\"");

            // TODO validate and set reset_counter and counter_increment

            if (nameParameter != null)
            {
                CloudTable table = getTableClient(context);
                bool created = await table.CreateIfNotExistsAsync();
                log.Info(string.Format("C# HTTP trigger function table created: {0}.", created));

                // Create a retrieve operation that takes a customer entity.
                TableOperation tableOperation = TableOperation.Retrieve(userId, nameParameter);
                // Execute the retrieve operation.
                TableResult retrievedResult = await table.ExecuteAsync(tableOperation);


                // TODO: Loop on concurrancy errors
                CounterEntity counterEntity;


                if (retrievedResult.Result == null)
                {
                    // First time -- do an insert.  Fails on a concurrency error
                    log.Verbose(string.Format("No record found for key {0}", nameParameter));
                    counterEntity = new CounterEntity(userId, nameParameter, counterIncrement);
                    tableOperation = TableOperation.Insert(counterEntity, true);
                }
                else
                {
                    log.Verbose(string.Format("Record found for key {0}", nameParameter));
                    DynamicTableEntity namedCounter = (DynamicTableEntity)retrievedResult.Result;
                    namedCounter.Properties.TryGetValue("CurrentCount", out EntityProperty counterProperty);
                    System.Int64 updatedCount = counterReset ? 0 : (System.Int64) counterProperty.Int64Value;
                    updatedCount += counterIncrement;
                    counterEntity = new CounterEntity(userId, nameParameter, updatedCount)
                    {
                        ETag = retrievedResult.Etag
                    };
                    tableOperation = TableOperation.InsertOrReplace(counterEntity);
                }
                TableResult tableResult = await table.ExecuteAsync(tableOperation);
                log.Info("TableResult");
                return (ActionResult)new OkObjectResult($"{counterEntity.CurrentCount}");
            }
            return nameParameter != null
                ? (ActionResult)new OkObjectResult($"Hello, {nameParameter}")
                : new BadRequestObjectResult("Please pass a name on the query string or in the request body");
        }
    }
}
