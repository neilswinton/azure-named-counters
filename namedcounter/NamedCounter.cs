/**
Copyright(c) 2018 Pine Knoll Consulting, LLC

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
**/

using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Azure.WebJobs.Host;

using System.Threading.Tasks;

using Microsoft.Extensions.Configuration;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;


namespace namedcounter
{
    public static class NamedCounter
    {
        public class CounterEntity : TableEntity
        {
            public static readonly string tableName = "namedCounters";

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
            CloudTable table = tableClient.GetTableReference(CounterEntity.tableName);

            return table;
        }

        [FunctionName("Next")]
        public static async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Function, "get", Route = "Next/{nameParameter}")]HttpRequest req, 
                                                    string nameParameter, TraceWriter log, ExecutionContext context)
        {
            log.Verbose("C# HTTP trigger function processed a request.");
            string countParameter = req.Query["count"];
            string resetParameter = req.Query["reset"];
            string userId = req.Query["code"];

            // User ID from code query param of x-functions-key header
            if (userId == null)
                userId = req.Headers["x-functions-key"].ToString();
            if (userId == string.Empty)
                userId = "Development";

            // Default count to an increment of one
            if (countParameter == null)
                countParameter = "1";

            // Allow both ?reset=True and ?reset to reset counter
            if (resetParameter == null)
                resetParameter = bool.FalseString;
            else if (resetParameter == string.Empty)
                resetParameter = bool.TrueString;

            // Bail early on bad inputs
            if (nameParameter == null)
                return new BadRequestObjectResult("The name parameter must be present");

            if (!System.Int64.TryParse(countParameter, out System.Int64 counterIncrement) || counterIncrement < 0)
                return new BadRequestObjectResult($"count value must be a positive integer: \"{countParameter}\"");

            if (!bool.TryParse(resetParameter, out bool counterReset))
                return new BadRequestObjectResult($"reset value must be true/false: \"{resetParameter}\"");

            // Normalize inputs
            nameParameter = nameParameter.ToLower();
            userId = userId.ToLower();


            // Get a reference to the table and create it as needed
            CloudTable table = getTableClient(context);
            if (await table.CreateIfNotExistsAsync())
                log.Info($"Created table {CounterEntity.tableName}");

            // Read the current counter record if it exists
            TableOperation tableOperation = TableOperation.Retrieve(userId, nameParameter);
            TableResult retrievedResult = await table.ExecuteAsync(tableOperation);

            // TODO: Loop on concurrancy errors
            CounterEntity counterEntity;
            string operationMessage;

            if (retrievedResult.Result == null)
            {
                // First time -- do an insert.  If another process inserts first, this fails (appropriately)
                operationMessage = $"Creating new entry for {userId}:{nameParameter} = {counterIncrement}";
                counterEntity = new CounterEntity(userId, nameParameter, counterIncrement);
                tableOperation = TableOperation.Insert(counterEntity, true);
            }
            else
            {
                // Update the current value
                DynamicTableEntity namedCounter = (DynamicTableEntity)retrievedResult.Result;
                namedCounter.Properties.TryGetValue("CurrentCount", out EntityProperty counterProperty);
                System.Int64 updatedCount = counterReset ? 0 : (System.Int64)counterProperty.Int64Value;
                updatedCount += counterIncrement;
                operationMessage = $"Updating {userId}:{nameParameter} = {counterIncrement}";
                counterEntity = new CounterEntity(userId, nameParameter, updatedCount)
                {
                    ETag = retrievedResult.Etag
                };
                tableOperation = TableOperation.Replace(counterEntity);
            }

            // Execute the table operation -- Insert or Replace
            TableResult tableResult = await table.ExecuteAsync(tableOperation);
            log.Info($"Status: {tableResult.HttpStatusCode}.  {operationMessage}");
            if (tableResult.HttpStatusCode < 300)
                return (ActionResult)new OkObjectResult($"{counterEntity.CurrentCount}");
            else
                return (ActionResult)new StatusCodeResult(tableResult.HttpStatusCode);
        }
    }
}
