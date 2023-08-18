using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Rest;
using Microsoft.Rest.Serialization;
using Microsoft.Azure.Management.ResourceManager;
using Microsoft.Azure.Management.DataFactory;
using Microsoft.Azure.Management.DataFactory.Models;
using Microsoft.Identity.Client;
using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Azure.Identity;
using Azure.Security.KeyVault.Secrets;
using Azure.Core;

namespace HttpsTrigger
{
    public static class ExecutePipeline
    {

        [FunctionName("ExecutePipeline")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post", Route = null)] HttpRequest req,
            ILogger log)
        {
            log.LogInformation("C# HTTP trigger function processed a request.");

            string name = req.Query["name"];

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            name = name ?? data?.name;

            
               
                SecretClientOptions options = new SecretClientOptions()
                {
                    Retry =
                {
                    Delay= TimeSpan.FromSeconds(2),
                    MaxDelay = TimeSpan.FromSeconds(16),
                    MaxRetries = 5,
                    Mode = RetryMode.Exponential
                }
                };
                var secClient = new SecretClient(new Uri("https://gopiasalikeyvault1.vault.azure.net/"), new DefaultAzureCredential(), options);

            KeyVaultSecret usernameSec = secClient.GetSecret("username");
            if (string.Equals(name, usernameSec.Value))
            {

                log.LogInformation("Validated user");
                KeyVaultSecret tenantIDsecret = secClient.GetSecret("tenantID");
                KeyVaultSecret applicationIdsecret = secClient.GetSecret("applicationId");
                KeyVaultSecret authenticationKeysecret = secClient.GetSecret("authenticationKey");
                KeyVaultSecret resourceGroupsecret = secClient.GetSecret("resourceGroup");
                KeyVaultSecret subscriptionIdsecret = secClient.GetSecret("subscriptionId");
                KeyVaultSecret dataFactoryNamesecret = secClient.GetSecret("dataFactoryName");

                string tenantID = tenantIDsecret.Value;
                string applicationId = applicationIdsecret.Value;
                string authenticationKey = authenticationKeysecret.Value;
                string subscriptionId = subscriptionIdsecret.Value;
                string resourceGroup = resourceGroupsecret.Value;
                string region = "eastus";
                string dataFactoryName =
                    dataFactoryNamesecret.Value;

                string pipelineName = "ALLRun";

                //Create a data factory management client
                // Authenticate and create a data factory management client
                log.LogInformation("Creating data factory client");
                IConfidentialClientApplication app = ConfidentialClientApplicationBuilder.Create(applicationId)
                 .WithAuthority("https://login.microsoftonline.com/" + tenantID)
                 .WithClientSecret(authenticationKey)
                 .WithLegacyCacheCompatibility(false)
                 .WithCacheOptions(CacheOptions.EnableSharedCacheOptions)
                 .Build();

                AuthenticationResult result = await app.AcquireTokenForClient(
                  new string[] { "https://management.azure.com//.default" })
                   .ExecuteAsync();
                ServiceClientCredentials cred = new TokenCredentials(result.AccessToken);
                var client = new DataFactoryManagementClient(cred)
                {
                    SubscriptionId = subscriptionId
                };

                Console.WriteLine("Creating pipeline run...");
                Dictionary<string, object> parameters = new Dictionary<string, object>
{
    { "inputPath", "" },
    { "outputPath", "" }
};
                CreateRunResponse runResponse = client.Pipelines.CreateRunWithHttpMessagesAsync(
                    resourceGroup, dataFactoryName, pipelineName, parameters: parameters
                ).Result.Body;
                Console.WriteLine("Pipeline run ID: " + runResponse.RunId);
                return new OkObjectResult("Pipeline triggered successfully");
            }else
            {
                return new OkObjectResult("Invalid trigger");
            }

        }

    }
}
