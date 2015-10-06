using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using CommandLine;
using CommandLine.Parsing;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.Documents.Linq;
using Microsoft.Azure.Documents.Partitioning;
using Newtonsoft;
using Newtonsoft.Json;

namespace DocumentDBRepartition
{
    class Program
    {
        static int Main(string[] args)
        {
            Options options = new Options();
            if (!CommandLine.Parser.Default.ParseArguments(args, options))
            {
                Console.WriteLine("Invalid arguments");
                return 1;
            }

            using (DocumentClient client = new DocumentClient(
                new Uri(options.Endpoint), 
                options.AuthKey, 
                new ConnectionPolicy { ConnectionMode = ConnectionMode.Direct, ConnectionProtocol = Protocol.Tcp }))
            {
                Database database = client.CreateDatabaseQuery().Where(d => d.Id == options.Database).AsEnumerable().FirstOrDefault();
                if (database == null)
                {
                    Console.WriteLine("Cannot find database " + options.Database);
                    return 2;
                }
                
                List<DocumentCollection> collections = client.ReadDocumentCollectionFeedAsync(database.SelfLink).Result.ToList();
                int minimumRequiredCollections = Math.Max(options.NewCollections, options.CurrentCollections);

                if (collections.Count < minimumRequiredCollections)
                {
                    Console.WriteLine("At least {0} collections must be pre-created", minimumRequiredCollections);
                    return 3;
                }

                Console.WriteLine("Current distribution of documents across collections:");
                LogDocumentCountsPerCollection(client, database).Wait();
                Console.WriteLine();

                HashPartitionResolver currentPartitionResolver = new HashPartitionResolver(options.PartitionKeyName, collections.Take(options.CurrentCollections).Select(c => c.SelfLink));
                HashPartitionResolver nextPartitionResolver = new HashPartitionResolver(options.PartitionKeyName, collections.Take(options.NewCollections).Select(c => c.SelfLink));

                int numberOfMovedDocuments = 0;

                Parallel.ForEach(currentPartitionResolver.CollectionLinks, collectionLink =>                 
                {
                    ResourceFeedReader<Document> feedReader = client.CreateDocumentFeedReader(collectionLink, new FeedOptions { MaxItemCount = -1 });

                    while (feedReader.HasMoreResults)
                    {
                        foreach (Document document in DocumentClientHelper.ExecuteWithRetryAsync<FeedResponse<Document>>(() => feedReader.ExecuteNextAsync()).Result)
                        {
                            object partitionKey = nextPartitionResolver.GetPartitionKey(document);
                            string newCollectionLink = nextPartitionResolver.ResolveForCreate(partitionKey);

                            if (newCollectionLink != collectionLink)
                            {
                                int count = Interlocked.Increment(ref numberOfMovedDocuments);
                                DocumentClientHelper.ExecuteWithRetryAsync(() => client.DeleteDocumentAsync(document.SelfLink)).Wait();
                                DocumentClientHelper.ExecuteWithRetryAsync(() => client.CreateDocumentAsync(newCollectionLink, document)).Wait();

                                if (count % 100 == 0)
                                {
                                    Console.WriteLine("Moved {0} documents between partitions", numberOfMovedDocuments);
                                }
                            }
                        }
                    }
                });


                Console.WriteLine();
                Console.WriteLine("Moved {0} documents between partitions.", numberOfMovedDocuments);
                Console.WriteLine();

                Console.WriteLine("Current distribution of documents across collections:");
                LogDocumentCountsPerCollection(client, database).Wait();
                Console.WriteLine();           
            }

            return 0;
        }

        private static async Task LogDocumentCountsPerCollection(DocumentClient client, Database database)
        {
            foreach (DocumentCollection collection in await client.ReadDocumentCollectionFeedAsync(database.SelfLink))
            {
                int numDocuments = 0;
                foreach (int document in client.CreateDocumentQuery<int>(collection.SelfLink, "SELECT VALUE 1 FROM ROOT", new FeedOptions { MaxItemCount = -1 }))
                {
                    numDocuments++;
                }

                Console.WriteLine("Collection {0}: {1} documents", collection.Id, numDocuments);
            }
        }


        class Options
        {
            [Option('e', "endpoint", Required=true, HelpText = "The DocumentDB endpoint.")]
            public string Endpoint { get; set; }

            [Option('k', "authKey", Required = true, HelpText = "The DocumentDB auth key.")]
            public string AuthKey { get; set; }

            [Option('d', "database", Required = true, HelpText = "The DocumentDB database.")]
            public string Database { get; set; }

            [Option('p', "partitionKeyName", Required = false, DefaultValue="id", HelpText = "The partition key name.")]
            public string PartitionKeyName { get; set; }

            [Option('c', "currentCollectionCount", Required = true, HelpText = "The current number of collections.")]
            public int CurrentCollections { get; set; }

            [Option('n', "newCollectionCount", Required = true, HelpText = "The new number of collections.")]
            public int NewCollections { get; set; }
        }
    }
}
