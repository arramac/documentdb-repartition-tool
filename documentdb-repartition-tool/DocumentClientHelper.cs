using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using System;
using System.Threading.Tasks;

namespace DocumentDBRepartition
{
    internal static class DocumentClientHelper
    {
        private static readonly int StatusCodeTooManyRequests = 429;
        private static readonly int MaxRetryCount = 100;
        private static TimeSpan MinSleepTime = TimeSpan.FromMilliseconds(500);

        /// <summary>
        /// Execute the function with retries on throttle.
        /// </summary>
        /// <typeparam name="T">The type of return value from the execution.</typeparam>
        /// <param name="function">The function to execute.</param>
        /// <returns>The response from the execution.</returns>
        /// <summary>
        /// Execute the function with retries on throttle.
        /// </summary>
        /// <typeparam name="T">The type of return value from the execution.</typeparam>
        /// <param name="function">The function to execute.</param>
        /// <returns>The response from the execution.</returns>
        public static async Task<T> ExecuteWithRetryAsync<T>(Func<Task<T>> action)
        {
            int retryCount = 0;
            TimeSpan delayInterval = TimeSpan.Zero;

            while (true)
            {
                try
                {
                    return await action();
                }
                catch (Exception ex)
                {
                    retryCount++;
                    if (retryCount > MaxRetryCount)
                    {
                        throw;
                    }

                    while (ex is AggregateException) { ex = ((AggregateException)ex).InnerException; }

                    if (ex is DocumentClientException)
                    {
                        DocumentClientException de = (DocumentClientException)ex;
                        if ((int)de.StatusCode == StatusCodeTooManyRequests)
                        {
                            delayInterval = ((DocumentClientException)ex).RetryAfter;
                        }
                        else
                        {
                            throw;
                        }
                    }
                    else
                    {
                        throw;
                    }
                }

                await Task.Delay(delayInterval < MinSleepTime ? MinSleepTime : delayInterval);
            }
        }

        public static async Task<T> GetDocument<T>(DocumentClient client, string documentLink)
        {
            T document;
            try
            {
                var response = await ExecuteWithRetryAsync(() => client.ReadDocumentAsync(documentLink));
                document = (T)(dynamic)response.Resource;
            }
            catch (Exception e)
            {
                DocumentClientException de;
                if (e is DocumentClientException)
                {
                    de = (DocumentClientException)e;
                }
                else if (e is AggregateException)
                {
                    AggregateException ae = (AggregateException)e;
                    if (ae.InnerException is DocumentClientException)
                    {
                        de = (DocumentClientException)ae.InnerException;
                    }
                    else
                    {
                        throw;
                    }
                }
                else
                {
                    throw;
                }

                if (de.StatusCode == System.Net.HttpStatusCode.NotFound)
                {
                    document = default(T);
                }
                else
                {
                    throw;
                }
            }

            return document;
        }

        public static async Task CreateDocument<T>(DocumentClient client, string collectionLink, T document)
        {
            var response = await ExecuteWithRetryAsync(() => client.CreateDocumentAsync(collectionLink, document));
        }
    }
}