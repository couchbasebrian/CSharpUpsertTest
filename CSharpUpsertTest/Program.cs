using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Couchbase;
using Couchbase.Configuration.Client;

// CSharpUpsertTest
// Brian Williams
// February 5, 2016

// In the properties for CSharpUpsertTest in the Solution Explorer,
// be sure to set Target Framework to .Net Framework 4.5
// Also in the NuGet Package Manager Console, please be sure to do
// Install-Package CouchbaseNetClient
// The version used for this project was 
// Package 'CouchbaseNetClient.2.2.5' 

namespace CSharpUpsertTest
{
    class Program
    {

        static int successCounter = 0;
        static int failCounter    = 0;
        static string largeDocumentBody = "{ \"data\" : \"";
        static string largeDocumentTemp = "0123456789";
        static Random rnd;
        static bool keepGoing = true;

        static void Main(string[] args)
        {
            // Force Hard OOM on this bucket ( assuming 100 MB bucket quota )
            // create large document - about 1,310,735 bytes
            for (int i = 0; i < 17; i++)
            {
                largeDocumentTemp = largeDocumentTemp + largeDocumentTemp;
            }

            largeDocumentBody = largeDocumentBody + largeDocumentTemp + "\" }";

            Console.WriteLine(largeDocumentBody);
            Console.WriteLine("Done with body creation.  Size = " + largeDocumentBody.Length + "Press ENTER to continue.");
            Console.Read();

            rnd = new Random();

            // Replace your cluster's host name here
            string myurl = "http://10.4.2.121:8091";
            Console.WriteLine("About to init: " + myurl);
            ClusterHelper.Initialize(new ClientConfiguration
            {
                Servers = new List<Uri>
                {
                    new Uri(myurl)
                }
            });
            Console.WriteLine("About to do operations");


            while (keepGoing) {
                upsertAndCheck();
            }

            Console.WriteLine("Done.  Press ENTER to continue.");
            Console.Read();
            ClusterHelper.Close();
        }

        private static async void upsertAndCheck()
        {
            
            var bucket = ClusterHelper.GetBucket("BUCKETNAME");
            String randomKey = "randomkey" + rnd.Next(1, 100);

            TimeSpan myTimeSpan = new TimeSpan(0, 0, 0, 30);
            var result = bucket.Upsert(randomKey, largeDocumentBody, myTimeSpan);

            var myStatus = result.Status;
            string myMessage = result.Message;
            var myException = result.Exception;

            if (result.Success)
            {
                //Thread.Sleep(1000);
                successCounter++;

                // result = bucket.GetAndTouch<string>("foo", new TimeSpan(0, 0, 0, 3));
                // Console.WriteLine(result.Success);
            } else
            {
                Console.WriteLine("Status: " + myStatus);
                Console.WriteLine("Message:   " + myMessage);
                Console.WriteLine("Exception: " + myException);
                failCounter++;
            }

            String res1 = "Key: " + randomKey + " Success:" + successCounter + " Failures: " + failCounter;

            Console.WriteLine(res1);
        }
    }
}

// EOF
