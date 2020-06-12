using System;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using MongoDB.Driver.GridFS;

namespace DuaDOS
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var mongoSettings = MongoClientSettings.FromConnectionString(args[0]);
            var client = new MongoClient(mongoSettings);
            var mongoDatabase = client.GetDatabase("dr-move-public-api");
            var bucket = new GridFSBucket(mongoDatabase, new GridFSBucketOptions
            {
                BucketName = "packages",
            });

            var mongoFilter =
                     Builders<GridFSFileInfo<ObjectId>>.Filter.Lt(x => x.UploadDateTime, DateTime.UtcNow.AddDays(-Math.Abs(int.Parse(args[1]))));

            var options = new GridFSFindOptions
            {
                BatchSize = 50,
            };

            var batchnum = 0;
            var numFiles = 0;

            using (var cursor = await bucket.FindAsync(mongoFilter,options))
            {
                while (await cursor.MoveNextAsync())
                {
                    var batch = cursor.Current;
                    Console.WriteLine($"Deleting from batch {++batchnum}");

                    foreach (var item in batch)
                    {
                        numFiles++;
                      await bucket.DeleteAsync(item.Id);
                    }
                }
            }

            Console.WriteLine($"Deleted {numFiles} number of files");
        }
    }
}

