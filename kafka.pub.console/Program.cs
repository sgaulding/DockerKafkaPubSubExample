using System;
using System.Collections.Generic;
using System.Text;

using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace kafka.pub.console
{
    class Program
    {
        static void Main(string[] args)
        {
            string kafkaEndpoint = "192.168.99.100:9092";

            string kafkaTopic = "testtopic";

            var producerConfig = new Dictionary<string, object> {{"bootstrap.servers", kafkaEndpoint}};

            Console.WriteLine($"Starting Kafka Producer Console - {DateTime.Now:G}");

            using (var producer = new Producer<Null, string>(producerConfig, null, new StringSerializer(Encoding.UTF8)))
            {
                Console.WriteLine($"{producer.Name} producing on \"{kafkaTopic}\". q to exit.");

                string text;
                while ((text = Console.ReadLine()) != "q")
                {
                    var deliveryReport = producer.ProduceAsync(kafkaTopic, null, text);
                    deliveryReport.ContinueWith(task =>
                        {
                            Console.WriteLine($"Partition: {task.Result.Partition}, Offset: {task.Result.Offset}");
                        });
                }

                // Tasks are not waited on synchronously (ContinueWith is not synchronous),
                // so it's possible they may still in progress here.
                producer.Flush(TimeSpan.FromSeconds(10).Milliseconds);
            }
            Console.WriteLine($"Ended Kafka Producer Console - {DateTime.Now:G}");
        }
    }
}
