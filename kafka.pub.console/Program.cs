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

            string kafkaTopic = "testtopic3";

            var producerConfig =
                new Dictionary<string, object>
                    {
                        { "bootstrap.servers", kafkaEndpoint },
                        { "queue.buffering.max.ms", "10000" },
                        { "socket.blocking.max.ms", "10000" },
                        { "socket.nagle.disable", "true" },
                        //{ "debug", "protocol" },
                        { "log.connection.close", "false" }
                    };

            Console.WriteLine($"Starting Kafka Producer Console - {DateTime.Now:G}");

            using (var producer = new Producer<string, string>(producerConfig, new StringSerializer(Encoding.UTF8), new StringSerializer(Encoding.UTF8)))
            {
                Console.WriteLine($"{producer.Name} producing on \"{kafkaTopic}\". q to exit.");

                string text = Console.ReadLine();
                while (text != "q")
                {
                    var deliveryReport = producer.ProduceAsync(kafkaTopic, string.Empty, text).GetAwaiter().GetResult();
                    Console.WriteLine($"Partition: {deliveryReport.Partition}, Offset: {deliveryReport.Offset}");

                    text = Console.ReadLine();
                }
            }
            Console.WriteLine($"Ended Kafka Producer Console - {DateTime.Now:G}");
        }
    }
}
