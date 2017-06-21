using System;
using System.Collections.Generic;
using System.Text;

using Confluent.Kafka;
using Confluent.Kafka.Serialization;

namespace kafka.sub.console
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            var kafkaEndpoint = "192.168.99.100:9092";

            var kafkaTopic = "testtopic";

            Console.WriteLine($"Starting Kafka Consumer Console - {DateTime.Now:G}");

            var consumerConfig =
                new Dictionary<string, object> { { "group.id", "myconsumer" }, { "bootstrap.servers", kafkaEndpoint } };

            using (var consumer =
                new Consumer<Null, string>(consumerConfig, null, new StringDeserializer(Encoding.UTF8)))
            {
                Console.WriteLine($"Consumer Name: {consumer.Name}");
                Console.WriteLine($"Consumer MemberId: {consumer.MemberId}");

                // Subscribe to the OnMessage event
                consumer.OnMessage += (obj, msg) =>
                    {
                        Console.WriteLine($"Received: {msg.Value}");
                    };

                // Subscribe to the Kafka topic
                consumer.Subscribe(new List<string>() { kafkaTopic });

                // Handle Cancel Keypress 
                var canceled = false;
                Console.CancelKeyPress += (_, e) =>
                    {
                        e.Cancel = true; // prevent the process from terminating.
                        canceled = true;
                    };

                Console.WriteLine("Ctrl-C to exit.");

                // Poll for messages
                while (!canceled)
                {
                    consumer.Poll();
                }
            }
        }
    }
}