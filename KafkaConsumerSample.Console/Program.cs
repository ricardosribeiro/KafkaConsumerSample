using Confluent.Kafka;
using System;
using System.Threading;

namespace KafkaConsumerSample.Console
{
    class Program
    {
        static void Main(string[] args)
        {
            var config = new ConsumerConfig()
            {
                BootstrapServers = "omnibus-01.srvs.cloudkafka.com:9094,omnibus-02.srvs.cloudkafka.com:9094,omnibus-03.srvs.cloudkafka.com:9094",
                SaslUsername = "p5yt75io",
                SaslPassword = "UtSNvphLpUW01YR_tPTWgC_SQT2G7xUd",
                SaslMechanism = SaslMechanism.ScramSha256,
                SecurityProtocol = SecurityProtocol.SaslSsl,
                EnableSslCertificateVerification = false,
                GroupId = "test-consumer-group",
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                consumer.Subscribe("p5yt75io-test");

                var cts = new CancellationTokenSource();

                System.Console.CancelKeyPress += (_, e) =>
                {
                    e.Cancel = true;
                    cts.Cancel();
                };

                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = consumer.Consume(cts.Token);
                            System.Console.WriteLine($"Consumed message: {cr.Message} at: {cr.TopicPartitionOffset}");
                        }
                        catch (ConsumeException)
                        {
                            System.Console.WriteLine("Consumed fail");
                            throw;
                        }
                    }
                }
                catch (Exception)
                {

                    throw;
                }
            }

        }
    }
}
