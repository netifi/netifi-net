using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using RSocket;
using RSocket.Transports;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Com.Netifi.Broker.Info;
using Google.Protobuf.WellKnownTypes;

namespace Netifi.Broker.Tests
{
    [TestClass]
    public class BrokerClientTests
    {
        [TestMethod]
        public async Task BrokerClientTest()
        {
            var accessKey = 9007199254740991;
            var accessToken = "kTBDVtfRBO4tHOnZzSyY5ym2kfY=";
            var tags = new SortedDictionary<string, string> {
                { "key", "value" }
            };
            var transport = new SocketTransport("tcp://localhost:8001/");
            var client = new Client.BrokerClient(accessKey, accessToken, null, null, "group", "destination", 0, tags, transport, RSocketOptions.Default);

            await client.ConnectAsync();

            var group = client.Group("com.netifi.broker.brokerServices", new SortedDictionary<string, string>());
            var brokerInfoService = new BrokerInfoService.BrokerInfoServiceClient(group);
            var stream = brokerInfoService.Brokers(new Empty(), new ReadOnlySequence<byte>());

            var enumerator = stream.GetAsyncEnumerator();
            try
            {
                while (await enumerator.MoveNextAsync()) {
                    Console.WriteLine($"Stream Result: {enumerator.Current.ToString()}");
                }
                Console.WriteLine("Stream Done");
            }
            finally { await enumerator.DisposeAsync(); }
        }
    }
}