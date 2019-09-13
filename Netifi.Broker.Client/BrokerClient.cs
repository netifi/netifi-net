using System;
using System.Buffers;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using RSocket;
using RSocket.RPC;

namespace Netifi.Broker.Client
{
    public class BrokerClient
    {
        private static readonly string DEFAULT_DESTINATION = Guid.NewGuid().ToString();

        private readonly long accessKey;
        private readonly byte[] accessToken;
        private readonly Guid connectionId;
        private readonly IPAddress ipAddress;
        private readonly string group;
        private readonly string destination;
        private readonly short additionalFlags;
        private readonly SortedDictionary<string, string> tags;
        private readonly IRSocketTransport transport;
        private readonly RSocketOptions options;
        private readonly RSocketClient client;

        public BrokerClient(
            long accessKey,
            string accessToken,
            Guid? connectionId,
            IPAddress ipAddress,
            string group,
            string destination,
            short additionalFlags,
            SortedDictionary<string, string> tags,
            IRSocketTransport transport,
            RSocketOptions options)
        {
            this.accessKey = accessKey;
            this.accessToken = Convert.FromBase64String(accessToken);
            this.connectionId = connectionId.GetValueOrDefault(Guid.NewGuid());
            this.ipAddress = ipAddress;
            this.group = group;
            this.destination = destination ?? DEFAULT_DESTINATION;
            this.additionalFlags = additionalFlags;
            this.tags = tags;
            this.tags.Add("com.netifi.destination", this.destination);
            this.transport = transport;
            this.options = options;
            this.client = new RSocketClient(transport, options);
        }

        private byte[] writeSetupMetadata()
        {
            var setup = new ArrayBufferWriter<byte>();
            var writer = new BufferWriter(setup, null);
            new Frames.DestinationSetup(ipAddress, group, accessKey, new ReadOnlySequence<byte>(accessToken), connectionId, additionalFlags, tags).Write(writer);
            writer.Flush();

            return setup.WrittenMemory.ToArray();
        }

        public async Task ConnectAsync()
        {
            await client.ConnectAsync(options, metadata: writeSetupMetadata());
        }

        public void AddService(IRSocketService service)
        {
            RSocketService.Register(client, service, metadata => new RSocketService.RemoteProcedureCallMetadata(FramesUtility.UnwrapMetadata(new SequenceReader<byte>(metadata))));
        }

        public RSocket.RSocket Group(string group, SortedDictionary<string, string> tags = default)
        {
            return new BrokerSocket(client, metadata =>
            {
                var bytes = new ArrayBufferWriter<byte>();
                var writer = new BufferWriter(bytes, null);
                new Frames.Group(group, metadata, tags).Write(writer);
                writer.Flush();

                return new ReadOnlySequence<byte>(bytes.WrittenMemory);
            });
        }

        public RSocket.RSocket Broadcast(string group, SortedDictionary<string, string> tags = default)
        {
            return new BrokerSocket(client, metadata =>
            {
                var bytes = new ArrayBufferWriter<byte>();
                var writer = new BufferWriter(bytes, null);
                new Frames.Broadcast(group, metadata, tags).Write(writer);
                writer.Flush();

                return new ReadOnlySequence<byte>(bytes.WrittenMemory);
            });
        }

        public RSocket.RSocket Shard(string group, ReadOnlySequence<byte> shardKey, SortedDictionary<string, string> tags = default)
        {
            return new BrokerSocket(client, metadata =>
            {
                var bytes = new ArrayBufferWriter<byte>();
                var writer = new BufferWriter(bytes, null);
                new Frames.Shard(group, metadata, shardKey, tags).Write(writer);
                writer.Flush();

                return new ReadOnlySequence<byte>(bytes.WrittenMemory);
            });
        }
    }
}
