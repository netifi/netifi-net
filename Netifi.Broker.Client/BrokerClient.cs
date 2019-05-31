using System;
using System.Buffers;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using RSocket;

namespace Netifi.Broker.Client
{
    public class BrokerClient : RSocketClient
    {
        private readonly long AccessKey;
        private readonly ReadOnlySequence<byte> AccessToken;
        private readonly Guid ConnectionId;
        private readonly IPAddress IpAddress;
        private readonly string Group;
        private readonly string Destination;
        private readonly short AdditionalFlags;
        private readonly SortedDictionary<string, string> Tags;
        private readonly RSocketOptions Options;

        public BrokerClient(
            long AccessKey,
            ReadOnlySequence<byte> AccessToken,
            Guid? ConnectionId,
            IPAddress IpAddress,
            string Group,
            string Destination,
            short AdditionalFlags,
            SortedDictionary<string, string> Tags,
            RSocketOptions Options,
            IRSocketTransport Transport) : base(Transport, Options)
        {
            this.AccessKey = AccessKey;
            this.AccessToken = AccessToken;
            this.ConnectionId = ConnectionId.GetValueOrDefault(Guid.NewGuid());
            this.IpAddress = IpAddress;
            this.Group = Group;
            this.Destination = Destination;
            this.AdditionalFlags = AdditionalFlags;
            this.Tags = Tags;
            this.Options = Options;
        }

        public new async Task ConnectAsync()
        {
            await Transport.StartAsync();
            var handler = Connect(CancellationToken.None);

            var setup = new ArrayBufferWriter<byte>();
            var writer = new BufferWriter(setup, null);
            new Frames.DestinationSetup(IpAddress, Group, AccessKey, AccessToken, ConnectionId, AdditionalFlags, Tags).Write(writer);

            writer.Flush();
            var metadata = new ReadOnlySequence<byte>(setup.WrittenMemory);

            new RSocketProtocol.Setup((int)Options.KeepAlive.TotalMilliseconds, (int)Options.Lifetime.TotalMilliseconds, Options.MetadataMimeType, Options.DataMimeType, metadata: metadata).Write(Transport.Output);
            await Transport.Output.FlushAsync();

            setup.Dispose();
        }
    }
}
