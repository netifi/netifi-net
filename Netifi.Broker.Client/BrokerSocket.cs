using System;
using System.Buffers;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using RSocket;

namespace Netifi.Broker.Client
{
    public class BrokerSocket : RSocket.RSocket
    {
        private readonly RSocketClient client;
        private readonly Func<ReadOnlySequence<byte>, ReadOnlySequence<byte>> transformer;

        public BrokerSocket(RSocketClient client, Func<ReadOnlySequence<byte>, ReadOnlySequence<byte>> transformer) : base(client.Transport)
        {
            this.client = client;
            this.transformer = transformer;
        }

        public override IAsyncEnumerable<T> RequestChannel<TSource, T>(IAsyncEnumerable<TSource> source, Func<TSource, ReadOnlySequence<byte>> sourcemapper,
            Func<(ReadOnlySequence<byte> data, ReadOnlySequence<byte> metadata), T> resultmapper,
            ReadOnlySequence<byte> data = default, ReadOnlySequence<byte> metadata = default)
        {
            return client.RequestChannel(source, sourcemapper, resultmapper, data, transformer(metadata));
        }

        public override IAsyncEnumerable<T> RequestStream<T>(Func<(ReadOnlySequence<byte> data, ReadOnlySequence<byte> metadata), T> resultmapper,
            ReadOnlySequence<byte> data = default, ReadOnlySequence<byte> metadata = default)
        {
            return client.RequestStream(resultmapper, data, transformer(metadata));
        }

        public override Task<T> RequestResponse<T>(Func<(ReadOnlySequence<byte> data, ReadOnlySequence<byte> metadata), T> resultmapper,
            ReadOnlySequence<byte> data = default, ReadOnlySequence<byte> metadata = default)
        {
            return client.RequestResponse(resultmapper, data, transformer(metadata));
        }

        public override Task RequestFireAndForget(
            ReadOnlySequence<byte> data = default, ReadOnlySequence<byte> metadata = default)
        {
            return client.RequestFireAndForget(data, transformer(metadata));
        }
    }
}
