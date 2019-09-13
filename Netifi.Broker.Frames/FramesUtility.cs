using System;
using System.Buffers;
namespace Netifi.Broker
{
    public class FramesUtility
    {
        public static ReadOnlySequence<byte> UnwrapMetadata(SequenceReader<byte> frame)
        {
            var header = new Frames.Header(ref frame);

            switch (header.Type)
            {
                case Frames.Types.Group:
                    return new Frames.Group(in header, ref frame).metadata;
                case Frames.Types.Broadcast:
                    return new Frames.Broadcast(in header, ref frame).metadata;
                case Frames.Types.Shard:
                    return new Frames.Shard(in header, ref frame).metadata;
                default:
                    throw new ArgumentException("unknown frame type " + header.Type);
            }
        }
    }
}
