using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Net;
using System.Text;
using RSocket;

namespace Netifi.Broker
{
    public partial class Frames
    {
        public ref struct BrokerSetup
        {
            private Header header;
            public string brokerId;
            public string clusterId;
            public long accessKey;
            public ReadOnlySequence<byte> accessToken;

            public int Length => header.Length
                + sizeof(Int32) + Encoding.UTF8.GetByteCount(brokerId)
                + sizeof(Int32) + Encoding.UTF8.GetByteCount(clusterId)
                + sizeof(Int64) + sizeof(Int32) + (int)accessToken.Length;

            public BrokerSetup(string brokerId, string clusterId, long accessKey, ReadOnlySequence<byte> accessToken)
            {
                this.header = new Header(Types.Broker_Setup);
                this.brokerId = brokerId;
                this.clusterId = clusterId;
                this.accessKey = accessKey;
                this.accessToken = accessToken;
            }

            public BrokerSetup(in Header header, ref SequenceReader<byte> reader)
            {
                this.header = header;
                reader.TryReadBigEndian(out int brokerIdLength);
                reader.TryRead(out brokerId, brokerIdLength, Encoding.UTF8);
                reader.TryReadBigEndian(out int clusterIdLength);
                reader.TryRead(out clusterId, clusterIdLength, Encoding.UTF8);
                reader.TryReadBigEndian(out accessKey);
                reader.TryReadBigEndian(out int accessTokenLength);
                accessToken = reader.Sequence.Slice(reader.Position, accessTokenLength);
                reader.Advance(accessTokenLength);
            }

            public void Write(PipeWriter pipe)
            {
                var writer = BufferWriter.Get(pipe);
                this.Write(writer);
                writer.Flush();
                BufferWriter.Return(writer);
            }

            void Write(BufferWriter writer)
            {
                var written = header.Write(writer);

                int brokerIdLength = Encoding.UTF8.GetByteCount(brokerId);
                written += writer.WriteInt32BigEndian(brokerIdLength);
                written += writer.Write(Encoding.UTF8.GetBytes(brokerId));

                int clusterIdLength = Encoding.UTF8.GetByteCount(clusterId);
                written += writer.WriteInt32BigEndian(clusterIdLength);
                written += writer.Write(Encoding.UTF8.GetBytes(clusterId));

                written += writer.WriteInt64BigEndian(accessKey);
                written += writer.WriteInt32BigEndian((int)accessToken.Length);
                written += writer.Write(accessToken);
            }
        }

        public ref struct DestinationSetup
        {
            private Header header;
            public IPAddress ipAddress;
            public string group;
            public long accessKey;
            public ReadOnlySequence<byte> accessToken;
            public Guid connectionId;
            public short additionalFlags;
            public SortedDictionary<string, string> tags;

            public int TagsLength => tags.Aggregate(0, (acc, entry) => acc
                + sizeof(Int32) + Encoding.UTF8.GetByteCount(entry.Key)
                + sizeof(Int32) + Encoding.UTF8.GetByteCount(entry.Value));

            public int Length => header.Length
                + sizeof(Int32) + (ipAddress != null ? ipAddress.GetAddressBytes().Length : 0)
                + sizeof(Int32) + Encoding.UTF8.GetByteCount(group)
                + sizeof(Int64) + sizeof(Int32) + (int)accessToken.Length
                + sizeof(Int64) + sizeof(Int64) + sizeof(Int16) + TagsLength;

            public DestinationSetup(IPAddress ipAddress, string group, long accessKey, ReadOnlySequence<byte> accessToken, Guid connectionId, short additionalFlags, SortedDictionary<string, string> tags)
            {
                this.header = new Header(Types.Destination_Setup);
                this.ipAddress = ipAddress;
                this.group = group;
                this.accessKey = accessKey;
                this.accessToken = accessToken;
                this.connectionId = connectionId;
                this.additionalFlags = additionalFlags;
                this.tags = tags;
            }

            public DestinationSetup(in Header header, ref SequenceReader<byte> reader)
            {
                this.header = header;
                reader.TryReadBigEndian(out int ipAddressLength);
                if (ipAddressLength > 0)
                {
                    var ipAddressBytes = reader.Sequence.Slice(reader.Position, ipAddressLength);
                    reader.Advance(ipAddressLength);
                    ipAddress = new IPAddress(ipAddressBytes.ToArray());
                }
                else
                {
                    ipAddress = null;
                }

                reader.TryReadBigEndian(out int groupLength);
                reader.TryRead(out group, groupLength, Encoding.UTF8);
                reader.TryReadBigEndian(out accessKey);
                reader.TryReadBigEndian(out int accessTokenLength);
                accessToken = reader.Sequence.Slice(reader.Position, accessTokenLength);
                reader.Advance(accessTokenLength);

                var guidBytes = reader.Sequence.Slice(reader.Position, 16).ToArray();
                reader.Advance(16);
                GuidUtility.SwapByteOrder(guidBytes);
                connectionId = new Guid(guidBytes);

                reader.TryReadBigEndian(out additionalFlags);

                tags = new SortedDictionary<string, string>();
                while (reader.Remaining > 0)
                {
                    reader.TryReadBigEndian(out int keyLength);
                    reader.TryRead(out string key, keyLength, Encoding.UTF8);
                    reader.TryReadBigEndian(out int valueLength);
                    reader.TryRead(out string value, valueLength, Encoding.UTF8);
                    tags.Add(key, value);
                }
            }

            public void Write(PipeWriter pipe)
            {
                var writer = BufferWriter.Get(pipe);
                this.Write(writer);
                writer.Flush();
                BufferWriter.Return(writer);
            }

            void Write(BufferWriter writer)
            {
                var written = header.Write(writer);

                if (ipAddress != null)
                {
                    var AddressBytes = ipAddress.GetAddressBytes();
                    written += writer.WriteInt32BigEndian(AddressBytes.Length);
                    written += writer.Write(AddressBytes);
                }
                else
                {
                    written += writer.WriteInt32BigEndian(0);
                }

                int groupLength = Encoding.UTF8.GetByteCount(group);
                written += writer.WriteInt32BigEndian(groupLength);
                written += writer.Write(Encoding.UTF8.GetBytes(group));

                written += writer.WriteInt64BigEndian(accessKey);
                written += writer.WriteInt32BigEndian((int)accessToken.Length);
                written += writer.Write(accessToken);

                var guidBytes = connectionId.ToByteArray();
                GuidUtility.SwapByteOrder(guidBytes);
                written += writer.Write(guidBytes);

                written += writer.WriteUInt16BigEndian(additionalFlags);

                foreach (var entry in tags)
                {
                    var keyLength = Encoding.UTF8.GetByteCount(entry.Key);
                    written += writer.WriteInt32BigEndian(keyLength);
                    written += writer.Write(Encoding.UTF8.GetBytes(entry.Key));
                    var valueLength = Encoding.UTF8.GetByteCount(entry.Value);
                    written += writer.WriteInt32BigEndian(valueLength);
                    written += writer.Write(Encoding.UTF8.GetBytes(entry.Value));
                }
            }
        }

        public ref struct Group
        {
            private Header header;
            public string group;
            public ReadOnlySequence<byte> metadata;
            public SortedDictionary<string, string> tags;

            public int TagsLength => tags.Aggregate(0, (acc, entry) => acc
                + sizeof(Int32) + Encoding.UTF8.GetByteCount(entry.Key)
                + sizeof(Int32) + Encoding.UTF8.GetByteCount(entry.Value));

            public int Length => header.Length
                + sizeof(Int32) + Encoding.UTF8.GetByteCount(group)
                + sizeof(Int32) + (int)metadata.Length + TagsLength;

            public Group(string group, ReadOnlySequence<byte> metadata, SortedDictionary<string, string> tags)
            {
                this.header = new Header(Types.Group);
                this.group = group;
                this.metadata = metadata;
                this.tags = tags;
            }

            public Group(in Header header, ref SequenceReader<byte> reader)
            {
                this.header = header;
                reader.TryReadBigEndian(out int groupLength);
                reader.TryRead(out group, groupLength, Encoding.UTF8);
                reader.TryReadBigEndian(out int metadataLength);
                metadata = reader.Sequence.Slice(reader.Position, metadataLength);
                reader.Advance(metadataLength);

                tags = new SortedDictionary<string, string>();
                while (reader.Remaining > 0)
                {
                    reader.TryReadBigEndian(out int keyLength);
                    reader.TryRead(out string key, keyLength, Encoding.UTF8);
                    reader.TryReadBigEndian(out int valueLength);
                    reader.TryRead(out string value, valueLength, Encoding.UTF8);
                    tags.Add(key, value);
                }
            }

            public void Write(PipeWriter pipe)
            {
                var writer = BufferWriter.Get(pipe);
                this.Write(writer);
                writer.Flush();
                BufferWriter.Return(writer);
            }

            void Write(BufferWriter writer)
            {
                var written = header.Write(writer);

                int groupLength = Encoding.UTF8.GetByteCount(group);
                written += writer.WriteInt32BigEndian(groupLength);
                written += writer.Write(Encoding.UTF8.GetBytes(group));

                written += writer.WriteInt32BigEndian((int)metadata.Length);
                written += writer.Write(metadata);

                foreach (var entry in tags)
                {
                    var keyLength = Encoding.UTF8.GetByteCount(entry.Key);
                    written += writer.WriteInt32BigEndian(keyLength);
                    written += writer.Write(Encoding.UTF8.GetBytes(entry.Key));
                    var valueLength = Encoding.UTF8.GetByteCount(entry.Value);
                    written += writer.WriteInt32BigEndian(valueLength);
                    written += writer.Write(Encoding.UTF8.GetBytes(entry.Value));
                }
            }
        }

        public ref struct Broadcast
        {
            private Header header;
            public string group;
            public ReadOnlySequence<byte> metadata;
            public SortedDictionary<string, string> tags;

            public int TagsLength => tags.Aggregate(0, (acc, entry) => acc
                + sizeof(Int32) + Encoding.UTF8.GetByteCount(entry.Key)
                + sizeof(Int32) + Encoding.UTF8.GetByteCount(entry.Value));

            public int Length => header.Length
                + sizeof(Int32) + Encoding.UTF8.GetByteCount(group)
                + sizeof(Int32) + (int)metadata.Length + TagsLength;

            public Broadcast(string group, ReadOnlySequence<byte> metadata, SortedDictionary<string, string> tags)
            {
                this.header = new Header(Types.Broadcast);
                this.group = group;
                this.metadata = metadata;
                this.tags = tags;
            }

            public Broadcast(in Header header, ref SequenceReader<byte> reader)
            {
                this.header = header;
                reader.TryReadBigEndian(out int groupLength);
                reader.TryRead(out group, groupLength, Encoding.UTF8);
                reader.TryReadBigEndian(out int metadataLength);
                metadata = reader.Sequence.Slice(reader.Position, metadataLength);
                reader.Advance(metadataLength);

                tags = new SortedDictionary<string, string>();
                while (reader.Remaining > 0)
                {
                    reader.TryReadBigEndian(out int keyLength);
                    reader.TryRead(out string key, keyLength, Encoding.UTF8);
                    reader.TryReadBigEndian(out int valueLength);
                    reader.TryRead(out string value, valueLength, Encoding.UTF8);
                    tags.Add(key, value);
                }
            }

            public void Write(PipeWriter pipe)
            {
                var writer = BufferWriter.Get(pipe);
                this.Write(writer);
                writer.Flush();
                BufferWriter.Return(writer);
            }

            void Write(BufferWriter writer)
            {
                var written = header.Write(writer);

                int groupLength = Encoding.UTF8.GetByteCount(group);
                written += writer.WriteInt32BigEndian(groupLength);
                written += writer.Write(Encoding.UTF8.GetBytes(group));

                written += writer.WriteInt32BigEndian((int)metadata.Length);
                written += writer.Write(metadata);

                foreach (var entry in tags)
                {
                    var keyLength = Encoding.UTF8.GetByteCount(entry.Key);
                    written += writer.WriteInt32BigEndian(keyLength);
                    written += writer.Write(Encoding.UTF8.GetBytes(entry.Key));
                    var valueLength = Encoding.UTF8.GetByteCount(entry.Value);
                    written += writer.WriteInt32BigEndian(valueLength);
                    written += writer.Write(Encoding.UTF8.GetBytes(entry.Value));
                }
            }
        }

        public ref struct Shard
        {
            private Header header;
            public string group;
            public ReadOnlySequence<byte> metadata;
            public ReadOnlySequence<byte> shardKey;
            public SortedDictionary<string, string> tags;

            public int TagsLength => tags.Aggregate(0, (acc, entry) => acc
                + sizeof(Int32) + Encoding.UTF8.GetByteCount(entry.Key)
                + sizeof(Int32) + Encoding.UTF8.GetByteCount(entry.Value));

            public int Length => header.Length
                + sizeof(Int32) + Encoding.UTF8.GetByteCount(group)
                + sizeof(Int32) + (int)metadata.Length
                + sizeof(Int32) + (int)shardKey.Length + TagsLength;

            public Shard(string group, ReadOnlySequence<byte> metadata, ReadOnlySequence<byte> shardKey, SortedDictionary<string, string> tags)
            {
                this.header = new Header(Types.Shard);
                this.group = group;
                this.metadata = metadata;
                this.shardKey = shardKey;
                this.tags = tags;
            }

            public Shard(in Header header, ref SequenceReader<byte> reader)
            {
                this.header = header;
                reader.TryReadBigEndian(out int groupLength);
                reader.TryRead(out group, groupLength, Encoding.UTF8);

                reader.TryReadBigEndian(out int metadataLength);
                metadata = reader.Sequence.Slice(reader.Position, metadataLength);
                reader.Advance(metadataLength);

                reader.TryReadBigEndian(out int shardKeyLength);
                shardKey = reader.Sequence.Slice(reader.Position, shardKeyLength);
                reader.Advance(shardKeyLength);

                tags = new SortedDictionary<string, string>();
                while (reader.Remaining > 0)
                {
                    reader.TryReadBigEndian(out int keyLength);
                    reader.TryRead(out string key, keyLength, Encoding.UTF8);
                    reader.TryReadBigEndian(out int valueLength);
                    reader.TryRead(out string value, valueLength, Encoding.UTF8);
                    tags.Add(key, value);
                }
            }

            public void Write(PipeWriter pipe)
            {
                var writer = BufferWriter.Get(pipe);
                this.Write(writer);
                writer.Flush();
                BufferWriter.Return(writer);
            }

            void Write(BufferWriter writer)
            {
                var written = header.Write(writer);

                int groupLength = Encoding.UTF8.GetByteCount(group);
                written += writer.WriteInt32BigEndian(groupLength);
                written += writer.Write(Encoding.UTF8.GetBytes(group));

                written += writer.WriteInt32BigEndian((int)metadata.Length);
                written += writer.Write(metadata);

                written += writer.WriteInt32BigEndian((int)shardKey.Length);
                written += writer.Write(shardKey);

                foreach (var entry in tags)
                {
                    var keyLength = Encoding.UTF8.GetByteCount(entry.Key);
                    written += writer.WriteInt32BigEndian(keyLength);
                    written += writer.Write(Encoding.UTF8.GetBytes(entry.Key));
                    var valueLength = Encoding.UTF8.GetByteCount(entry.Value);
                    written += writer.WriteInt32BigEndian(valueLength);
                    written += writer.Write(Encoding.UTF8.GetBytes(entry.Value));
                }
            }
        }

        public ref struct Header
        {
            public UInt16 MajorVersion;
            public UInt16 MinorVersion;
            public Types Type;
            public int Length => sizeof(UInt16) + sizeof(UInt16) + sizeof(UInt16);

            public Header(Types type)
            {
                Type = type;
                MajorVersion = MAJOR_VERSION;
                MinorVersion = MINOR_VERSION;
            }

            public Header(ref SequenceReader<byte> reader)
            {
                reader.TryReadBigEndian(out UInt16 majorVersion); MajorVersion = majorVersion;
                reader.TryReadBigEndian(out UInt16 minorVersion); MinorVersion = minorVersion;
                reader.TryReadBigEndian(out UInt16 type); Type = (Types)type;
            }

            public int Write(BufferWriter writer)
            {
                writer.WriteUInt16BigEndian(MajorVersion);
                writer.WriteUInt16BigEndian(MinorVersion);
                writer.WriteUInt16BigEndian((int)Type);
                return Length;
            }
        }
    }
}
