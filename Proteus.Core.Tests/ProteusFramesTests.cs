using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO.Pipelines;
using System.Linq;
using System.Net;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Proteus.Tests
{
	[TestClass]
	public class ProteusFramesTests
	{
		[TestMethod]
		public void BrokerSetupTest()
		{
            var accessToken = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes("access token"));
            var input = new Frames.BrokerSetup("brokerId", "clusterId", long.MaxValue, accessToken);

            var pipe = new Pipe();
            input.Write(pipe.Writer);
            pipe.Writer.FlushAsync().GetAwaiter().GetResult();

            var read = pipe.Reader.ReadAsync().GetAwaiter().GetResult();
            var reader = new SequenceReader<byte>(read.Buffer);

            var header = new Frames.Header(ref reader);
            Assert.AreEqual(header.Type, Frames.Types.Broker_Setup);

            var output = new Frames.BrokerSetup(header, ref reader);
            Assert.AreEqual(input.brokerId, output.brokerId);
            Assert.AreEqual(input.clusterId, output.clusterId);
            Assert.AreEqual(input.accessKey, output.accessKey);
            Assert.IsTrue(input.accessToken.ToArray().SequenceEqual(output.accessToken.ToArray()));
        }

        [TestMethod]
        public void DestinationSetupTest()
        {
            var accessToken = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes("access token"));
            var connectiondId = Guid.NewGuid();
            short additionalFlags = 0b00000000_00000001;
            var tags = new SortedDictionary<string, string> {
                { "destination", "destination" }
            };

            var input = new Frames.DestinationSetup(IPAddress.Loopback, "group", long.MaxValue, accessToken, connectiondId, additionalFlags, tags);

            var pipe = new Pipe();
            input.Write(pipe.Writer);
            pipe.Writer.FlushAsync().GetAwaiter().GetResult();

            var read = pipe.Reader.ReadAsync().GetAwaiter().GetResult();
            var reader = new SequenceReader<byte>(read.Buffer);

            var header = new Frames.Header(ref reader);
            Assert.AreEqual(header.Type, Frames.Types.Destination_Setup);

            var output = new Frames.DestinationSetup(header, ref reader);
            Assert.AreEqual(input.ipAddress, output.ipAddress);
            Assert.AreEqual(input.group, output.group);
            Assert.AreEqual(input.accessKey, output.accessKey);
            Assert.IsTrue(input.accessToken.ToArray().SequenceEqual(output.accessToken.ToArray()));
            Assert.AreEqual(input.connectionId, output.connectionId);
            Assert.AreEqual(input.additionalFlags, output.additionalFlags);
            Assert.IsTrue(input.tags.SequenceEqual(output.tags));
        }

        [TestMethod]
        public void GroupTest()
        {
            var metadata = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes("metadata"));
            var tags = new SortedDictionary<string, string> {
                { "destination", "destination" }
            };

            var input = new Frames.Group("group", metadata, tags);

            var pipe = new Pipe();
            input.Write(pipe.Writer);
            pipe.Writer.FlushAsync().GetAwaiter().GetResult();

            var read = pipe.Reader.ReadAsync().GetAwaiter().GetResult();
            var reader = new SequenceReader<byte>(read.Buffer);

            var header = new Frames.Header(ref reader);
            Assert.AreEqual(header.Type, Frames.Types.Group);

            var output = new Frames.Group(header, ref reader);
            Assert.AreEqual(input.group, output.group);
            Assert.IsTrue(input.metadata.ToArray().SequenceEqual(output.metadata.ToArray()));
            Assert.IsTrue(input.tags.SequenceEqual(output.tags));
        }

        [TestMethod]
        public void BroadcastTest()
        {
            var metadata = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes("metadata"));
            var tags = new SortedDictionary<string, string> {
                { "destination", "destination" }
            };

            var input = new Frames.Broadcast("group", metadata, tags);

            var pipe = new Pipe();
            input.Write(pipe.Writer);
            pipe.Writer.FlushAsync().GetAwaiter().GetResult();

            var read = pipe.Reader.ReadAsync().GetAwaiter().GetResult();
            var reader = new SequenceReader<byte>(read.Buffer);

            var header = new Frames.Header(ref reader);
            Assert.AreEqual(header.Type, Frames.Types.Broadcast);

            var output = new Frames.Broadcast(header, ref reader);
            Assert.AreEqual(input.group, output.group);
            Assert.IsTrue(input.metadata.ToArray().SequenceEqual(output.metadata.ToArray()));
            Assert.IsTrue(input.tags.SequenceEqual(output.tags));
        }

        [TestMethod]
        public void ShardTest()
        {
            var metadata = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes("metadata"));
            var shardKey = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes("shardKey"));
            var tags = new SortedDictionary<string, string> {
                { "destination", "destination" }
            };

            var input = new Frames.Shard("group", metadata, shardKey, tags);

            var pipe = new Pipe();
            input.Write(pipe.Writer);
            pipe.Writer.FlushAsync().GetAwaiter().GetResult();

            var read = pipe.Reader.ReadAsync().GetAwaiter().GetResult();
            var reader = new SequenceReader<byte>(read.Buffer);

            var header = new Frames.Header(ref reader);
            Assert.AreEqual(header.Type, Frames.Types.Shard);

            var output = new Frames.Shard(header, ref reader);
            Assert.AreEqual(input.group, output.group);
            Assert.IsTrue(input.metadata.ToArray().SequenceEqual(output.metadata.ToArray()));
            Assert.IsTrue(input.shardKey.ToArray().SequenceEqual(output.shardKey.ToArray()));
            Assert.IsTrue(input.tags.SequenceEqual(output.tags));
        }
    }
}