using System;

namespace Netifi.Broker
{
    partial class Frames
    {
        public const UInt16 MAJOR_VERSION = 1;
        public const UInt16 MINOR_VERSION = 0;

        private const int MaxMetadataLength = 16777215;

        public enum Types
        {
            Undefined = 0x00,
            Broker_Setup = 0x01,
            Destination_Setup = 0x02,
            Group = 0x03,
            Broadcast = 0x04,
            Shard = 0x05,
        }
    }
}
