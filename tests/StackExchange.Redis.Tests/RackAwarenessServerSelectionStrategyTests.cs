using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading.Tasks;

using Moq;

using StackExchange.Redis.QXExtensions;

using Xunit;
using Xunit.Abstractions;

namespace StackExchange.Redis.Tests;
#if NET6_0_OR_GREATER
[Collection(SharedConnectionFixture.Key)]
public class RackAwarenessServerSelectionStrategyTests : TestBase
{
    public RackAwarenessServerSelectionStrategyTests(ITestOutputHelper output, SharedConnectionFixture fixture) : base(output, fixture) { }

    internal class MyRackAwareness : IRackAwareness
    {
        private readonly string clientRackId;
        public MyRackAwareness(string rackId)
        {
            clientRackId = rackId;
        }
        private readonly Dictionary<IPEndPoint, string> rackIdMap = new()
        {
            { IPEndPoint.Parse("172.16.1.1"), "1a" },
            { IPEndPoint.Parse("172.16.1.2"), "1b" },
            { IPEndPoint.Parse("172.16.1.3"), "1c" },
            { IPEndPoint.Parse("172.16.2.1"), "1c" },
            { IPEndPoint.Parse("172.16.2.2"), "1b" },
            { IPEndPoint.Parse("172.16.2.3"), "1a" },
        };
        public string? GetClientRackId() => clientRackId;
        public bool? IsInSameRack(EndPoint nodeEndPoint, ConnectionMultiplexer multiplexer)
        {
            foreach (var item in rackIdMap)
            {
                if (item.Key.Equals(nodeEndPoint))
                {
                    if (item.Value == clientRackId)
                        return true;
                    else
                        return false;
                }
            }
            return default;
        }
    }

    private RedisKey GetRedisKey(int slot)
    {
        /**
         target.HashSlot((RedisKey)"aa")
        1180
        target.HashSlot((RedisKey)"abcd")
        10294
         */
        if (slot <= (16384 / 2))
        {
            return "aa";
        }
        return "abcd";
    }

    [Theory]
    [InlineData("1a", 0, "172.16.1.1")]
    [InlineData("1b", 0, "172.16.1.2")]
    [InlineData("1c", 0, "172.16.1.3")]
    [InlineData("1a", 10000, "172.16.2.3")]
    [InlineData("1b", 10000, "172.16.2.2")]
    [InlineData("1c", 10000, "172.16.2.1")]
    public void SelectServer_Cluster_PreferReplica_MatchedRackId(string clientRackId, int redisKeySlot, string readNode)
    {
        var target = CreateClusterTarget(clientRackId);
        var msg = Message.Create(0, CommandFlags.PreferReplica, RedisCommand.GET, GetRedisKey(redisKeySlot));

        var endPoint = target.Select(msg, true);

        Assert.Equal(IPEndPoint.Parse(readNode), endPoint?.EndPoint);
    }
    [Theory]
    [InlineData("1d", 0, "172.16.1.2,172.16.1.3")]
    [InlineData("1e", 0, "172.16.1.2,172.16.1.3")]
    [InlineData("1f", 0, "172.16.1.2,172.16.1.3")]
    [InlineData("1d", 10000, "172.16.2.2,172.16.2.3")]
    [InlineData("1e", 10000, "172.16.2.2,172.16.2.3")]
    [InlineData("1f", 10000, "172.16.2.2,172.16.2.3")]
    public void SelectServer_Cluster_PreferReplica_NoMatchedRackId(string clientRackId, int redisKeySlot, string readNodes)
    {
        var target = CreateClusterTarget(clientRackId);
        var msg = Message.Create(0, CommandFlags.PreferReplica, RedisCommand.GET, GetRedisKey(redisKeySlot));

        var endPoint = target.Select(msg, true);

        Assert.Contains<EndPoint>(endPoint?.EndPoint!, readNodes.Split(',').Select(IPEndPoint.Parse).Cast<EndPoint>().ToArray());
    }

    [Theory]
    [InlineData("1d", 0, "172.16.1.2,172.16.1.3")]
    [InlineData("1e", 0, "172.16.1.2,172.16.1.3")]
    [InlineData("1f", 0, "172.16.1.2,172.16.1.3")]
    [InlineData("1d", 10000, "172.16.2.2,172.16.2.3")]
    [InlineData("1e", 10000, "172.16.2.2,172.16.2.3")]
    [InlineData("1f", 10000, "172.16.2.2,172.16.2.3")]
    public void SelectServer_Cluster_PreferReplica_NoMatchedRackId_RandomNodes(string clientRackId, int redisKeySlot, string readNodes)
    {
        var target = CreateClusterTarget(clientRackId);
        var msg = Message.Create(0, CommandFlags.PreferReplica, RedisCommand.GET, GetRedisKey(redisKeySlot));

        var endPoint = target.Select(msg, true);
        var endPoint2 = target.Select(msg, true);

        Assert.NotEqual(endPoint, endPoint2);

        Assert.Contains<EndPoint>(endPoint?.EndPoint!, readNodes.Split(',').Select(IPEndPoint.Parse).Cast<EndPoint>().ToArray());
        Assert.Contains<EndPoint>(endPoint2?.EndPoint!, readNodes.Split(',').Select(IPEndPoint.Parse).Cast<EndPoint>().ToArray());
    }

    [Theory]
    [InlineData("1d", 0, "172.16.1.2,172.16.1.3")]
    [InlineData("1e", 0, "172.16.1.2,172.16.1.3")]
    [InlineData("1f", 0, "172.16.1.2,172.16.1.3")]
    [InlineData("1d", 10000, "172.16.2.2,172.16.2.3")]
    [InlineData("1e", 10000, "172.16.2.2,172.16.2.3")]
    [InlineData("1f", 10000, "172.16.2.2,172.16.2.3")]
    public void SelectServer_Cluster_DemandReplica_NoMatchedRackId(string clientRackId, int redisKeySlot, string readNodes)
    {
        var target = CreateClusterTarget(clientRackId);
        var msg = Message.Create(0, CommandFlags.DemandReplica, RedisCommand.GET, GetRedisKey(redisKeySlot));

        var endPoint = target.Select(msg, true);

        Assert.Contains<EndPoint>(endPoint?.EndPoint!, readNodes.Split(',').Select(IPEndPoint.Parse).Cast<EndPoint>().ToArray());
    }

    [Theory]
    [InlineData("1a", "172.16.1.1")]
    [InlineData("1b", "172.16.1.2")]
    [InlineData("1c", "172.16.1.3")]
    public void SelectServer_Standalone_PreferReplica_MatchedRackId(string clientRackId, string readNodes)
    {
        var target = CreateStandaloneTarget(clientRackId);
        var msg = Message.Create(0, CommandFlags.PreferReplica, RedisCommand.GET, GetRedisKey(0));

        var endPoint = target.Select(msg, true);

        Assert.Contains<EndPoint>(endPoint?.EndPoint!, readNodes.Split(',').Select(IPEndPoint.Parse).Cast<EndPoint>().ToArray());
    }

    [Theory]
    [InlineData("1d", "172.16.1.2,172.16.1.3")]
    [InlineData("1e", "172.16.1.2,172.16.1.3")]
    [InlineData("1f", "172.16.1.2,172.16.1.3")]
    public void SelectServer_Standalone_PreferReplica_NoMatchedRackId(string clientRackId, string readNodes)
    {
        var target = CreateStandaloneTarget(clientRackId);
        var msg = Message.Create(0, CommandFlags.PreferReplica, RedisCommand.GET, GetRedisKey(0));

        var endPoint = target.AnyServerWithRack(ServerType.Standalone, 0, RedisCommand.GET, CommandFlags.PreferReplica, true);

        Assert.Contains<EndPoint>(endPoint?.EndPoint!, readNodes.Split(',').Select(IPEndPoint.Parse).Cast<EndPoint>().ToArray());
    }
    [Theory]
    [InlineData("1d", "172.16.1.2,172.16.1.3")]
    [InlineData("1e", "172.16.1.2,172.16.1.3")]
    [InlineData("1f", "172.16.1.2,172.16.1.3")]
    public void SelectServer_Standalone_PreferReplica_NoMatchedRackId_Random(string clientRackId, string readNodes)
    {
        var target = CreateStandaloneTarget(clientRackId);
        var msg = Message.Create(0, CommandFlags.PreferReplica, RedisCommand.GET, GetRedisKey(0));

        var endPoint = target.AnyServerWithRack(ServerType.Standalone, 0, RedisCommand.GET, CommandFlags.PreferReplica, true);
        var endPoint2 = target.AnyServerWithRack(ServerType.Standalone, 3, RedisCommand.GET, CommandFlags.PreferReplica, true);

        Assert.NotEqual(endPoint, endPoint2);
        Assert.Contains<EndPoint>(endPoint?.EndPoint!, readNodes.Split(',').Select(IPEndPoint.Parse).Cast<EndPoint>().ToArray());
        Assert.Contains<EndPoint>(endPoint2?.EndPoint!, readNodes.Split(',').Select(IPEndPoint.Parse).Cast<EndPoint>().ToArray());
    }
    [Theory]
    [InlineData("1d", "172.16.1.2,172.16.1.3")]
    [InlineData("1e", "172.16.1.2,172.16.1.3")]
    [InlineData("1f", "172.16.1.2,172.16.1.3")]
    public void SelectServer_Standalone_DemandReplica_NoMatchedRackId_Random(string clientRackId, string readNodes)
    {
        var target = CreateStandaloneTarget(clientRackId);
        var msg = Message.Create(0, CommandFlags.DemandReplica, RedisCommand.GET, GetRedisKey(0));

        var endPoint = target.AnyServerWithRack(ServerType.Standalone, 0, RedisCommand.GET, CommandFlags.DemandReplica, true);
        var endPoint2 = target.AnyServerWithRack(ServerType.Standalone, 3, RedisCommand.GET, CommandFlags.DemandReplica, true);

        Assert.NotEqual(endPoint, endPoint2);
        Assert.Contains<EndPoint>(endPoint?.EndPoint!, readNodes.Split(',').Select(IPEndPoint.Parse).Cast<EndPoint>().ToArray());
        Assert.Contains<EndPoint>(endPoint2?.EndPoint!, readNodes.Split(',').Select(IPEndPoint.Parse).Cast<EndPoint>().ToArray());
    }

    [Theory]
    [InlineData("1a", "172.16.1.2,172.16.1.3")]
    [InlineData("1b", "172.16.1.2")]
    [InlineData("1c", "172.16.1.3")]
    [InlineData("1d", "172.16.1.2,172.16.1.3")]
    [InlineData("1e", "172.16.1.2,172.16.1.3")]
    [InlineData("1f", "172.16.1.2,172.16.1.3")]
    public void SelectServer_Standalone_DemandReplica(string clientRackId, string readNodes)
    {
        var target = CreateStandaloneTarget(clientRackId);
        var msg = Message.Create(0, CommandFlags.DemandReplica, RedisCommand.GET, GetRedisKey(0));

        var endPoint = target.Select(msg, true);

        Assert.Contains<EndPoint>(endPoint?.EndPoint!, readNodes.Split(',').Select(IPEndPoint.Parse).Cast<EndPoint>().ToArray());
    }

    private RackAwarenessServerSelectionStrategy CreateClusterTarget(string clientRackId)
    {
        var multiplexer = Create(log: Writer).UnderlyingMultiplexer;
        multiplexer.RawConfig.RackAwareness = new MyRackAwareness(clientRackId);

        var testBridge = multiplexer.GetServerSnapshot()[0].GetBridge(ConnectionType.Interactive, true)!;

        var target = new RackAwarenessServerSelectionStrategy(multiplexer)
        {
            ServerType = ServerType.Cluster,
        };
        var serverMainEndPoint = new ServerEndPoint(multiplexer, IPEndPoint.Parse("172.16.1.1")).WithBridge(testBridge);
        var serverReplicaEndPoints = new List<ServerEndPoint>
        {
            new ServerEndPoint(multiplexer, IPEndPoint.Parse("172.16.1.2")) { IsReplica = true }.WithBridge(testBridge),
            new ServerEndPoint(multiplexer, IPEndPoint.Parse("172.16.1.3")) { IsReplica = true }.WithBridge(testBridge),
        };
        serverMainEndPoint.Replicas = serverReplicaEndPoints.ToArray();

        var serverMainEndPoint2 = new ServerEndPoint(multiplexer, IPEndPoint.Parse("172.16.2.1")).WithBridge(testBridge);
        var serverReplicaEndPoints2 = new List<ServerEndPoint>
        {
            new ServerEndPoint(multiplexer, IPEndPoint.Parse("172.16.2.2")) { IsReplica = true }.WithBridge(testBridge),
            new ServerEndPoint(multiplexer, IPEndPoint.Parse("172.16.2.3")) { IsReplica = true }.WithBridge(testBridge),
        };
        serverMainEndPoint2.Replicas = serverReplicaEndPoints2.ToArray();

        var halfSlot = 16384 / 2;
        target.UpdateClusterRange(0, halfSlot - 1, serverMainEndPoint);
        target.UpdateClusterRange(halfSlot, 16384 - 1, serverMainEndPoint2);
        return target;
    }

    private RackAwarenessServerSelectionStrategy CreateStandaloneTarget(string clientRackId)
    {
        var multiplexer = Create(shared: false, log: Writer).UnderlyingMultiplexer;
        multiplexer.RawConfig.RackAwareness = new MyRackAwareness(clientRackId);

        var testBridge = multiplexer.GetServerSnapshot()[0].GetBridge(ConnectionType.Interactive, true)!;

        var target = new RackAwarenessServerSelectionStrategy(multiplexer)
        {
            ServerType = ServerType.Standalone,
        };
        multiplexer.GetServerEndPoint(IPEndPoint.Parse("172.16.1.1"), activate: false).WithBridge(testBridge);
        multiplexer.GetServerEndPoint(IPEndPoint.Parse("172.16.1.2")).WithBridge(testBridge).WithReplica();
        multiplexer.GetServerEndPoint(IPEndPoint.Parse("172.16.1.3")).WithBridge(testBridge).WithReplica();

        return target;
    }
}
internal static class ServerEndPointExtensions
{
    internal static ServerEndPoint WithBridge(this ServerEndPoint serverEndPoint, PhysicalBridge bridge)
    {
        serverEndPoint.GetType()
            .GetField("interactive", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Instance)!
            .SetValue(serverEndPoint, bridge);

        return serverEndPoint;
    }
    internal static ServerEndPoint WithReplica(this ServerEndPoint serverEndPoint)
    {
        serverEndPoint.IsReplica = true;

        return serverEndPoint;
    }
}
#endif
