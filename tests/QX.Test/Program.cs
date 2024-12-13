// See https://aka.ms/new-console-template for more information
using System.Collections.Concurrent;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;

using StackExchange.Redis;
using StackExchange.Redis.Profiling;
using StackExchange.Redis.QXExtensions;

Console.WriteLine("Hello, World!");

await StandloneReplicaTestAsync();
static async Task StandloneReplicaTestAsync()
{
    var options = ConfigurationOptions.Parse("forex.jgsyzg.ng.0001.use1.cache.amazonaws.com:14533");
    var conn = await StackExchange.Redis.ConnectionMultiplexer.ConnectAsync(options);
    // await conn.ConfigureAsync();
    var c = await conn.GetDatabase().StringGetAsync("249:IQDMXV", CommandFlags.DemandReplica);
    Console.WriteLine(c);
}

var options = ConfigurationOptions.Parse("staging-new.jgsyzg.clustercfg.use1.cache.amazonaws.com:13983,connectTimeout=30000");
var rackAwareness = new RackAwarenessImpl();
options.RackAwareness = rackAwareness;
var conn = await StackExchange.Redis.ConnectionMultiplexer.ConnectAsync(options);

var servers = conn.GetServers();
HashSet<EndPoint> mainNodes = new(EqualityComparer<EndPoint>.Create((x, y) => x != null && x.Equals(y), (_) => 0));
foreach (var node in servers[0].ClusterConfiguration!.Nodes)
{
    if (!node.IsReplica)
    {
        mainNodes.Add(node.EndPoint!);

        Console.WriteLine($"main node:{node.EndPoint} on {rackAwareness.GetRackId(node.EndPoint!)}. for {string.Join(", ", node.Slots.Select(x => $"{x.From}~{x.To}"))}");
        foreach (var item in node.Children)
        {
            Console.WriteLine($"replica node: {item.EndPoint} on {rackAwareness.GetRackId(item.EndPoint!)}");
        }
    }
}

var profiler = new ProfilingSession();
conn.RegisterProfiler(() => profiler);

var db = conn.GetDatabase();

while (mainNodes.Count != 0)
{
    var id = Guid.NewGuid().ToString("N");
    var slot = conn.GetHashSlot(id);
    var node = servers[0].ClusterConfiguration!.GetBySlot(slot);
    if (mainNodes.Remove(node!.EndPoint!))
    {
        Console.WriteLine($"redis key '{id}' is on slot '{slot}', is on node '{node!.EndPoint}', on rack '{rackAwareness.GetRackId(node.EndPoint!)}'");
    }
}

/*

main node:172.16.250.218:13983 on us-east-1e. for 1242~2578, 2872~3163, 3456~3576, 3870~4654, 9533~9590, 10177~10209, 11365~11468
replica node: 172.16.212.103:13983 on us-east-1b
main node:172.16.219.144:13983 on us-east-1b. for 0~168, 7678~9532, 9591~9681, 9975~10176, 12695~12729, 15671~16025, 16360~16383
replica node: 172.16.254.154:13983 on us-east-1e
main node:172.16.239.33:13983 on us-east-1d. for 4655~6702, 9682~9974, 11902~12291
replica node: 172.16.241.244:13983 on us-east-1e
main node:172.16.243.162:13983 on us-east-1e. for 3577~3869, 10210~11364, 12407~12694, 12730~13622, 16026~16127
replica node: 172.16.232.203:13983 on us-east-1d
main node:172.16.220.198:13983 on us-east-1b. for 169~1241, 2579~2871, 6703~7677, 11595~11901, 12292~12374
replica node: 172.16.237.15:13983 on us-east-1d
main node:172.16.215.42:13983 on us-east-1b. for 3164~3455, 11469~11594, 12375~12406, 13623~15670, 16128~16359
replica node: 172.16.239.180:13983 on us-east-1d

redis key '645786023b29445ca3fca4f13ee07c44' is on slot '9050', is on node '172.16.219.144:13983', on rack 'us-east-1b'
redis key '9560e2b9178d42a0a1f0bf0ae3136e4b' is on slot '2007', is on node '172.16.250.218:13983', on rack 'us-east-1e'
redis key '394c4a49b65148eb9339fba6e2fcf7f7' is on slot '14233', is on node '172.16.215.42:13983', on rack 'us-east-1b'
redis key 'e35c501f8299474290ebd50cc5ff9f2c' is on slot '10764', is on node '172.16.243.162:13983', on rack 'us-east-1e'
redis key '1ee053b9173742ae825df2b9570f796b' is on slot '7545', is on node '172.16.220.198:13983', on rack 'us-east-1b'
redis key 'a44144451e0a423b803716caa259862d' is on slot '6628', is on node '172.16.239.33:13983', on rack 'us-east-1d'
 */

// key a44144451e0a423b803716caa259862d on main 1d node, replica is 172.16.241.244:13983 on us-east-1e
// key 394c4a49b65148eb9339fba6e2fcf7f7 on mian 1b node, replica is 172.16.239.180:13983 on us-east-1d
Console.WriteLine("---------------a44144451e0a423b803716caa259862d----(main:1d,repl:1e)---------------");
Console.WriteLine("PreferReplica");
var v1 = await db.StringGetAsync("a44144451e0a423b803716caa259862d", CommandFlags.PreferReplica); // on main
Console.WriteLine("DemandReplica");
var v0 = await db.StringGetAsync("a44144451e0a423b803716caa259862d", CommandFlags.DemandReplica); // on main
Console.WriteLine("PreferMaster");
var v2 = await db.StringGetAsync("a44144451e0a423b803716caa259862d", CommandFlags.PreferMaster); // on main

Console.WriteLine("---------------394c4a49b65148eb9339fba6e2fcf7f7----(main:1b,repl:1d)---------------");
Console.WriteLine("PreferReplica");
var v3 = await db.StringGetAsync("394c4a49b65148eb9339fba6e2fcf7f7", CommandFlags.PreferReplica); // on replica
Console.WriteLine("PreferMaster");
var v4 = await db.StringGetAsync("394c4a49b65148eb9339fba6e2fcf7f7", CommandFlags.PreferMaster); // on main
Console.WriteLine("................");
foreach (var command in profiler.FinishProfiling())
{
    Console.WriteLine($"command on {command.EndPoint.ToString()}, cmd:{command.Command}, flag:{command.Flags.ToString()}");
}

public class RackAwarenessImpl : IRackAwareness
{
    public RackAwarenessImpl()
    {
        _clientRackId = "us-east-1d";
    }
    private readonly string? _clientRackId;
    public string? GetClientRackId() => _clientRackId;

    private readonly Dictionary<string, string> _az_cidr = new()
    {
        { "us-east-1a", "172.16.192.0/20" },
        { "us-east-1b", "172.16.208.0/20" },
        { "us-east-1d", "172.16.224.0/20" },
        { "us-east-1e", "172.16.240.0/20" },
    };
    public string? GetRackId(EndPoint endPoint)
    {
        var ip = GetIpAddress(endPoint);
        if (ip is not null)
            return GetRackId(ip);
        return default;
    }

    private static IPAddress? GetIpAddress(EndPoint nodeEndPoint)
    {
        IPAddress? ip = default;
        if (nodeEndPoint is IPEndPoint ipEndPoint)
            ip = ipEndPoint.Address;
        else if (nodeEndPoint is DnsEndPoint dnsEndPoint)
            ip = Dns.GetHostAddresses(dnsEndPoint.Host).FirstOrDefault();
        return ip;
    }

    private static bool IsInSubnet(IPAddress ipAddress, string cidr)
    {
        var parts = cidr.Split('/');

        var baseAddress = BitConverter.ToInt32(IPAddress.Parse(parts[0]).GetAddressBytes(), 0);

        var address = BitConverter.ToInt32(ipAddress.GetAddressBytes(), 0);

        var mask = IPAddress.HostToNetworkOrder(-1 << (32 - int.Parse(parts[1])));

        return (baseAddress & mask) == (address & mask);
    }
    private string? GetRackId(IPAddress ip)
    {
        foreach (var rule in _az_cidr)
        {
            if (IsInSubnet(ip.MapToIPv4(), rule.Value))
            {
                return rule.Key;
            }
        }
        return default;
    }

    public string? GetRackId(IServer server) => GetRackId(server.EndPoint);
}
