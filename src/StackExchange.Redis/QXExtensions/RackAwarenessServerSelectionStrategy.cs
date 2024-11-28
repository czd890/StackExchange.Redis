using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace StackExchange.Redis.QXExtensions;

/// <summary>
/// The interface for the RackAwareness implementation.
/// </summary>
#pragma warning disable RS0016 // Add public types and members to the declared API
public interface IRackAwareness
{
    /// <summary>
    /// Get the client rack id.
    /// </summary>
    /// <returns>Rack id.</returns>
    string? GetClientRackId();

    /// <summary>
    /// Check if the node is in the same rack.
    /// </summary>
    /// <param name="nodeEndPoint">The redis node.</param>
    /// <param name="multiplexer">The multiplexer.</param>
    /// <returns> The client and redis node is in same rack range. </returns>
    bool? IsInSameRack(EndPoint nodeEndPoint, ConnectionMultiplexer multiplexer);
#pragma warning restore RS0016 // Add public types and members to the declared API
}

internal sealed class RackAwarenessServerSelectionStrategy : ServerSelectionStrategy
{
    private readonly IRackAwareness rackAwareness;
    public RackAwarenessServerSelectionStrategy(ConnectionMultiplexer multiplexer) : base(multiplexer)
    {
        rackAwareness = multiplexer.RawConfig.RackAwareness!;
    }

    protected override ServerEndPoint? Select(int slot, RedisCommand command, CommandFlags flags, bool allowDisconnected)
    {
        // Only interested in primary/replica preferences
        flags = Message.GetPrimaryReplicaFlags(flags);

        ServerEndPoint[]? arr;
        if (slot == NoSlot || (arr = map) == null) return Any(command, flags, allowDisconnected);

        ServerEndPoint endpoint = arr[slot];
        ServerEndPoint? testing; // but: ^^^ is the PRIMARY slots; if we want a replica, we need to do some thinking

        if (endpoint != null)
        {
            testing = endpoint;
            switch (flags)
            {
                case CommandFlags.DemandReplica:
                    testing = FindRackEndPoint(endpoint, command, false, allowDisconnected);
                    break;
                case CommandFlags.PreferReplica:
                    testing = FindRackEndPoint(endpoint, command, true, allowDisconnected);
                    break;
                case CommandFlags.DemandMaster:
                    testing = FindPrimary(endpoint, command);
                    break;
                case CommandFlags.PreferMaster:
                    testing = FindPrimary(endpoint, command);
                    break;
            }
            if (testing != null) return testing;
            if (endpoint.IsSelectable(command, allowDisconnected)) return endpoint;
        }
        return Any(command, flags, allowDisconnected);
    }

    private ServerEndPoint? FindRackEndPoint(ServerEndPoint mainEndpoint, RedisCommand command, bool anyRack, bool allowDisconnected)
    {
        var replicas = mainEndpoint.Replicas;
        var len = replicas.Length;
        uint startOffset = len <= 1 ? 0 : mainEndpoint.NextReplicaOffset();
        ServerEndPoint? fallback = null;
        for (int i = 0; i < len; i++)
        {
            var endpoint = replicas[(int)(((uint)i + startOffset) % len)];
            if (endpoint.IsReplica && endpoint.IsSelectable(command, allowDisconnected))
            {
                if (IsInSameRack(endpoint) == true)
                {
                    return endpoint;
                }
                else
                {
                    fallback = endpoint;
                }
            }
        }

        if (anyRack && mainEndpoint.IsSelectable(command, allowDisconnected) && (IsInSameRack(mainEndpoint) == true))
        {
            return mainEndpoint;
        }
        return fallback;
    }

    private ServerEndPoint? Any(RedisCommand command, CommandFlags flags, bool allowDisconnected) => AnyServerWithRack(ServerType, (uint)Interlocked.Increment(ref anyStartOffset), command, flags, allowDisconnected);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal ServerEndPoint? AnyServerWithRack(ServerType serverType, uint startOffset, RedisCommand command, CommandFlags flags, bool allowDisconnected)
    {
        var tmp = multiplexer.GetServerSnapshot();
        int len = tmp.Length;
        ServerEndPoint? fallback = null;
        ServerEndPoint? fallback2 = null;
        for (int i = 0; i < len; i++)
        {
            var server = tmp[(int)(((uint)i + startOffset) % len)];
            if (server != null && server.ServerType == serverType && server.IsSelectable(command, allowDisconnected))
            {
                if (server.IsReplica)
                {
                    switch (flags)
                    {
                        case CommandFlags.DemandReplica:
                        case CommandFlags.PreferReplica:
                            if (IsInSameRack(server) == true)
                                return server;
                            else
                                fallback = server;
                            break;
                        case CommandFlags.PreferMaster:
                            if (IsInSameRack(server) == true)
                                fallback2 = server;
                            else if (fallback == null)
                                fallback = server;
                            break;
                    }
                }
                else
                {
                    switch (flags)
                    {
                        case CommandFlags.DemandMaster:
                        case CommandFlags.PreferMaster:
                            if (IsInSameRack(server) == true)
                                return server;
                            else
                                fallback = server;
                            break;
                        case CommandFlags.PreferReplica:
                            if (IsInSameRack(server) == true)
                                fallback2 = server;
                            else if (fallback == null) fallback = server;
                            break;
                    }
                }
            }
        }
        return fallback2 ?? fallback;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private bool? IsInSameRack(ServerEndPoint endpoint)
    {
        // some logic to determine if the endpoint is in the same rack
        return rackAwareness.IsInSameRack(endpoint.EndPoint, multiplexer);
    }
}
