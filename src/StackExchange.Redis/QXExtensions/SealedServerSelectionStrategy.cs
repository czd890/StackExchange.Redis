using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StackExchange.Redis.QXExtensions;
internal sealed class SealedServerSelectionStrategy : ServerSelectionStrategy
{
    public SealedServerSelectionStrategy(ConnectionMultiplexer multiplexer) : base(multiplexer)
    {
    }
}
