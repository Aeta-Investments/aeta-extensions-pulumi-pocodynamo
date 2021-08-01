using System.Linq;
using FluentAssertions;
using ServiceStack.DataAnnotations;
using Xunit;

namespace Aeta.Extensions.Pulumi.PocoDynamo.Tests
{
    public class PlatformExtensionsTests
    {
        [Fact]
        public void FindTables_Should_Find_Table_In_Assembly_Decorated_With_AliasAttribute()
        {
            var assembly = typeof(TableToFind).Assembly;
            var builders = assembly.FindPocoDynamoTableDefinitions().ToList();

            builders.Should().NotBeEmpty();
            builders.Should().Contain(builder => builder.TableName == "TableToFind");
        }

        [Fact]
        public void FindTables_Should_Find_Polymorphic_Tables_In_Assembly_Decorated_With_AliasAttribute()
        {
            var assembly = typeof(TableToFind).Assembly;
            var builders = assembly.FindPocoDynamoTableDefinitions().ToList();
            var builder = builders.Find(b => b.TableName == "TableToFind");

            builder.Should().NotBeNull();
            builder!.Metadatas.Select(m => m.Type)
                .Should().BeEquivalentTo(typeof(TableToFind), typeof(TableToFindKindOne), typeof(TableToFindKindTwo));
        }
    }

    [Alias("TableToFind")]
    public class TableToFind
    {
        [HashKey] public string HashKey { get; set; }
    }

    [Alias("TableToFind")]
    public class TableToFindKindOne : TableToFind
    {
    }


    [Alias("TableToFind")]
    public class TableToFindKindTwo : TableToFind
    {
    }
}