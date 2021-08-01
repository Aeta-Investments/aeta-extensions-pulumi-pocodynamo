using System;
using System.Linq;
using FluentAssertions;
using Pulumi.Aws.DynamoDB;
using ServiceStack.Aws.DynamoDb;
using ServiceStack.DataAnnotations;
using Xunit;

namespace Aeta.Extensions.Pulumi.PocoDynamo.Tests
{
    public class TableBuilderTests
    {
        [Fact]
        public void Builder_Should_Correctly_Set_Key_Schema()
        {
            var builder = new TableBuilder("TestTable", new[] {typeof(Table)});
            var tableArgs = new TableArgs();

            builder.SetKeySchema(tableArgs);
            tableArgs.HashKey.GetValue().Should().Be(nameof(Table.TestHashKey));
            tableArgs.RangeKey.GetValue().Should().Be(nameof(Table.TestRangeKey));
        }

        [Fact]
        public void Builder_Should_Correctly_Set_Table_Attributes_For_Unit_Table_Group()
        {
            var builder = new TableBuilder("TestTable", new[] {typeof(Table)});
            var tableArgs = new TableArgs();

            builder.SetTableAttributes(tableArgs);

            var expectedAttributes = new[]
            {
                (name: nameof(Table.TestHashKey), type: "S"),
                (name: nameof(Table.TestRangeKey), type: "S"),
                (name: nameof(Table.NumberAttribute), type: "N")
            };

            var actualAttributes = tableArgs.Attributes.GetValue()
                .Select(attribute => (name: attribute.Name.GetValue(), type: attribute.Type.GetValue()));

            actualAttributes.Should().BeEquivalentTo(expectedAttributes);
        }

        [Fact]
        public void Builder_Should_Set_BillingMode_To_OnDemand_If_ProvisionedThroughputAttribute_Is_Not_Set()
        {
            var builder = new TableBuilder("TestTable", new[] {typeof(Table)});
            var tableArgs = new TableArgs();

            builder.SetBillingMode(tableArgs);
            tableArgs.BillingMode.GetValue().Should().Be("PAY_PER_REQUEST");
        }

        [Fact]
        public void Builder_Should_Set_BillingMode_To_Provisioned_If_ProvisionedThroughputAttribute_Is_Set()
        {
            var builder = new TableBuilder("TestTable", new[] {typeof(ProvisionedTable)});
            var tableArgs = new TableArgs();

            builder.SetBillingMode(tableArgs);
            tableArgs.BillingMode.GetValue().Should().Be("PROVISIONED");

            var throughput = (ProvisionedThroughputAttribute) Attribute.GetCustomAttribute(
                typeof(ProvisionedTable), typeof(ProvisionedThroughputAttribute));

            tableArgs.ReadCapacity.GetValue().Should().Be(throughput!.ReadCapacityUnits);
            tableArgs.WriteCapacity.GetValue().Should().Be(throughput.WriteCapacityUnits);
        }

        [Fact]
        public void Builder_Should_Set_GlobalIndexes()
        {
            var builder = new TableBuilder("TestTable", new[] {typeof(Table)});
            var tableArgs = new TableArgs();

            builder.SetGlobalSecondaryIndexes(tableArgs);
            var indexes = tableArgs.GlobalSecondaryIndexes.GetValue();
            var indexOne = indexes.SingleOrDefault(index => index.Name.GetValue() == nameof(GlobalIndexOne));
            var indexTwo = indexes.SingleOrDefault(index => index.Name.GetValue() == nameof(GlobalIndexTwo));

            indexOne.Should().NotBeNull();
            indexOne!.HashKey.GetValue().Should().Be(nameof(GlobalIndexOne.TestRangeKey));
            indexOne.ProjectionType.GetValue().Should().Be("INCLUDE");
            indexOne.NonKeyAttributes.GetValue().Should().Contain(nameof(GlobalIndexOne.NumberAttribute));
            indexOne.ReadCapacity.Should().BeNull();
            indexOne.WriteCapacity.Should().BeNull();

            indexTwo.Should().NotBeNull();
            indexTwo!.HashKey.GetValue().Should().Be(nameof(GlobalIndexTwo.TestRangeKey));
            indexTwo.RangeKey.GetValue().Should().Be(nameof(GlobalIndexTwo.TestHashKey));

            var throughput = (ProvisionedThroughputAttribute) Attribute.GetCustomAttribute(
                typeof(GlobalIndexTwo), typeof(ProvisionedThroughputAttribute));
            indexTwo.ReadCapacity.GetValue().Should().Be(throughput!.ReadCapacityUnits);
            indexTwo.WriteCapacity.GetValue().Should().Be(throughput.WriteCapacityUnits);
        }
    }

    [Alias("TestTableProvisioned")]
    [ProvisionedThroughput(ReadCapacityUnits = 10, WriteCapacityUnits = 5)]
    public class ProvisionedTable
    {
        [HashKey] public string TestHashKey { get; set; }
        public int NumberAttribute { get; set; }
    }

    [Alias("TestTable")]
    [References(typeof(GlobalIndexOne))]
    [References(typeof(GlobalIndexTwo))]
    public class Table
    {
        [HashKey] public string TestHashKey { get; set; }
        [RangeKey] public string TestRangeKey { get; set; }

        public int NumberAttribute { get; set; }
    }

    public class GlobalIndexOne : IGlobalIndex<Table>
    {
        [HashKey] public string TestRangeKey { get; set; }
        public int NumberAttribute { get; set; }
    }

    [ProvisionedThroughput(ReadCapacityUnits = 10, WriteCapacityUnits = 5)]
    public class GlobalIndexTwo : IGlobalIndex<Table>
    {
        [HashKey] public string TestRangeKey { get; set; }
        [RangeKey] public string TestHashKey { get; set; }
    }
}