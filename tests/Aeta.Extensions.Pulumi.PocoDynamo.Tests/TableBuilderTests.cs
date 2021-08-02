using System;
using System.Linq;
using FluentAssertions;
using Pulumi;
using Pulumi.Aws.DynamoDB;
using Pulumi.Aws.DynamoDB.Inputs;
using ServiceStack.Aws.DynamoDb;
using ServiceStack.DataAnnotations;
using Xunit;

namespace Aeta.Extensions.Pulumi.PocoDynamo.Tests
{
    public class TableBuilderTests
    {
        [Fact]
        public void SetKeySchema_Should_Set_Primary_Key_Properties()
        {
            var builder = new TableBuilder("TestTable", new[] {typeof(Table)});
            var tableArgs = new TableArgs();

            builder.SetKeySchema(tableArgs);
            tableArgs.HashKey.GetValue().Should().Be(nameof(Table.TestHashKey));
            tableArgs.RangeKey.GetValue().Should().Be(nameof(Table.TestRangeKey));
        }
        
        [Fact]
        public void SetKeySchema_Should_Add_Primary_Key_Properties_To_Table_Attributes()
        {
            var builder = new TableBuilder("TestTable", new[] {typeof(Table)});
            var tableArgs = new TableArgs();

            builder.SetKeySchema(tableArgs);
            var tableAttributes = tableArgs.Attributes.GetValue()
                .Select(attr => (attr.Name.GetValue(), attr.Type.GetValue()));

            tableAttributes.Should().BeEquivalentTo(new[]
            {
                (name: nameof(Table.TestHashKey), "S"),
                (name: nameof(Table.TestRangeKey), "S"),
            });
        }

        [Fact]
        public void SetKeySchema_Should_Not_Duplicate_Primary_Key_Properties_In_Table_Attributes()
        {
            var builder = new TableBuilder("TestTable", new[] {typeof(Table)});
            var tableArgs = new TableArgs()
            {
                Attributes = new InputList<TableAttributeArgs>
                {
                    new TableAttributeArgs
                    {
                        Name = nameof(Table.TestHashKey),
                        Type = "S"
                    }
                }
            };

            builder.SetKeySchema(tableArgs);
            var tableAttributes = tableArgs.Attributes.GetValue()
                .Select(attr => (attr.Name.GetValue(), attr.Type.GetValue()));

            tableAttributes.Should().BeEquivalentTo(new[]
            {
                (name: nameof(Table.TestHashKey), "S"),
                (name: nameof(Table.TestRangeKey), "S"),
            });
        }

        [Fact]
        public void SetBillingMode_Should_Set_BillingMode_To_OnDemand_If_ProvisionedThroughputAttribute_Is_Not_Set()
        {
            var builder = new TableBuilder("TestTable", new[] {typeof(Table)});
            var tableArgs = new TableArgs();

            builder.SetBillingMode(tableArgs);
            tableArgs.BillingMode.GetValue().Should().Be("PAY_PER_REQUEST");
        }

        [Fact]
        public void SetBillingMode_Should_Set_BillingMode_To_Provisioned_If_ProvisionedThroughputAttribute_Is_Set()
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
        public void SetGlobalIndexes_Should_Set_GlobalIndex_PrimaryKey_Schema()
        {
            var builder = new TableBuilder("TestTable", new[] {typeof(Table)});
            var tableArgs = new TableArgs();

            builder.SetGlobalSecondaryIndexes(tableArgs);
            var indexes = tableArgs.GlobalSecondaryIndexes.GetValue();
            var indexOne = indexes.SingleOrDefault(index => index.Name.GetValue() == nameof(GlobalIndexOne));
            var indexTwo = indexes.SingleOrDefault(index => index.Name.GetValue() == nameof(GlobalIndexTwo));

            indexOne.Should().NotBeNull();
            indexOne!.HashKey.GetValue().Should().Be(nameof(GlobalIndexOne.TestHashKey));
            indexOne.RangeKey.GetValue().Should().Be(nameof(GlobalIndexOne.GlobalIndexOneRangeKey));
            
            indexTwo.Should().NotBeNull();
            indexTwo!.HashKey.GetValue().Should().Be(nameof(GlobalIndexTwo.TestHashKey));
            indexTwo.RangeKey.GetValue().Should().Be(nameof(GlobalIndexTwo.GlobalIndexTwoRangeKey));
        }
        
        [Fact]
        public void SetGlobalIndexes_Should_Add_GlobalIndex_PrimaryKey_Properties_To_Table_Attributes()
        {
            var builder = new TableBuilder("TestTable", new[] {typeof(Table)});
            var tableArgs = new TableArgs();

            builder.SetGlobalSecondaryIndexes(tableArgs);
            var tableAttributes = tableArgs.Attributes.GetValue()
                .Select(attr => (attr.Name.GetValue(), attr.Type.GetValue()));

            tableAttributes.Should().BeEquivalentTo(new[]
            {
                (name: nameof(Table.TestHashKey), "S"),
                (name: nameof(Table.GlobalIndexOneRangeKey), "S"),
                (name: nameof(Table.GlobalIndexTwoRangeKey), "S"),
            });
        }
        
        [Fact]
        public void SetGlobalIndexes_Should_Not_Duplicate_GlobalIndex_PrimaryKey_Properties_In_Table_Attributes()
        {
            var builder = new TableBuilder("TestTable", new[] {typeof(Table)});
            var tableArgs = new TableArgs()
            {
                Attributes = new InputList<TableAttributeArgs>
                {
                    new TableAttributeArgs
                    {
                        Name = nameof(Table.TestHashKey),
                        Type = "S"
                    },
                    new TableAttributeArgs
                    {
                        Name = nameof(Table.TestRangeKey),
                        Type = "S"
                    }
                }
            };

            builder.SetGlobalSecondaryIndexes(tableArgs);
            var tableAttributes = tableArgs.Attributes.GetValue()
                .Select(attr => (attr.Name.GetValue(), attr.Type.GetValue()));

            tableAttributes.Should().BeEquivalentTo(new[]
            {
                (name: nameof(Table.TestHashKey), "S"),
                (name: nameof(Table.TestRangeKey), "S"),
                (name: nameof(Table.GlobalIndexOneRangeKey), "S"),
                (name: nameof(Table.GlobalIndexTwoRangeKey), "S"),
            });
        }
        
        [Fact]
        public void SetGlobalIndexes_Should_Set_Global_Index_Projection_Attributes()
        {
            var builder = new TableBuilder("TestTable", new[] {typeof(Table)});
            var tableArgs = new TableArgs();

            builder.SetGlobalSecondaryIndexes(tableArgs);
            var indexes = tableArgs.GlobalSecondaryIndexes.GetValue();
            var indexOne = indexes.Single(index => index.Name.GetValue() == nameof(GlobalIndexOne));
            indexOne.ProjectionType.GetValue().Should().Be("INCLUDE");
            indexOne.NonKeyAttributes.GetValue().Should()
                .BeEquivalentTo(new[] {nameof(GlobalIndexOne.NumberAttribute)});
        }
        
        [Fact]
        public void SetGlobalIndexes_Should_Set_Provisioned_Capacity_When_ProvisionedThroughputAttribute_Is_Used()
        {
            var builder = new TableBuilder("TestTable", new[] {typeof(Table)});
            var tableArgs = new TableArgs();

            builder.SetGlobalSecondaryIndexes(tableArgs);
            var indexes = tableArgs.GlobalSecondaryIndexes.GetValue();
            var indexTwo = indexes.Single(index => index.Name.GetValue() == nameof(GlobalIndexTwo));
            
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
        
        public string GlobalIndexOneRangeKey { get; set; }
        
        public string GlobalIndexTwoRangeKey { get; set; }
    }

    public class GlobalIndexOne : IGlobalIndex<Table>
    {
        [HashKey] public string TestHashKey { get; set; }
        [RangeKey] public string GlobalIndexOneRangeKey { get; set; }
        public int NumberAttribute { get; set; }
    }

    [ProvisionedThroughput(ReadCapacityUnits = 10, WriteCapacityUnits = 5)]
    public class GlobalIndexTwo : IGlobalIndex<Table>
    {
        [HashKey] public string TestHashKey { get; set; }
        [RangeKey] public string GlobalIndexTwoRangeKey { get; set; }
    }
}