using System;
using System.Collections.Generic;
using System.Linq;
using Pulumi.Aws.DynamoDB;
using Pulumi.Aws.DynamoDB.Inputs;
using ServiceStack;
using ServiceStack.Aws.DynamoDb;

namespace Aeta.Extensions.Pulumi.PocoDynamo
{
    public class TableBuilder
    {
        public TableBuilder(string tableName, IEnumerable<Type> types)
        {
            TableName = tableName;
            Metadatas = types.Select(DynamoMetadata.RegisterTable).ToList();
        }

        public IEnumerable<DynamoMetadataType> Metadatas { get; }

        public string TableName { get; }

        public TableBuilder SetBillingMode(TableArgs tableArgs)
        {
            var metadata = Metadatas.FirstOrDefault(table => table.ReadCapacityUnits.HasValue);
            if (metadata is null)
            {
                tableArgs.BillingMode = "PAY_PER_REQUEST";
            }
            else
            {
                tableArgs.BillingMode = "PROVISIONED";
                tableArgs.ReadCapacity = metadata.ReadCapacityUnits;
                tableArgs.WriteCapacity = metadata.WriteCapacityUnits;
            }

            return this;
        }

        public TableBuilder SetKeySchema(TableArgs tableArgs)
        {
            var metadata = Metadatas.FirstOrDefault(table => table.HashKey is not null);
            if (metadata is null)
                throw new ArgumentException("Could not find a property with HashKey attribute in this table group.");

            var existingAttributes =
                new HashSet<string>(tableArgs.Attributes.GetValue().Select(attribute => attribute.Name.GetValue()));

            tableArgs.HashKey = metadata.HashKey.Name;
            if (!existingAttributes.Contains(metadata.HashKey.Name))
                tableArgs.Attributes.Add(new TableAttributeArgs
                {
                    Name = metadata.HashKey.Name,
                    Type = metadata.HashKey.DbType
                });

            if (metadata.RangeKey is null) return this;
            tableArgs.RangeKey = metadata.RangeKey.Name;
            if (!existingAttributes.Contains(metadata.RangeKey.Name))
                tableArgs.Attributes.Add(new TableAttributeArgs
                {
                    Name = metadata.RangeKey.Name,
                    Type = metadata.RangeKey.DbType
                });

            return this;
        }

        public TableBuilder SetGlobalSecondaryIndexes(TableArgs tableArgs)
        {
            var metadata = Metadatas.FirstOrDefault(table => !table.GlobalIndexes.IsEmpty());
            if (metadata is null) return this;

            tableArgs.GlobalSecondaryIndexes = metadata.GlobalIndexes.Select(index =>
                new TableGlobalSecondaryIndexArgs
                {
                    Name = index.Name,
                    HashKey = index.HashKey.Name,
                    RangeKey = index.RangeKey.Name,
                    ProjectionType = index.ProjectionType,
                    NonKeyAttributes = index.ProjectedFields
                        .Safe()
                        .Where(field => field != index.HashKey.Name && field != index.RangeKey.Name)
                        .ToList(),
                    ReadCapacity = (int?) index.ReadCapacityUnits,
                    WriteCapacity = (int?) index.WriteCapacityUnits
                }).ToList();

            var existingAttributes =
                new HashSet<string>(tableArgs.Attributes.GetValue().Select(attribute => attribute.Name.GetValue()));

            var indexKeyAttributes = metadata.GlobalIndexes
                .SelectMany(index => new[] {index.HashKey, index.RangeKey})
                .Where(field => field is not null)
                .DistinctBy(field => field.Name)
                .Where(field => !existingAttributes.Contains(field.Name))
                .Select(field => new TableAttributeArgs
                {
                    Name = field.Name,
                    Type = field.DbType
                });

            foreach (var tableAttributeArgs in indexKeyAttributes)
                tableArgs.Attributes.Add(tableAttributeArgs);

            return this;
        }
    }
}