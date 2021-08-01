using System;
using System.Collections.Generic;
using System.Linq;
using Pulumi.Aws.DynamoDB;
using Pulumi.Aws.DynamoDB.Inputs;
using ServiceStack;
using ServiceStack.Aws.DynamoDb;

namespace Aeta.Extensions.Pulumi.PocoDynamo
{
    public class TableGroup
    {
        private readonly IEnumerable<DynamoMetadataType> _metadatas;

        public string TableName { get; }
        
        public TableGroup(string tableName, IEnumerable<Type> types)
        {
            TableName = tableName;
            _metadatas = types.Select(DynamoMetadata.RegisterTable).ToList();
        }

        public void SetTableAttributes(TableArgs tableArgs)
        {
            tableArgs.Attributes = _metadatas
                .SelectMany(metadata => metadata.Fields)
                .DistinctBy(field => field.Name)
                .Select(field => new TableAttributeArgs
                {
                    Name = field.Name,
                    Type = field.DbType
                }).ToList();
        }

        public void SetBillingMode(TableArgs tableArgs)
        {
            var metadata = _metadatas.FirstOrDefault(table => table.ReadCapacityUnits.HasValue);
            if (metadata is null)
            {
                tableArgs.BillingMode = "PAY_PER_REQUEST";
            }
            else
            {
                tableArgs.BillingMode = "PROVISIONED";
                tableArgs.ReadCapacity = metadata.ReadCapacityUnits;
                tableArgs.WriteCapacity = metadata.ReadCapacityUnits;
            }
        }

        public void SetPrimaryKey(TableArgs tableArgs)
        {
            var metadata = _metadatas.FirstOrDefault(table => table.HashKey is not null);
            if (metadata is null)
                throw new ArgumentException("Could not find a property with HashKey attribute in this table group.");

            tableArgs.HashKey = metadata.HashKey.Name;
            tableArgs.RangeKey = metadata.RangeKey?.Name;
        }

        public void SetGlobalSecondaryIndexes(TableArgs tableArgs)
        {
            var metadata = _metadatas.FirstOrDefault(table => !table.GlobalIndexes.IsEmpty());
            if (metadata is null) return;

            tableArgs.GlobalSecondaryIndexes = metadata.GlobalIndexes.Select(index =>
                new TableGlobalSecondaryIndexArgs()
                {
                    Name = index.Name,
                    HashKey = index.HashKey.Name,
                    RangeKey = index.RangeKey?.Name,
                    ProjectionType = index.ProjectionType,
                    NonKeyAttributes = index.ProjectedFields.Safe().ToList(),
                    ReadCapacity = (int?) index.ReadCapacityUnits,
                    WriteCapacity = (int?) index.WriteCapacityUnits
                }).ToList();
        }
    }
}