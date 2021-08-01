using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using ServiceStack.DataAnnotations;

namespace Aeta.Extensions.Pulumi.PocoDynamo
{
    public static class ReflectionExtensions
    {
        public static IEnumerable<TableGroup> FindTables(this Assembly assembly)
        {
            return assembly.GetTypes()
                .Select(type => (type,
                    attribute: (AliasAttribute) Attribute.GetCustomAttribute(type, typeof(AliasAttribute))))
                .Where(metadata => metadata.attribute is not null)
                .GroupBy(metadata => metadata.attribute.Name)
                .Select(group => new TableGroup(group.Key, group.Select(g => g.type)));
        }
    }
}