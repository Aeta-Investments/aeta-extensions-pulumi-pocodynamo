using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Pulumi;
using ServiceStack.DataAnnotations;

namespace Aeta.Extensions.Pulumi.PocoDynamo
{
    public static class PlatformExtensions
    {
        public static IEnumerable<TableBuilder> FindPocoDynamoTableDefinitions(this Assembly assembly)
        {
            return assembly.GetTypes()
                .Select(type => (type,
                    attribute: (AliasAttribute) Attribute.GetCustomAttribute(type, typeof(AliasAttribute))))
                .Where(metadata => metadata.attribute is not null)
                .GroupBy(metadata => metadata.attribute.Name)
                .Select(group => new TableBuilder(group.Key, group.Select(g => g.type)));
        }
        
        public static T GetValue<T>(this Input<T> input)
        {
            var tcs = new TaskCompletionSource<T>();
            input.Apply(v =>
            {
                tcs.SetResult(v);
                return v;
            });
            return tcs.Task.Result;
        }
    }
}