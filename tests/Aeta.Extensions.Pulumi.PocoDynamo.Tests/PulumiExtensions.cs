using System.Threading.Tasks;
using Pulumi;

namespace Aeta.Extensions.Pulumi.PocoDynamo.Tests
{
    public static class PulumiExtensions
    {
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