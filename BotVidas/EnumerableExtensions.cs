using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BotVidas
{
    public static class EnumerableExtensions
    {
        public static IEnumerable<IEnumerable<T>> Chunk<T>(this IEnumerable<T> source, int size)
        {
            if (size <= 0)
                throw new ArgumentException("El tamaño del lote debe ser mayor que 0.", nameof(size));

            using (var enumerator = source.GetEnumerator())
            {
                while (enumerator.MoveNext())
                {
                    yield return YieldChunkElements(enumerator, size - 1);
                }
            }
        }

        private static IEnumerable<T> YieldChunkElements<T>(IEnumerator<T> source, int size)
        {
            yield return source.Current;

            for (int i = 0; i < size && source.MoveNext(); i++)
            {
                yield return source.Current;
            }
        }
    }

}
