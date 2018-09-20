namespace Hafslund.Akka.Persistence.Bigtable
{
    public interface ISerializerWrapper
    {
         byte[] ToBinary(object obj);

         T FromBinary<T>(byte[] bytes);
    }
}