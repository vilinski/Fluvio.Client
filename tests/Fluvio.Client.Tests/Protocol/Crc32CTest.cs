using System.Text;
using Fluvio.Client.Protocol;
using Xunit.Abstractions;

namespace Fluvio.Client.Tests.Protocol;

public class Crc32CTest(ITestOutputHelper output)
{
    [Fact]
    public void TestKnownVector()
    {
        // Known test vector: CRC32C of "123456789" should be 0xE3069283
        var data = Encoding.ASCII.GetBytes("123456789");
        var crc = Crc32C.Compute(data);

        output.WriteLine($"Data: 123456789");
        output.WriteLine($"Calculated CRC: 0x{crc:X8}");
        output.WriteLine($"Expected CRC:   0xE3069283");

        Assert.Equal(0xE3069283u, crc);
    }
}
