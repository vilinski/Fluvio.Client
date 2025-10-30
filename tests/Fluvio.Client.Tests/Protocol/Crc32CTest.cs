using System.Text;
using Fluvio.Client.Protocol;
using Xunit;
using Xunit.Abstractions;

namespace Fluvio.Client.Tests.Protocol;

public class Crc32CTest
{
    private readonly ITestOutputHelper _output;

    public Crc32CTest(ITestOutputHelper output)
    {
        _output = output;
    }

    [Fact]
    public void TestKnownVector()
    {
        // Known test vector: CRC32C of "123456789" should be 0xE3069283
        var data = Encoding.ASCII.GetBytes("123456789");
        var crc = Crc32C.Compute(data);

        _output.WriteLine($"Data: 123456789");
        _output.WriteLine($"Calculated CRC: 0x{crc:X8}");
        _output.WriteLine($"Expected CRC:   0xE3069283");

        Assert.Equal(0xE3069283u, crc);
    }
}
