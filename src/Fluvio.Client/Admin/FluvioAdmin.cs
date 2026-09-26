using System.Text;
using System.Text.Json;
using Fluvio.Client.Abstractions;
using Fluvio.Client.Interop;

namespace Fluvio.Client.Admin;

/// <summary>
/// Fluvio admin implementation for topic management. Backed by the native Rust FFI layer
/// (see <see cref="Interop"/>), which wraps the official <c>fluvio</c> Rust client's
/// <c>FluvioAdmin</c>.
/// </summary>
internal sealed class FluvioAdmin : IFluvioAdmin
{
    private readonly RustResource _clientHandle;

    /// <summary>
    /// Initializes a new instance of the <see cref="FluvioAdmin"/> class.
    /// </summary>
    /// <param name="clientHandle">The native client handle admin operations are issued against.</param>
    public FluvioAdmin(RustResource clientHandle)
    {
        _clientHandle = clientHandle;
    }

    /// <summary>
    /// Creates a new topic in the Fluvio cluster.
    /// </summary>
    /// <param name="name">Topic name.</param>
    /// <param name="spec">Topic specification (optional, defaults to 1 partition and replication factor 1).</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="ArgumentException">Thrown when topic name is invalid</exception>
    public async Task CreateTopicAsync(string name, TopicSpec? spec = null, CancellationToken cancellationToken = default)
    {
        ValidateTopicName(name);
        spec ??= new TopicSpec();

        var nameBytes = Encoding.UTF8.GetBytes(name);
        var specJson = BuildTopicSpecJson(spec);
        var specBytes = Encoding.UTF8.GetBytes(specJson);

        Task<nint> task;
        unsafe
        {
            fixed (byte* np = nameBytes)
            fixed (byte* sp = specBytes)
            {
                var nameAddr = (nint)np;
                var specAddr = (nint)sp;
                task = _clientHandle.RunAsyncWithIncrement(h =>
                    Callbacks.CallAsync(tcb =>
                    {
                        unsafe
                        {
                            Native.AdminCreateTopic(h, (byte*)nameAddr, (nuint)nameBytes.Length, (byte*)specAddr, (nuint)specBytes.Length, tcb);
                        }
                    }));
            }
        }
        await task.ConfigureAwait(false);
    }

    /// <summary>
    /// Deletes a topic from the Fluvio cluster.
    /// </summary>
    /// <param name="name">Topic name.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <exception cref="ArgumentException">Thrown when topic name is invalid</exception>
    public async Task DeleteTopicAsync(string name, CancellationToken cancellationToken = default)
    {
        ValidateTopicName(name);
        var nameBytes = Encoding.UTF8.GetBytes(name);

        Task<nint> task;
        unsafe
        {
            fixed (byte* np = nameBytes)
            {
                var nameAddr = (nint)np;
                task = _clientHandle.RunAsyncWithIncrement(h =>
                    Callbacks.CallAsync(tcb =>
                    {
                        unsafe
                        {
                            Native.AdminDeleteTopic(h, (byte*)nameAddr, (nuint)nameBytes.Length, tcb);
                        }
                    }));
            }
        }
        await task.ConfigureAwait(false);
    }

    /// <summary>
    /// Lists all topics in the Fluvio cluster.
    /// </summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>List of topic metadata.</returns>
    public async Task<IReadOnlyList<TopicMetadata>> ListTopicsAsync(CancellationToken cancellationToken = default)
    {
        var jsonPtr = await _clientHandle.RunAsyncWithIncrement(h =>
            Callbacks.CallAsync(tcb => Native.AdminListTopics(h, tcb))).ConfigureAwait(false);
        var json = Native.ReadAndFreeString(jsonPtr);
        if (string.IsNullOrEmpty(json))
        {
            return [];
        }

        using var document = JsonDocument.Parse(json);
        var topics = new List<TopicMetadata>();
        foreach (var element in document.RootElement.EnumerateArray())
        {
            topics.Add(ParseTopicMetadata(element));
        }
        return topics;
    }

    /// <summary>
    /// Gets metadata for a specific topic.
    /// </summary>
    /// <param name="name">Topic name.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Topic metadata or null if not found.</returns>
    public async Task<TopicMetadata?> GetTopicAsync(string name, CancellationToken cancellationToken = default)
    {
        var nameBytes = Encoding.UTF8.GetBytes(name);

        Task<nint> task;
        unsafe
        {
            fixed (byte* np = nameBytes)
            {
                var nameAddr = (nint)np;
                task = _clientHandle.RunAsyncWithIncrement(h =>
                    Callbacks.CallAsync(tcb =>
                    {
                        unsafe
                        {
                            Native.AdminGetTopic(h, (byte*)nameAddr, (nuint)nameBytes.Length, tcb);
                        }
                    }));
            }
        }
        var jsonPtr = await task.ConfigureAwait(false);

        var json = Native.ReadAndFreeString(jsonPtr);
        if (string.IsNullOrEmpty(json))
        {
            return null;
        }

        using var document = JsonDocument.Parse(json);
        return ParseTopicMetadata(document.RootElement);
    }

    /// <summary>
    /// Parses the JSON shape produced by the native <c>ffi_admin_list_topics</c>/
    /// <c>ffi_admin_get_topic</c> functions in <c>native/fluvio-dotnet/src/admin.rs</c>.
    /// Built with <see cref="JsonDocument"/> rather than reflection-based
    /// <see cref="JsonSerializer"/> so this stays trim/AOT compatible.
    /// </summary>
    private static TopicMetadata ParseTopicMetadata(JsonElement element) =>
        new(
            Name: element.GetProperty("name").GetString() ?? "",
            Partitions: element.GetProperty("partitions").GetInt32(),
            ReplicationFactor: element.GetProperty("replicationFactor").GetInt32(),
            Status: ToApiStatus(element.GetProperty("status").GetString() ?? ""),
            PartitionMetadata: []);

    /// <summary>
    /// Builds the topic-spec JSON consumed by the native <c>ffi_admin_create_topic</c>
    /// function, which reads the <c>partitions</c>/<c>replicationFactor</c> fields.
    /// </summary>
    private static string BuildTopicSpecJson(TopicSpec spec)
    {
        using var stream = new MemoryStream();
        using (var writer = new Utf8JsonWriter(stream))
        {
            writer.WriteStartObject();
            writer.WriteNumber("partitions", spec.Partitions);
            writer.WriteNumber("replicationFactor", spec.ReplicationFactor);
            writer.WriteEndObject();
        }
        return Encoding.UTF8.GetString(stream.ToArray());
    }

    /// <summary>
    /// Maps the native Rust side's <c>TopicResolution</c> debug-formatted variant name
    /// (Init/Pending/InsufficientResources/InvalidConfig/Provisioned/Deleting) to the public
    /// <see cref="TopicStatus"/> surface.
    /// </summary>
    private static TopicStatus ToApiStatus(string resolution) =>
        resolution switch
        {
            "Provisioned" => TopicStatus.Provisioned,
            _ => TopicStatus.Offline,
        };

    /// <summary>
    /// Validates a topic name according to Fluvio rules.
    /// Topic names must:
    /// - Be 63 characters or less
    /// - Contain only lowercase alphanumeric characters or '-'
    /// - Not start or end with '-'
    /// </summary>
    /// <param name="name">Topic name to validate</param>
    /// <exception cref="ArgumentException">Thrown when topic name is invalid</exception>
    private static void ValidateTopicName(string name)
    {
        const int maxResourceNameLen = 63;

        if (string.IsNullOrEmpty(name))
        {
            throw new ArgumentException("Topic name cannot be null or empty", nameof(name));
        }

        if (name.Length > maxResourceNameLen)
        {
            throw new ArgumentException(
                $"Invalid topic name: '{name}'. Name exceeds max characters allowed {maxResourceNameLen}",
                nameof(name));
        }

        if (name.StartsWith('-') || name.EndsWith('-'))
        {
            throw new ArgumentException(
                $"Invalid topic name: '{name}'. Name cannot start or end with '-'",
                nameof(name));
        }

        foreach (var ch in name)
        {
            if (!char.IsAsciiLetterLower(ch) && !char.IsAsciiDigit(ch) && ch != '-')
            {
                throw new ArgumentException(
                    $"Invalid topic name: '{name}'. Name can only contain lowercase alphanumeric characters or '-'",
                    nameof(name));
            }
        }
    }

    /// <summary>
    /// Disposes the admin instance. (No-op, does not own the client handle.)
    /// </summary>
    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
