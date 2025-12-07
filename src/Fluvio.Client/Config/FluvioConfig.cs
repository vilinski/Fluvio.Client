namespace Fluvio.Client.Config;

/// <summary>
/// Fluvio configuration file reader.
/// Reads from ~/.fluvio/config (TOML format).
/// </summary>
internal static class FluvioConfig
{
    private static readonly string ConfigPath = Path.Combine(
        Environment.GetFolderPath(Environment.SpecialFolder.UserProfile),
        ".fluvio",
        "config"
    );

    /// <summary>
    /// Load Fluvio configuration from ~/.fluvio/config
    /// Returns null if file doesn't exist.
    /// </summary>
    public static FluvioConfigFile? Load()
    {
        if (!File.Exists(ConfigPath))
            return null;

        try
        {
            var content = File.ReadAllText(ConfigPath);
            return ParseToml(content);
        }
        catch
        {
            return null; // Silently fail if config can't be read
        }
    }

    /// <summary>
    /// Simple TOML parser for Fluvio config.
    /// Only parses the subset we need (current_profile, profiles, clusters).
    /// </summary>
    private static FluvioConfigFile ParseToml(string content)
    {
        var config = new FluvioConfigFile();
        var lines = content.Split('\n');
        string? currentSection = null;
        string? currentSubsection = null;

        foreach (var line in lines)
        {
            var trimmed = line.Trim();
            if (string.IsNullOrEmpty(trimmed) || trimmed.StartsWith('#'))
                continue;

            // Section headers: [section.name]
            if (trimmed.StartsWith('[') && trimmed.EndsWith(']'))
            {
                var section = trimmed.Trim('[', ']');
                var parts = section.Split('.');

                currentSection = parts[0];
                currentSubsection = parts.Length > 1 ? parts[1] : null;

                if (currentSection == "profile" && currentSubsection != null)
                {
                    if (!config.Profiles.ContainsKey(currentSubsection))
                        config.Profiles[currentSubsection] = new FluvioProfile();
                }
                else if (currentSection == "cluster" && currentSubsection != null)
                {
                    if (!config.Clusters.ContainsKey(currentSubsection))
                        config.Clusters[currentSubsection] = new FluvioCluster();
                }

                continue;
            }

            // Key-value pairs
            var kvp = trimmed.Split('=', 2);
            if (kvp.Length != 2)
                continue;

            var key = kvp[0].Trim();
            var value = kvp[1].Trim().Trim('"');

            // Top-level properties
            if (currentSection == null)
            {
                if (key == "current_profile")
                    config.CurrentProfile = value;
                continue;
            }

            // Profile properties
            if (currentSection == "profile" && currentSubsection != null)
            {
                var profile = config.Profiles[currentSubsection];
                if (key == "cluster")
                    profile.Cluster = value;
                continue;
            }

            // Cluster properties
            if (currentSection == "cluster" && currentSubsection != null)
            {
                var cluster = config.Clusters[currentSubsection];
                if (key == "endpoint")
                    cluster.Endpoint = value;
                else if (key == "tls_policy")
                    cluster.TlsPolicy = value;
                else if (key == "use_spu_local_address")
                    cluster.UseSpuLocalAddress = value.ToLowerInvariant() == "true";
                continue;
            }
        }

        return config;
    }

    /// <summary>
    /// Get the active cluster configuration.
    /// Returns null if no active profile or cluster found.
    /// </summary>
    public static FluvioCluster? GetActiveCluster(string? profileName = null)
    {
        var config = Load();
        if (config == null)
            return null;

        // Use specified profile, or current profile, or default to "local"
        var profile = profileName ?? config.CurrentProfile ?? "local";

        if (!config.Profiles.TryGetValue(profile, out var profileData))
            return null;

        if (profileData.Cluster == null)
            return null;

        return config.Clusters.GetValueOrDefault(profileData.Cluster);
    }
}

/// <summary>
/// Fluvio configuration file structure
/// </summary>
internal class FluvioConfigFile
{
    public string? CurrentProfile { get; set; }
    public Dictionary<string, FluvioProfile> Profiles { get; } = new();
    public Dictionary<string, FluvioCluster> Clusters { get; } = new();
}

/// <summary>
/// Fluvio profile
/// </summary>
internal class FluvioProfile
{
    public string? Cluster { get; set; }
}

/// <summary>
/// Fluvio cluster configuration
/// </summary>
internal class FluvioCluster
{
    public string? Endpoint { get; set; }
    public string? TlsPolicy { get; set; }
    public bool UseSpuLocalAddress { get; set; }

    public bool IsTlsEnabled => TlsPolicy?.ToLowerInvariant() != "disabled";
}
