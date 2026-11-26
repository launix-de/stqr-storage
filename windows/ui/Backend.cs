// SPDX-License-Identifier: GPL-3.0-only
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Stqr.UI;

public sealed record MountConfig
{
    public string URI { get; set; } = "http://127.0.0.1:7419";
    public string Username { get; set; } = "";
    public string Password { get; set; } = "";
    public string Bucket { get; set; } = "";
    public string Drive { get; set; } = "S:";
    [JsonPropertyName("readonly")] public bool ReadOnly { get; set; }
    [JsonPropertyName("lower_bucket")] public string LowerBucket { get; set; } = "";
    public string[]? Whitelist { get; set; }
    public uint? UID { get; set; }
    public uint? GID { get; set; }
}
public sealed record Request(string Action, string? Drive = null, MountConfig? Config = null, bool Persist = false, bool Initialize = false)
{ public int Version => 1; }
public sealed record Profile(string Drive, string Bucket, bool Running)
{ public string Label => $"{Drive}  ·  {Bucket}"; }
public sealed record Reply(int Version, string Type, string? Message, MountConfig? Config, Profile[]? Profiles, bool Running, bool Saved, bool WinFsp, bool ReadOnly = false);

public sealed class Backend(string executable)
{
    private static readonly JsonSerializerOptions JSON = new() { PropertyNamingPolicy = JsonNamingPolicy.CamelCase, PropertyNameCaseInsensitive = true };
    private Process Start()
    {
        var p = new Process { StartInfo = new ProcessStartInfo(executable)
        {
            UseShellExecute = false, CreateNoWindow = true,
            RedirectStandardInput = true, RedirectStandardOutput = true, RedirectStandardError = true,
            StandardInputEncoding = new UTF8Encoding(false), StandardOutputEncoding = Encoding.UTF8
        }};
        p.StartInfo.ArgumentList.Add("--ui-rpc");
        p.Start(); return p;
    }
    public async IAsyncEnumerable<Reply> Stream(Request request, [EnumeratorCancellation] CancellationToken cancel = default)
    {
        using var process = Start();
        var errors = DrainErrors(process.StandardError);
        await process.StandardInput.WriteAsync(JsonSerializer.Serialize(request, JSON));
        process.StandardInput.Close();
        bool reported = false;
        while (await process.StandardOutput.ReadLineAsync(cancel) is { } line)
        {
            if (line.Length > 65536) throw new InvalidDataException("Ungültige Antwort des STQR-Clients.");
            Reply? reply;
            try { reply = JsonSerializer.Deserialize<Reply>(line, JSON); }
            catch (JsonException) { continue; }
            if (reply is null || reply.Version != 1) continue;
            reported = true;
            yield return reply;
        }
        await process.WaitForExitAsync(cancel);
        await errors;
        if (process.ExitCode != 0 && !reported)
            throw new InvalidOperationException("Der STQR-Client konnte nicht gestartet werden.");
    }
    public async Task<Reply> Send(Request request)
    {
        Reply? result = null;
        await foreach (var reply in Stream(request)) result = reply;
        if (result?.Type == "error") throw new InvalidOperationException(result.Message);
        return result ?? throw new InvalidDataException("Keine Antwort des STQR-Clients.");
    }
    private static async Task DrainErrors(StreamReader reader)
    {
        var buffer = new char[2048];
        while (await reader.ReadAsync(buffer) != 0) { }
    }
}
