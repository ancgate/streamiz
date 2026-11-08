using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Streamiz.Examples.DataGenerator;

/// <summary>JSON Lines output: one compact camelCase JSON object per line, UTF-8 without BOM, '\n' line endings.</summary>
public static class Json
{
    public static readonly JsonSerializerOptions Options = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        // keep '+', '<', '>', '\'' readable in the files
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping
    };

    private static readonly JsonSerializerOptions Indented = new(Options) { WriteIndented = true };

    private static readonly UTF8Encoding Utf8 = new(false);

    public static string Serialize<T>(T value) => JsonSerializer.Serialize(value, Options);

    public static void WriteLines<T>(string path, IEnumerable<T> records)
        => WriteRawLines(path, records.Select(Serialize));

    public static void WriteRawLines(string path, IEnumerable<string> lines)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        var sb = new StringBuilder();
        foreach (var line in lines)
            sb.Append(line).Append('\n');
        File.WriteAllText(path, sb.ToString(), Utf8);
    }

    public static void WriteFacts(string path, object facts)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        File.WriteAllText(path, JsonSerializer.Serialize(facts, Indented).Replace("\r\n", "\n") + "\n", Utf8);
    }

    /// <summary>A UTC time rounded to the second.</summary>
    public static DateTime Utc(int year, int month, int day, int hour = 0, int minute = 0, int second = 0)
        => new(year, month, day, hour, minute, second, DateTimeKind.Utc);
}
