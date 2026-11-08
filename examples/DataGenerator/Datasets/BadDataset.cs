namespace Streamiz.Examples.DataGenerator.Datasets;

/// <summary>
/// Malformed records for the error-handling demos: values that can't be deserialized as a banking transaction.
/// Unlike the other files, each line is a wrapper {key, value, timestamp} whose value is the raw (broken) payload.
/// </summary>
public static class BadDataset
{
    public static readonly RawRecord[] Transactions =
    {
        new("A0005", "{\"transactionId\":\"X000001\",\"accountId\":\"A0005\",\"amount\":12.5", Json.Utc(2026, 1, 5, 10, 0)),
        new("A0011", "{\"transactionId\":\"X000002\",\"accountId\":\"A0011\",\"amount\":\"twelve\",\"currency\":\"EUR\"}", Json.Utc(2026, 1, 5, 11, 30)),
        new("A0023", "not json at all", Json.Utc(2026, 1, 5, 12, 15)),
        new("A0031", "42", Json.Utc(2026, 1, 5, 13, 45)),
        new("A0047", "[\"A0047\", 18.20, \"EUR\"]", Json.Utc(2026, 1, 5, 16, 20)),
        new("A0058", "{\"transactionId\":\"X000006\",\"accountId\":\"A0058\",\"amount\":19.9,\"timestamp\":\"yesterday\"}", Json.Utc(2026, 1, 6, 8, 5)),
        new("A0064", "<transaction><amount>10</amount></transaction>", Json.Utc(2026, 1, 6, 14, 50)),
        new("A0072", "{\"transactionId\":\"X000008\",\"accountId\":\"A0072\",\"amount\":1e999}", Json.Utc(2026, 1, 6, 19, 30)),
    };

    public static void Write(string dataDir, int seed)
    {
        var dir = Path.Combine(dataDir, "bad");
        Json.WriteLines(Path.Combine(dir, "transactions-malformed.jsonl"), Transactions);
        Json.WriteFacts(Path.Combine(dir, "facts.json"), new
        {
            records = Transactions.Length,
            topic = "banking.transactions",
            note = "each line is {key, value, timestamp}: produce 'value' as the raw record value"
        });
    }
}
