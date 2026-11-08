using System.Text;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Examples.ProcessorApi.Examples;
using static Streamiz.Examples.ProcessorApi.Tests.ExampleDriver;

namespace Streamiz.Examples.ProcessorApi.Tests;

public class MaskCardNumbersTests
{
    [Test]
    public void CardNumbersAreMaskedAndHeadersKept()
    {
        using var driver = Create(new MaskCardNumbersExample());
        var payments = driver.CreateInputTopic<string, string>(MaskCardNumbersExample.PaymentsTopic);
        var output = driver.CreateOutputTopic<string, string>(MaskCardNumbersExample.OutputTopic);

        var headers = new Headers { { "source", Encoding.UTF8.GetBytes("web") } };
        payments.PipeInput("pay-1", "card=4111111111111111;amount=10.00", headers);

        var record = output.ReadKeyValue();
        Assert.AreEqual("pay-1", record.Message.Key);
        Assert.AreEqual("card=************1111;amount=10.00", record.Message.Value);
        Assert.AreEqual("web", Encoding.UTF8.GetString(record.Message.Headers.GetLastBytes("source")));
    }
}

public class RekeyByCountryTests
{
    [Test]
    public void EventsAreKeyedByCountry()
    {
        using var driver = Create(new RekeyByCountryExample());
        var events = driver.CreateInputTopic<string, string>(RekeyByCountryExample.EventsTopic);
        var output = driver.CreateOutputTopic<string, string>(RekeyByCountryExample.OutputTopic);

        events.PipeInput("u1", "FR;signup");
        events.PipeInput("u2", "DE;login");

        CollectionAssert.AreEqual(new[]
        {
            ("FR", "u1:signup"),
            ("DE", "u2:login")
        }, output.ReadAll());
    }
}

public class DeduplicateEventsTests
{
    [Test]
    public void DuplicatesWithinTenMinutesAreDropped()
    {
        using var driver = Create(new DeduplicateEventsExample());
        var events = driver.CreateInputTopic<string, string>(DeduplicateEventsExample.EventsTopic);
        var output = driver.CreateOutputTopic<string, string>(DeduplicateEventsExample.OutputTopic);

        events.PipeInput("evt-1", "paid 10.00", T0);
        events.PipeInput("evt-1", "paid 10.00", T0.AddMinutes(2));   // duplicate: dropped
        events.PipeInput("evt-2", "paid 25.00", T0.AddMinutes(3));
        events.PipeInput("evt-1", "paid 10.00", T0.AddMinutes(11));  // 11 minutes after the last one: kept

        CollectionAssert.AreEqual(new[]
        {
            ("evt-1", "paid 10.00"),
            ("evt-2", "paid 25.00"),
            ("evt-1", "paid 10.00")
        }, output.ReadAll());

        // The store remembers when each id was last forwarded.
        var seen = driver.GetKeyValueStore<string, long>(DeduplicateEventsExample.StoreName);
        Assert.AreEqual(new DateTimeOffset(T0.AddMinutes(11)).ToUnixTimeMilliseconds(), seen.Get("evt-1"));
    }
}
