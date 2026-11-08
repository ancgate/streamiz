using System.Text;
using NUnit.Framework;

namespace Streamiz.Examples.Testing.Tests;

/// <summary>
/// Error handling is part of the topology's behaviour: test it like any output, by reading the dead letter queue.
/// </summary>
public class ErrorHandlingTests
{
    [Test]
    public void BadRecordsGoToTheDeadLetterQueueAndProcessingContinues()
    {
        using var driver = Drivers.Create(SafeAmountParser.Build, SafeAmountParser.Configure);
        var raw = driver.CreateInputTopic<string, string>(SafeAmountParser.Input);
        var amounts = driver.CreateOutputTopic<string, string>(SafeAmountParser.Output);
        var deadLetters = driver.CreateOutputTopic<string, string>(SafeAmountParser.DeadLetterQueue);

        raw.PipeInput("t-1", "12.34");
        raw.PipeInput("t-2", "twelve");
        raw.PipeInput("t-3", "5");

        CollectionAssert.AreEqual(new[] { "1234", "500" }, amounts.ReadValueList().ToList());

        var dead = deadLetters.ReadKeyValue();
        Assert.AreEqual("t-2", dead.Message.Key);
        Assert.AreEqual("twelve", dead.Message.Value);
        Assert.AreEqual(typeof(FormatException).FullName,
            Encoding.UTF8.GetString(dead.Message.Headers.GetLastBytes("__streams.errors.exception")));
        Assert.AreEqual("raw-amounts",
            Encoding.UTF8.GetString(dead.Message.Headers.GetLastBytes("__streams.errors.topic")));
    }

    [Test]
    public void WithoutAHandlerTheExceptionReachesTheTest()
    {
        // in SYNC_TASK mode an exception thrown by the topology is thrown by PipeInput
        using var driver = Drivers.Create(SafeAmountParser.Build);
        var raw = driver.CreateInputTopic<string, string>(SafeAmountParser.Input);

        Assert.Throws<FormatException>(() => raw.PipeInput("t-2", "twelve"));
    }
}
