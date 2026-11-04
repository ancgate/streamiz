using System;
using System.Globalization;
using System.Linq;
using System.Text;
using Confluent.Kafka;
using NUnit.Framework;
using Streamiz.Kafka.Net.Errors;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using Streamiz.Kafka.Net.State;
using Streamiz.Kafka.Net.Stream;
using Streamiz.Kafka.Net.Table;

namespace Streamiz.Kafka.Net.Tests.Processors;

public class KTableSuppressTests
{
    private StreamConfig streamConfig;
    private DateTime dt;

    [SetUp]
    public void Init()
    {
        streamConfig = new StreamConfig();
        streamConfig.ApplicationId = "test-ktable-suppress-test";
        
        var c = CultureInfo.CreateSpecificCulture("fr-FR");
        dt = DateTime.Parse("15/10/2024 08:00:00", c);
    }
    
    private StreamBuilder BuildTopology(Suppressed<Windowed<string>, long> suppressed)
    {
        var builder = new StreamBuilder();
        
        builder.Stream("input", new StringSerDes(), new StringSerDes())
            .GroupByKey()
            .WindowedBy(TumblingWindowOptions.Of(TimeSpan.FromMinutes(1)))
            .Count(
                InMemoryWindows
                    .As<string, long>("count-store")
                    .WithKeySerdes(new StringSerDes())
                    .WithValueSerdes(new Int64SerDes()))
            .Suppress(suppressed
                .WithKeySerdes(new TimeWindowedSerDes<string>(new StringSerDes(), (long)TimeSpan.FromMinutes(1).TotalMilliseconds))
                .WithValueSerdes(new Int64SerDes()))
            .ToStream()
            .To("output", 
                new TimeWindowedSerDes<string>(new StringSerDes(), (long)TimeSpan.FromMinutes(1).TotalMilliseconds), 
                new Int64SerDes());

        return builder;
    }
    
    [Test]
    public void WindowedZeroTimeLimitShouldImmediatelyEmit()
    {
        var builder =
            BuildTopology(
                SuppressedBuilder.UntilTimeLimit<Windowed<string>, long>(TimeSpan.Zero,
                    StrictBufferConfig.Unbounded().WithLoggingEnabled(null)));
        
        using var driver = new TopologyTestDriver(builder.Build(), streamConfig);
        var inputTopic = driver.CreateInputTopic("input", new StringSerDes(), new StringSerDes());
        var outputTopic = driver.CreateOutputTopic("output",
            TimeSpan.FromSeconds(5),
            new TimeWindowedSerDes<string>(new StringSerDes(), (long)TimeSpan.FromMinutes(1).TotalMilliseconds),
            new Int64SerDes());
        
        inputTopic.PipeInput("key1", "value1", DateTime.Now);
        inputTopic.PipeInput("key1", "value2", DateTime.Now);

        var records = outputTopic.ReadKeyValueList();
        Assert.AreEqual(2, records.Count());
    }

    [Test]
    public void IntermediateSuppressionShouldBufferAndEmitLater()
    {
        var builder =
            BuildTopology(
                SuppressedBuilder.UntilTimeLimit<Windowed<string>, long>(TimeSpan.FromSeconds(1),
                    StrictBufferConfig.Unbounded().WithLoggingDisabled()));
        
        using var driver = new TopologyTestDriver(builder.Build(), streamConfig);
        var inputTopic = driver.CreateInputTopic("input", new StringSerDes(), new StringSerDes());
        var outputTopic = driver.CreateOutputTopic("output",
            TimeSpan.FromSeconds(5),
            new TimeWindowedSerDes<string>(new StringSerDes(), (long)TimeSpan.FromMinutes(1).TotalMilliseconds),
            new Int64SerDes());
        
        inputTopic.PipeInput("key1", "value1", dt);
        inputTopic.PipeInput("key1", "value2", dt.AddSeconds(30)); // should process and emit

        var records = outputTopic.ReadKeyValueList();
        Assert.AreEqual(1, records.Count());
        Assert.AreEqual(2, records.First().Message.Value);
    }

    [Test]
    public void FinalResultsSuppressionShouldBufferAndEmitAtGraceExpiration()
    {
        var builder =
            BuildTopology(
                SuppressedBuilder.UntilWindowClose<Windowed<string>, long>(TimeSpan.FromMinutes(1),
                    StrictBufferConfig.Unbounded()));
        
        using var driver = new TopologyTestDriver(builder.Build(), streamConfig);
        var inputTopic = driver.CreateInputTopic("input", new StringSerDes(), new StringSerDes());
        var outputTopic = driver.CreateOutputTopic("output",
            TimeSpan.FromSeconds(5),
            new TimeWindowedSerDes<string>(new StringSerDes(), (long)TimeSpan.FromMinutes(1).TotalMilliseconds),
            new Int64SerDes());
        
        inputTopic.PipeInput("key1", "value1", dt);
        
        // although the stream time is now 15/10/2024 08:01:00, we have to wait 1 minutes after the window *end* before we
        // emit "key1", so we don't emit yet.
        inputTopic.PipeInput("key2", "value1", dt.AddMinutes(1));
        
        // ok, now it's time to emit "key1"
        inputTopic.PipeInput("key3", "value1", dt.AddMinutes(2).AddSeconds(1));
        
        var records = outputTopic.ReadKeyValueList();
        Assert.AreEqual(1, records.Count());
        Assert.AreEqual("key1", records.First().Message.Key.Key);
        Assert.AreEqual(1, records.First().Message.Value);
    }

    [Test]
    public void FinalResultsWithZeroGraceShouldStillBufferUntilTheWindowEnd()
    {
        var builder =
            BuildTopology(
                SuppressedBuilder.UntilWindowClose<Windowed<string>, long>(TimeSpan.Zero,
                    StrictBufferConfig.Unbounded()));
        
        using var driver = new TopologyTestDriver(builder.Build(), streamConfig);
        var inputTopic = driver.CreateInputTopic("input", new StringSerDes(), new StringSerDes());
        var outputTopic = driver.CreateOutputTopic("output",
            TimeSpan.FromSeconds(5),
            new TimeWindowedSerDes<string>(new StringSerDes(), (long)TimeSpan.FromMinutes(1).TotalMilliseconds),
            new Int64SerDes());
        
        inputTopic.PipeInput("key1", "value1", dt);
        
        // although the stream time is now 15/10/2024 08:01:00, we have to wait 1 minutes after the window *end* before we
        // emit "key1", so we don't emit yet.
        inputTopic.PipeInput("key2", "value1", dt.AddMinutes(1));
        
        var records = outputTopic.ReadKeyValueList();
        Assert.AreEqual(1, records.Count());
        Assert.AreEqual("key1", records.First().Message.Key.Key);
        Assert.AreEqual(1, records.First().Message.Value);
    }

    [Test]
    public void FinalResultsWithZeroGraceAtWindowEndShouldImmediatelyEmit()
    {
        var builder =
            BuildTopology(
                SuppressedBuilder.UntilWindowClose<Windowed<string>, long>(TimeSpan.Zero,
                    StrictBufferConfig.Unbounded()));
        
        using var driver = new TopologyTestDriver(builder.Build(), streamConfig);
        var inputTopic = driver.CreateInputTopic("input", new StringSerDes(), new StringSerDes());
        var outputTopic = driver.CreateOutputTopic("output",
            TimeSpan.FromSeconds(5),
            new TimeWindowedSerDes<string>(new StringSerDes(), (long)TimeSpan.FromMinutes(1).TotalMilliseconds),
            new Int64SerDes());
        
        inputTopic.PipeInput("key1", "value1", dt);
        
        // although the stream time is now 15/10/2024 08:01:00, we have to wait 1 minutes after the window *end* before we
        // emit "key1", so we don't emit yet.
        inputTopic.PipeInput("key2", "value1", dt.AddMinutes(1));
        
        var records = outputTopic.ReadKeyValueList();
        Assert.AreEqual(1, records.Count());
        Assert.AreEqual("key1", records.First().Message.Key.Key);
        Assert.AreEqual(1, records.First().Message.Value);
    }

    [Test]
    public void SuppressShouldEmitWhenOverRecordCapacity()
    {
        var builder =
            BuildTopology(
                SuppressedBuilder.UntilWindowClose<Windowed<string>, long>(TimeSpan.FromDays(100),
                    EagerConfig.EmitEarlyWhenFull(1)));
        
        using var driver = new TopologyTestDriver(builder.Build(), streamConfig);
        var inputTopic = driver.CreateInputTopic("input", new StringSerDes(), new StringSerDes());
        var outputTopic = driver.CreateOutputTopic("output",
            TimeSpan.FromSeconds(5),
            new TimeWindowedSerDes<string>(new StringSerDes(), (long)TimeSpan.FromMinutes(1).TotalMilliseconds),
            new Int64SerDes());
        
        inputTopic.PipeInput("key1", "value1", dt);
        inputTopic.PipeInput("dummy", "dummy", dt.AddMinutes(2));
        
        var records = outputTopic.ReadKeyValueList();
        Assert.AreEqual(1, records.Count());
        Assert.AreEqual("key1", records.First().Message.Key.Key);
        Assert.AreEqual(1, records.First().Message.Value);
    }

    [Test]
    public void SuppressShouldEmitWhenOverByteCapacity()
    {
        var builder =
            BuildTopology(
                SuppressedBuilder.UntilWindowClose<Windowed<string>, long>(TimeSpan.FromDays(100),
                    EagerConfig.EmitEarlyWhenFull(CacheSize.OfB(80)).WithLoggingDisabled()));
        
        using var driver = new TopologyTestDriver(builder.Build(), streamConfig);
        var inputTopic = driver.CreateInputTopic("input", new StringSerDes(), new StringSerDes());
        var outputTopic = driver.CreateOutputTopic("output",
            TimeSpan.FromSeconds(5),
            new TimeWindowedSerDes<string>(new StringSerDes(), (long)TimeSpan.FromMinutes(1).TotalMilliseconds),
            new Int64SerDes());
        
        inputTopic.PipeInput("key1", "value1", dt);
        inputTopic.PipeInput("dummy", "dummy", dt.AddMinutes(2));
        
        var records = outputTopic.ReadKeyValueList();
        Assert.AreEqual(1, records.Count());
        Assert.AreEqual("key1", records.First().Message.Key.Key);
        Assert.AreEqual(1, records.First().Message.Value);
    }
    
    [Test]
    public void SuppressShouldEmitWhenOverByteAndRecordCapacity()
    {
        var builder =
            BuildTopology(
                SuppressedBuilder.UntilWindowClose<Windowed<string>, long>(TimeSpan.FromDays(100),
                    EagerConfig.EmitEarlyWhenFull(3, CacheSize.OfB(120)).WithLoggingEnabled(null)));
        
        using var driver = new TopologyTestDriver(builder.Build(), streamConfig);
        var inputTopic = driver.CreateInputTopic("input", new StringSerDes(), new StringSerDes());
        var outputTopic = driver.CreateOutputTopic("output",
            TimeSpan.FromSeconds(5),
            new TimeWindowedSerDes<string>(new StringSerDes(), (long)TimeSpan.FromMinutes(1).TotalMilliseconds),
            new Int64SerDes());
        
        inputTopic.PipeInput("key1", "value1", dt);
        inputTopic.PipeInput("key1", "value2", dt);
        inputTopic.PipeInput("key1", "value3", dt);
        inputTopic.PipeInput("dummy", "dummy", dt.AddMinutes(2));
        
        var records = outputTopic.ReadKeyValueList();
        Assert.AreEqual(1, records.Count());
        Assert.AreEqual("key1", records.First().Message.Key.Key);
        Assert.AreEqual(3, records.First().Message.Value);
    }

    [Test]
    public void SuppressShouldShutDownWhenOverRecordCapacity()
    {
        var builder =
            BuildTopology(
                SuppressedBuilder.UntilWindowClose<Windowed<string>, long>(TimeSpan.FromDays(100),
                    StrictBufferConfig.Bounded(1)));
        
        using var driver = new TopologyTestDriver(builder.Build(), streamConfig);
        var inputTopic = driver.CreateInputTopic("input", new StringSerDes(), new StringSerDes());
        var outputTopic = driver.CreateOutputTopic("output",
            TimeSpan.FromSeconds(5),
            new TimeWindowedSerDes<string>(new StringSerDes(), (long)TimeSpan.FromMinutes(1).TotalMilliseconds),
            new Int64SerDes());
        
        inputTopic.PipeInput("key1", "value1", dt);

        Assert.Throws<StreamsException>(() => inputTopic.PipeInput("dummy", "dummy", dt.AddMinutes(2)));
    }

    [Test]
    public void SuppressShouldShutDownWhenOverByteCapacity()
    {
        var builder =
            BuildTopology(
                SuppressedBuilder.UntilWindowClose<Windowed<string>, long>(TimeSpan.FromDays(100),
                    StrictBufferConfig.Bounded(CacheSize.OfB(80))));
        
        using var driver = new TopologyTestDriver(builder.Build(), streamConfig);
        var inputTopic = driver.CreateInputTopic("input", new StringSerDes(), new StringSerDes());
        var outputTopic = driver.CreateOutputTopic("output",
            TimeSpan.FromSeconds(5),
            new TimeWindowedSerDes<string>(new StringSerDes(), (long)TimeSpan.FromMinutes(1).TotalMilliseconds),
            new Int64SerDes());
        
        inputTopic.PipeInput("key1", "value1", dt);

        Assert.Throws<StreamsException>(() => inputTopic.PipeInput("dummy", "dummy", dt.AddMinutes(2)));
    }
    
    [Test]
    public void EmitEndWindowWithHeaders()
    {
        var builder =
            BuildTopology(
                SuppressedBuilder.UntilWindowClose<Windowed<string>, long>(TimeSpan.Zero,
                    StrictBufferConfig.Unbounded()));
        
        using var driver = new TopologyTestDriver(builder.Build(), streamConfig);
        var inputTopic = driver.CreateInputTopic("input", new StringSerDes(), new StringSerDes());
        var outputTopic = driver.CreateOutputTopic("output",
            TimeSpan.FromSeconds(5),
            new TimeWindowedSerDes<string>(new StringSerDes(), (long)TimeSpan.FromMinutes(1).TotalMilliseconds),
            new Int64SerDes());

        Headers headers = new Headers();
        headers.Add("header", Encoding.UTF8.GetBytes("header-value"));
        
        inputTopic.PipeInput("key1", "value1", dt, headers);
        inputTopic.PipeInput("key1", "value2", dt.AddMinutes(1));

        var records = outputTopic.ReadKeyValueList().ToList();
        Assert.AreEqual(1, records.Count);
        Assert.AreEqual(1, records[0].Message.Headers.Count);
        Assert.AreEqual("key1", records[0].Message.Key.Key);
        Assert.AreEqual(1L, records[0].Message.Value);
        Assert.AreEqual("header", records[0].Message.Headers[0].Key);
    }

    #region Wall-clock time limit (KIP-424)

    private static readonly TimeSpan WallClockLimit = TimeSpan.FromMinutes(1);

    private StreamBuilder BuildTableTopology(Suppressed<string, string> suppressed)
    {
        var builder = new StreamBuilder();

        builder.Table("input", new StringSerDes(), new StringSerDes(),
                InMemory.As<string, string>("table-store"))
            .Suppress(suppressed
                .WithKeySerdes(new StringSerDes())
                .WithValueSerdes(new StringSerDes()))
            .ToStream()
            .To("output", new StringSerDes(), new StringSerDes());

        return builder;
    }

    private (TopologyTestDriver driver, TestInputTopic<string, string> input, TestOutputTopic<string, string> output)
        CreateWallClockDriver(IBufferConfig bufferConfig)
    {
        var builder = BuildTableTopology(
            SuppressedBuilder.UntilWallClockTimeLimit<string, string>(WallClockLimit, bufferConfig));
        var driver = new TopologyTestDriver(builder.Build(), streamConfig);
        var input = driver.CreateInputTopic("input", new StringSerDes(), new StringSerDes());
        var output = driver.CreateOutputTopic("output", TimeSpan.FromSeconds(5), new StringSerDes(), new StringSerDes());
        return (driver, input, output);
    }

    [Test]
    public void WallClockTimeLimitShouldNotEmitBeforeTheLimit()
    {
        var (driver, input, output) = CreateWallClockDriver(StrictBufferConfig.Unbounded());
        using (driver)
        {
            input.PipeInput("key1", "value1", dt);
            driver.AdvanceWallClockTime(TimeSpan.FromSeconds(30));

            Assert.AreEqual(0, output.ReadKeyValueList().Count());
        }
    }

    [Test]
    public void WallClockTimeLimitShouldEmitWithoutNewRecords()
    {
        var (driver, input, output) = CreateWallClockDriver(StrictBufferConfig.Unbounded());
        using (driver)
        {
            input.PipeInput("key1", "value1", dt);
            driver.AdvanceWallClockTime(WallClockLimit.Add(TimeSpan.FromSeconds(1)));

            var records = output.ReadKeyValueList().ToList();
            Assert.AreEqual(1, records.Count);
            Assert.AreEqual("key1", records[0].Message.Key);
            Assert.AreEqual("value1", records[0].Message.Value);
        }
    }

    [Test]
    public void WallClockTimeLimitShouldEmitOnlyTheLatestValueOfAKey()
    {
        var (driver, input, output) = CreateWallClockDriver(StrictBufferConfig.Unbounded().WithLoggingDisabled());
        using (driver)
        {
            input.PipeInput("key1", "value1", dt);
            input.PipeInput("key1", "value2", dt);
            input.PipeInput("key1", "value3", dt);
            driver.AdvanceWallClockTime(WallClockLimit.Add(TimeSpan.FromSeconds(1)));

            var records = output.ReadKeyValueList().ToList();
            Assert.AreEqual(1, records.Count);
            Assert.AreEqual("value3", records[0].Message.Value);
        }
    }

    [Test]
    public void WallClockTimeLimitShouldNotRestartTheTimerWhenAKeyIsUpdated()
    {
        var (driver, input, output) = CreateWallClockDriver(StrictBufferConfig.Unbounded());
        using (driver)
        {
            input.PipeInput("key1", "value1", dt);
            driver.AdvanceWallClockTime(TimeSpan.FromSeconds(40));
            input.PipeInput("key1", "value2", dt);
            driver.AdvanceWallClockTime(TimeSpan.FromSeconds(21));

            var records = output.ReadKeyValueList().ToList();
            Assert.AreEqual(1, records.Count);
            Assert.AreEqual("value2", records[0].Message.Value);
        }
    }

    [Test]
    public void WallClockTimeLimitShouldEmitEachKeyWhenItsOwnLimitIsReached()
    {
        var (driver, input, output) = CreateWallClockDriver(StrictBufferConfig.Unbounded());
        using (driver)
        {
            input.PipeInput("key1", "value1", dt);
            driver.AdvanceWallClockTime(TimeSpan.FromSeconds(30));
            input.PipeInput("key2", "value2", dt);
            driver.AdvanceWallClockTime(TimeSpan.FromSeconds(31));

            var records = output.ReadKeyValueList().ToList();
            Assert.AreEqual(1, records.Count);
            Assert.AreEqual("key1", records[0].Message.Key);

            driver.AdvanceWallClockTime(TimeSpan.FromSeconds(30));
            records = output.ReadKeyValueList().ToList();
            Assert.AreEqual(1, records.Count);
            Assert.AreEqual("key2", records[0].Message.Key);
        }
    }

    [Test]
    public void WallClockTimeLimitShouldIgnoreStreamTime()
    {
        var (driver, input, output) = CreateWallClockDriver(StrictBufferConfig.Unbounded());
        using (driver)
        {
            input.PipeInput("key1", "value1", dt);
            // stream time jumps far ahead, but the wall clock does not move
            input.PipeInput("key2", "value2", dt.AddHours(1));

            Assert.AreEqual(0, output.ReadKeyValueList().Count());
        }
    }

    [Test]
    public void WallClockTimeLimitShouldEmitEarlyWhenOverRecordCapacity()
    {
        var (driver, input, output) = CreateWallClockDriver(EagerConfig.EmitEarlyWhenFull(1));
        using (driver)
        {
            input.PipeInput("key1", "value1", dt);
            input.PipeInput("key2", "value2", dt);

            var records = output.ReadKeyValueList().ToList();
            Assert.AreEqual(1, records.Count);
            Assert.AreEqual("key1", records[0].Message.Key);

            driver.AdvanceWallClockTime(WallClockLimit.Add(TimeSpan.FromSeconds(1)));
            records = output.ReadKeyValueList().ToList();
            Assert.AreEqual(1, records.Count);
            Assert.AreEqual("key2", records[0].Message.Key);
        }
    }

    [Test]
    public void WallClockTimeLimitShouldShutDownWhenOverRecordCapacity()
    {
        var (driver, input, _) = CreateWallClockDriver(StrictBufferConfig.Bounded(1));
        using (driver)
        {
            input.PipeInput("key1", "value1", dt);
            Assert.Throws<StreamsException>(() => input.PipeInput("key2", "value2", dt));
        }
    }

    #endregion
}