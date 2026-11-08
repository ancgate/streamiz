using System;
using System.Linq;
using Microsoft.Extensions.Logging;
using NUnit.Framework;
using Streamiz.Kafka.Net.Mock;
using Streamiz.Kafka.Net.SerDes;
using StreamizLogger = Streamiz.Kafka.Net.Crosscutting.Logger;

namespace Streamiz.Kafka.Net.Tests.Private
{
    /// <summary>
    /// The logger factory of a stream's configuration must not break later streams once it's disposed.
    /// </summary>
    public class LoggerFactoryLifetimeTests
    {
        [TearDown]
        public void ResetLoggerFactory() => StreamizLogger.LoggerFactory = null;

        private static string RunUpperCase(ILoggerFactory loggerFactory)
        {
            var builder = new StreamBuilder();
            builder.Stream<string, string>("input").MapValues((v, _) => v.ToUpper()).To("output");

            var config = new StreamConfig<StringSerDes, StringSerDes> {ApplicationId = $"test-logger-{Guid.NewGuid()}"};
            if (loggerFactory != null)
                config.Logger = loggerFactory;

            using var driver = new TopologyTestDriver(builder.Build(), config);
            driver.CreateInputTopic<string, string>("input").PipeInput("k", "value");
            return driver.CreateOutputTopic<string, string>("output").ReadValueList().Single();
        }

        [Test]
        public void ADisposedLoggerFactoryDoesNotBreakTheNextStream()
        {
            var loggerFactory = LoggerFactory.Create(_ => { });
            Assert.AreEqual("VALUE", RunUpperCase(loggerFactory));
            loggerFactory.Dispose();

            Assert.AreEqual("VALUE", RunUpperCase(null));
        }

        [Test]
        public void ADisposedLoggerFactoryInUseFallsBackToTheDefaultOne()
        {
            var loggerFactory = LoggerFactory.Create(_ => { });
            StreamizLogger.LoggerFactory = loggerFactory;
            loggerFactory.Dispose();

            Assert.DoesNotThrow(() => StreamizLogger.GetLogger(typeof(LoggerFactoryLifetimeTests)));
            Assert.AreEqual("VALUE", RunUpperCase(null));
        }

        [Test]
        public void DisposingADriverPutsBackThePreviousLoggerFactory()
        {
            var first = LoggerFactory.Create(_ => { });
            var second = LoggerFactory.Create(_ => { });
            StreamizLogger.LoggerFactory = first;

            RunUpperCase(second);

            Assert.AreSame(first, StreamizLogger.LoggerFactory);
            first.Dispose();
            second.Dispose();
        }
    }
}
