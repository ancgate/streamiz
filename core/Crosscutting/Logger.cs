using System;
using Microsoft.Extensions.Logging;
using ILogger = Microsoft.Extensions.Logging.ILogger;
using ILoggerFactory = Microsoft.Extensions.Logging.ILoggerFactory;

namespace Streamiz.Kafka.Net.Crosscutting
{
    /// <summary>
    /// Helper logger to get logger from type class
    /// </summary>
    public static class Logger
    {
        private static readonly object factoryLock = new();
        private static ILoggerFactory _factory;
        private static ILoggerFactory _defaultFactory;

        // console logger, minimum level Information: used when no factory is set, or when the one set was disposed
        private static ILoggerFactory DefaultFactory
        {
            get
            {
                lock (factoryLock)
                {
                    return _defaultFactory ??= Microsoft.Extensions.Logging.LoggerFactory.Create(builder =>
                    {
                        builder.SetMinimumLevel(LogLevel.Information);
                        builder.AddConsole();
                    });
                }
            }
        }

        /// <summary>
        /// If logger factory is not set by the project using the library
        /// it will create a factory with console logger configured and minimum log level to Information.
        /// </summary>
        internal static ILoggerFactory LoggerFactory
        {
            get => _factory ?? DefaultFactory;
            set => _factory = value;
        }

        /// <summary>
        /// Use <paramref name="factory"/> (the logger factory of a stream's configuration) and return the factory used
        /// before, which <see cref="Restore"/> puts back when the stream is disposed.
        /// </summary>
        internal static ILoggerFactory Use(ILoggerFactory factory)
        {
            lock (factoryLock)
            {
                var previous = _factory;
                _factory = factory;
                return previous;
            }
        }

        /// <summary>
        /// Put back <paramref name="previous"/> if <paramref name="used"/> is still the factory in use
        /// (another stream may have set its own factory since).
        /// </summary>
        internal static void Restore(ILoggerFactory used, ILoggerFactory previous)
        {
            lock (factoryLock)
            {
                if (ReferenceEquals(_factory, used))
                    _factory = previous;
            }
        }

        /// <summary>
        /// Get logger from type class.
        /// </summary>
        /// <param name="type">Class type which call logger</param>
        /// <returns>Return logger configured</returns>
        public static ILogger GetLogger(Type type)
        {
            var factory = LoggerFactory;
            try
            {
                return factory.CreateLogger(type);
            }
            catch (ObjectDisposedException)
            {
                // the factory of a stream was disposed (typically at the end of a test): use the default factory
                lock (factoryLock)
                {
                    if (ReferenceEquals(_factory, factory))
                        _factory = null;
                }

                return DefaultFactory.CreateLogger(type);
            }
        }
    }
}
