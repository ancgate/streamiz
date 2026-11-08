using NUnit.Framework;
using Streamiz.Examples.StateAndQueries.Examples;

namespace Streamiz.Examples.StateAndQueries.Tests;

public class LocalQueriesExampleTests
{
    private static DateTime At(string time) => DateTime.SpecifyKind(DateTime.Parse($"2026-01-01T{time}"), DateTimeKind.Utc);

    private ExampleDriver test;

    [SetUp]
    public void PipeViewsAndProfiles()
    {
        test = new ExampleDriver(new LocalQueriesExample());
        var views = test.Driver.CreateInputTopic<string, string>("page-views");
        var profiles = test.Driver.CreateInputTopic<string, string>("user-profiles");

        profiles.PipeInput("alice", "plan=free", At("09:00:00"));
        views.PipeInput("alice", "home", At("10:00:10"));
        views.PipeInput("bob", "home", At("10:00:40"));
        views.PipeInput("carol", "home", At("10:01:05"));
        views.PipeInput("alice", "pricing", At("10:01:20"));
        profiles.PipeInput("alice", "plan=pro", At("10:30:00"));
    }

    [TearDown]
    public void Dispose() => test.Dispose();

    [Test]
    public void KeyValueStore()
    {
        var store = test.Driver.GetKeyValueStore<string, long>(LocalQueriesExample.ViewsPerPage);

        Assert.AreEqual(3L, PageViewQueries.ViewsOf(store, "home"));
        Assert.AreEqual(0L, PageViewQueries.ViewsOf(store, "never-viewed"));
        CollectionAssert.AreEqual(new[] { ("home", 3L), ("pricing", 1L) }, PageViewQueries.TopPages(store));
    }

    [Test]
    public void WindowStore()
    {
        var store = test.Driver.GetWindowStore<string, long>(LocalQueriesExample.ViewsPerPagePerMinute);

        // "home" was viewed twice in the 10:00 minute and once in the 10:01 minute
        CollectionAssert.AreEqual(
            new[] { (At("10:00:00"), 2L), (At("10:01:00"), 1L) },
            PageViewQueries.ViewsPerMinute(store, "home", At("09:00:00"), At("11:00:00")));
    }

    [Test]
    public void SessionStore()
    {
        var store = test.Driver.GetSessionStore<string, long?>(LocalQueriesExample.UserSessions);

        // alice's two views are 70 seconds apart (less than the 30 minutes gap): one session
        CollectionAssert.AreEqual(
            new[] { (At("10:00:10"), At("10:01:20"), 2L) },
            PageViewQueries.SessionsOf(store, "alice"));
    }

    [Test]
    public void VersionedStore()
    {
        var store = test.Driver.GetVersionedKeyValueStore<string, string>(LocalQueriesExample.UserProfiles);

        Assert.AreEqual("plan=pro", PageViewQueries.CurrentProfile(store, "alice"));
        // the profile alice had at 10:00, before her upgrade at 10:30
        Assert.AreEqual("plan=free", PageViewQueries.ProfileAt(store, "alice", At("10:00:00")));
        Assert.IsNull(PageViewQueries.ProfileAt(store, "alice", At("08:00:00")));
    }
}
