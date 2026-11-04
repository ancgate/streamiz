namespace Streamiz.Kafka.Net.Tests.Processors
{
    // The async processors tests, run with the in-task engine (AsyncProcessingMode.V2)

    public class KStreamMapAsyncV2Tests : KStreamMapAsyncTests
    {
        protected override AsyncProcessingMode AsyncMode => AsyncProcessingMode.V2;
    }

    public class KStreamMapValuesAsyncV2Tests : KStreamMapValuesAsyncTests
    {
        protected override AsyncProcessingMode AsyncMode => AsyncProcessingMode.V2;
    }

    public class KStreamFlatMapAsyncV2Tests : KStreamFlatMapAsyncTests
    {
        protected override AsyncProcessingMode AsyncMode => AsyncProcessingMode.V2;
    }

    public class KStreamFlatMapValuesAsyncV2Tests : KStreamFlatMapValuesAsyncTests
    {
        protected override AsyncProcessingMode AsyncMode => AsyncProcessingMode.V2;
    }

    public class KStreamForeachAsyncV2Tests : KStreamForeachAsyncTests
    {
        protected override AsyncProcessingMode AsyncMode => AsyncProcessingMode.V2;
    }
}
