using Newtonsoft.Json;

namespace Streamiz.Kafka.Net.Azure.Cosmos.RemoteStorage.Internal
{
    /// <summary>
    /// One entry of a store in its container. Items are partitioned by task (<see cref="T"/>, partition key path <c>/t</c>).
    /// </summary>
    internal class CosmosItem
    {
        /// <summary>
        /// Id of the item in the partition (see <see cref="CosmosKeys.Id"/>)
        /// </summary>
        [JsonProperty("id")]
        public string Id { get; set; }

        /// <summary>
        /// Task id: the partition key
        /// </summary>
        [JsonProperty("t")]
        public string T { get; set; }

        /// <summary>
        /// Key bytes in lowercase hexadecimal: comparing these strings orders the keys like their bytes
        /// </summary>
        [JsonProperty("k")]
        public string K { get; set; }

        /// <summary>
        /// Value bytes in base64
        /// </summary>
        [JsonProperty("v")]
        public string V { get; set; }
    }
}
