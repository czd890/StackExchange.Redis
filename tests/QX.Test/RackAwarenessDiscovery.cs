using Microsoft.Extensions.Logging;

namespace StackExchange.Redis.QXExtensions;
internal class RackAwarenessDiscovery
{
    private readonly ILogger? logger;

    public RackAwarenessDiscovery(ILogger? logger)
    {
        this.logger = logger;
    }
    public string? GetRackAwareness()
    {
        // auto discovery for: aws ecs, aws ec2, azure cloud, google cloud, azure
        return GETAWSECSAvailabilityZone();
    }
    public string? GETAWSECSAvailabilityZone()
    {
        logger?.LogInformation("Discovering the availability zone of the AWS ECS task");
        // ${ECS_CONTAINER_METADATA_URI_V4}/task
        // http://169.254.170.2/v4/292187dc9c2d43df836ebb75377b0f73-0558900462/task
        /*
         {
            "AvailabilityZone":"us-east-1d"
        }
         discover the availability zone of the ECS task
         */
        return "us-east-1d";
    }
}
