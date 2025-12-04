using RabbitMQ.Client;
using System.Text;
using Serilog;
using Serilog.Events;
using System.Text.Json.Nodes;
using System.Text.Json;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Debug()
    .MinimumLevel.Override("Microsoft", LogEventLevel.Information)
    .MinimumLevel.Override("System", LogEventLevel.Information)
    .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u4}] {Message:lj}{NewLine}{Exception}")
    .CreateLogger();

var factory = new ConnectionFactory() { HostName = "121.36.58.218", Port = 5671, VirtualHost = "vei", CredentialsProvider = new BasicCredentialsProvider("root", userName: "root", password: "Xiangaimima@1" ), Ssl = new SslOption { 
    Enabled = true,
    Version = System.Security.Authentication.SslProtocols.Tls12,
    CertificateValidationCallback = (a, b, c, d) => true
} };
using var connection = await factory.CreateConnectionAsync();
using var publishChannel = await connection.CreateChannelAsync();
Log.Information("RabbitMQ: Publish Channel Ready!");
using var consumeChannel = await connection.CreateChannelAsync();
Log.Information("RabbitMQ: Consumer Channel Ready!");


var builder = WebApplication.CreateBuilder(args);
builder.Host.UseSerilog();

// Add services to the container.

var app = builder.Build();
app.UseSerilogRequestLogging();

// Configure the HTTP request pipeline.

var basicProperties = new BasicProperties()
{
    DeliveryMode = DeliveryModes.Persistent,
    Expiration = "86400000",
    ContentType = "application/json"
};

app.MapPost("/sendmessage", async (HttpRequest request) =>
{ 
    var routingKey = request.Query["routingKey"].ToString();
    if (routingKey == null || routingKey.Length == 0)
    {
        Log.Information("Missing routingKey query parameter");
        return Results.BadRequest(new { state = "E", error = "E001", message = "Missing routingKey query parameter" });
    }

    var msgType = request.Query["msgType"].ToString();
    if( msgType == null || msgType.Length == 0)
    {
        Log.Information("Missing msgType query parameter");
        return Results.BadRequest(new { state = "E", error = "E002", message = "Missing msgType query parameter" });
    }

    var source = request.Query["source"].ToString();
    if( source == null || source.Length == 0)
    {
        Log.Information("Missing source query parameter");
        return Results.BadRequest(new { state = "E", error = "E003", message = "Missing source query parameter" });
    }

    var version = "v1";

    var traceId = request.Query["traceId"].ToString();
    if( traceId == null || traceId.Length == 0)
    {
        Log.Information("Missing traceId query parameter");
        return Results.BadRequest(new { state = "E", error = "E004", message = "Missing traceId query parameter" });
    }


    try
    {
        using var reader = new StreamReader(request.Body, Encoding.UTF8);
        var body = await reader.ReadToEndAsync();
        JsonNode? node = null;
        if(!string.IsNullOrWhiteSpace(body))
            node = JsonNode.Parse(body);
        var message = new Message(msgType, source, version, traceId, node);
        var jsonMessage = JsonSerializer.Serialize(message, new JsonSerializerOptions { WriteIndented = true });

        var objMessage = Encoding.UTF8.GetBytes(jsonMessage);
        await publishChannel.BasicPublishAsync(exchange: "default",
                         routingKey: routingKey,
                         body: objMessage,
                         mandatory: false,
                         basicProperties: basicProperties);
        Log.Information($"Message sent! {message.msgId}");
        return Results.Ok();
    }
    catch(JsonException ex)
    {
        Log.Information("Body resolve error, check data: {0}", ex.Message);
        return Results.BadRequest(new { state = "E", error = "E005", message = "Body resolve error" });
    } 
});



app.Run();




internal record Message(string msgType, string source, string version, string traceId, JsonNode? body)
{
    public string msgId => System.Guid.NewGuid().ToString();

    public DateTime timestamp => DateTime.UtcNow;
}
