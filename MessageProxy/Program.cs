using RabbitMQ.Client;
using System.Text;
using Serilog;
using Serilog.Events;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Information()
    .MinimumLevel.Override("Microsoft", LogEventLevel.Warning)
    .MinimumLevel.Override("System", LogEventLevel.Information)
    .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u4}] {Message:lj}{NewLine}{Exception}")
    .CreateLogger();

var factory = new ConnectionFactory() { HostName = "121.36.58.218", Port = 5671, VirtualHost = "vei", CredentialsProvider = new BasicCredentialsProvider("root", userName: "root", password: "Xiangaimima@1") };
using var connection = await factory.CreateConnectionAsync();
using var publishChannel = await connection.CreateChannelAsync();
Log.Information("RabbitMQ: Publish Channel Ready!");
using var consumeChannel = await connection.CreateChannelAsync();
Log.Information("RabbitMQ: Consumer Channel Ready!");


var builder = WebApplication.CreateBuilder(args);
//builder.Host.UseSerilog();

// Add services to the container.

var app = builder.Build();
//app.UseSerilogRequestLogging();

// Configure the HTTP request pipeline.

var summaries = new[]
{
    "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
};

app.MapGet("/weatherforecast", () =>
{
    Log.Error("Generating weather forecast");
    var forecast = Enumerable.Range(1, 5).Select(index =>
        new WeatherForecast
        (
            DateOnly.FromDateTime(DateTime.Now.AddDays(index)),
            Random.Shared.Next(-20, 55),
            summaries[Random.Shared.Next(summaries.Length)]
        ))
        .ToArray();
    return forecast;
});

app.MapPost("/sendmessage", async (HttpRequest request) =>
{
    using var reader = new StreamReader(request.Body, Encoding.UTF8);
    var message = await reader.ReadToEndAsync();
    Log.Information("Received message: {Message}", message);
    
    
    var body = Encoding.UTF8.GetBytes(message);
    
    await publishChannel.BasicPublishAsync(exchange: "default",
                         routingKey: "6000170.master-data.create",
                         body: body);
    Log.Information("Sent message to RabbitMQ: {Message}", message);
    return Results.Ok(new { Status = "Message sent to RabbitMQ", Message = message });
});

app.Run();




internal record WeatherForecast(DateOnly Date, int TemperatureC, string? Summary)
{
    public int TemperatureF => 32 + (int)(TemperatureC / 0.5556);
}
