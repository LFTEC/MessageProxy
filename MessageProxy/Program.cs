using Google.OrTools.LinearSolver;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Serilog;
using Serilog.Events;
using System.Net.Http;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization;
using System.Threading.Channels;
using static MIPInputData;

Log.Logger = new LoggerConfiguration()
    .MinimumLevel.Debug()
    .MinimumLevel.Override("Microsoft", LogEventLevel.Information)
    .MinimumLevel.Override("System", LogEventLevel.Information)
    .WriteTo.Console(outputTemplate: "[{Timestamp:HH:mm:ss} {Level:u4}] {Message:lj}{NewLine}{Exception}")
    .CreateLogger();

var builder = WebApplication.CreateBuilder(args);
var configuration = builder.Configuration;
builder.Services.Configure<RabbitMQConnectionOptions>(configuration.GetSection("RabbitMQ"));
builder.Services.Configure<CallbackOptions>(configuration.GetSection("Callback"));
builder.Services.Configure<Microsoft.AspNetCore.Http.Json.JsonOptions>(options =>
{
    options.SerializerOptions.PropertyNamingPolicy = JsonNamingPolicy.CamelCase;
    options.SerializerOptions.DictionaryKeyPolicy = JsonNamingPolicy.CamelCase;
    options.SerializerOptions.Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping;
    options.SerializerOptions.WriteIndented = true;
    options.SerializerOptions.NumberHandling = System.Text.Json.Serialization.JsonNumberHandling.AllowReadingFromString;
});

builder.Services.AddSingleton<MessageProxyService>();
builder.Services.AddSingleton<IMessageProxyService>(sp => sp.GetRequiredService<MessageProxyService>());
builder.Services.AddSingleton<IHostedService>(sp => sp.GetRequiredService<MessageProxyService>());


builder.Services.AddHttpClient<ICallbackService, CallbackService>((serviceProvider, client) =>
{
    var option = serviceProvider.GetRequiredService<IOptions<CallbackOptions>>();
    client.BaseAddress = new Uri(option.Value.BaseUrl);
}).ConfigurePrimaryHttpMessageHandler((serviceProvider) => 
{
    var option = serviceProvider.GetRequiredService<IOptions<CallbackOptions>>().Value;
    return new HttpClientHandler()
    {
        SslProtocols = System.Security.Authentication.SslProtocols.Tls12,
        Credentials = new System.Net.NetworkCredential(option.UserName, option.Password)
    };
});

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

app.MapPost("/sendmessage", async (HttpRequest request, IMessageProxyService proxy) =>
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

    var dataType = request.Query["dataType"].ToString();
    if(dataType == null || dataType.Length == 0)
    {
        dataType = "object";
    }
    else if( dataType != "object" && dataType != "array")
    {
        Log.Information("incorrect dataType passed");
        return Results.BadRequest(new { state = "E", error = "E005", message = "incorrect dataType passed" });
    }


    try
    {
        using var reader = new StreamReader(request.Body, Encoding.UTF8);
        var body = await reader.ReadToEndAsync();
        List<JsonNode> nodes = new List<JsonNode>();
        if (dataType == "array")
        {
            var node = JsonNode.Parse(body);
            if (node is not JsonArray)
            {
                Log.Information("dataType is array but body is not JsonArray");
                return Results.BadRequest(new { state = "E", error = "E006", message = "dataType is array but body is not JsonArray" });
            }
            else
            {
                foreach (var item in node.AsArray())
                {
                    nodes.Add(item!);
                }
            }
        }
        else if (!string.IsNullOrWhiteSpace(body))
        {
            var node = JsonNode.Parse(body);
            nodes.Add(node!);
        }

        if(nodes.Count == 0)
        {
            var message = new Message(msgType, source, version, traceId, null);
            var jsonMessage = JsonSerializer.Serialize(message, new JsonSerializerOptions { WriteIndented = true, Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping });

            var objMessage = Encoding.UTF8.GetBytes(jsonMessage);
            await proxy.PublishAsync(exchange: "default",
                                routingKey: routingKey,
                                body: objMessage);
            Log.Information($"Message sent! {message.msgId}");
        }
        else
        {
            List<Task> tasks = new List<Task>();
            nodes.ForEach((node) =>
            {
                var message = new Message(msgType, source, version, traceId, node);
                var jsonMessage = JsonSerializer.Serialize(message, new JsonSerializerOptions { WriteIndented = true, Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping });
                var objMessage = Encoding.UTF8.GetBytes(jsonMessage);
                tasks.Add(proxy.PublishAsync(exchange: "default",
                                    routingKey: routingKey,
                                    body: objMessage).ContinueWith(t =>
                                    {
                                        if (t.IsCompletedSuccessfully)
                                        {
                                            Log.Information($"Message sent! {message.msgId}");
                                        }
                                        else if (t.IsFaulted)
                                        {
                                            Log.Error($"Message send failed! {message.msgId}, Error: {t.Exception?.GetBaseException().Message}");
                                        }
                                    }));
            });

            Task.WaitAll(tasks.ToArray());
        }

        return Results.Ok();
    }
    catch (JsonException ex)
    {
        Log.Information("Body resolve error, check data: {0}", ex.Message);
        return Results.BadRequest(new { state = "E", error = "E005", message = "Body resolve error" });
    } 
});

app.MapPost("/mip", async (MIPInputData data) =>
{
    try
    {
        Solver solver = Solver.CreateSolver("SCIP");
        if (solver == null)
        {
            return Results.BadRequest("无法创建求解器，请确保安装了 Google.OrTools");
        }

        Variable[,] x = new Variable[data.Vendors.Count, data.Reservations.Count];
        for (int i = 0; i < data.Vendors.Count; i++)
        {
            for (int j = 0; j < data.Reservations.Count; j++)
            {
                x[i, j] = solver.MakeNumVar(0, double.PositiveInfinity, $"x_{i}_{j}");
            }
        }

        Variable[] y = new Variable[data.Vendors.Count];
        for (int i = 0; i < data.Vendors.Count; i++)
        {
            y[i] = solver.MakeBoolVar($"y_{i}");
        }

        for (int j = 0; j < data.Reservations.Count; j++)
        {
            Constraint demandConstraint = solver.MakeConstraint(0, Convert.ToDouble(data.Reservations[j].Quantity), $"demand_{j}");
            for (int i = 0; i < data.Vendors.Count; i++)
            {
                demandConstraint.SetCoefficient(x[i, j], 1);
            }
        }

        for (int i = 0; i < data.Vendors.Count; i++)
        {
            for (int j = 0; j < data.Vendors[i].Stocks.Count; j++)
            {
                Constraint stockConstraint = solver.MakeConstraint(-double.PositiveInfinity, 0, $"stock_{i}_{j}");
                for (int k = 0; k < data.Reservations.Count; k++)
                {
                    if (data.Reservations[k].MatNo == data.Vendors[i].Stocks[j].MatNo)
                        stockConstraint.SetCoefficient(x[i, k], 1);
                }
                stockConstraint.SetCoefficient(y[i], -Convert.ToDouble(data.Vendors[i].Stocks[j].Quantity));
            }
        }

        double W_QTY = data.Params.ParamQty;
        double W_PPL = data.Params.ParamVendor;
        double W_VAL = data.Params.ParamOffset;

        Objective objective = solver.Objective();

        for (int i = 0; i < data.Vendors.Count; i++)
        {
            objective.SetCoefficient(y[i], -W_PPL);

            for (int j = 0; j < data.Reservations.Count; j++)
            {
                var mat = data.Vendors[i].Stocks.First(s => s.MatNo == data.Reservations[j].MatNo);
                double coeff = W_QTY + (W_VAL * Convert.ToDouble(mat.Offset));
                coeff += W_QTY * mat.Priority / 2;
                coeff += data.Vendors[i].Ability >= data.Reservations[j].Difficulty ? W_QTY : 0.0;
                objective.SetCoefficient(x[i, j], coeff);
            }
        }

        objective.SetMaximization();

        Solver.ResultStatus resultStatus = solver.Solve();

        if (resultStatus == Solver.ResultStatus.OPTIMAL)
        {
            MIPOutputData outputData = new MIPOutputData();
            decimal totalAllocated = 0m;
            int vendorUsed = 0;

            for (int i = 0; i < data.Vendors.Count; i++)
            {
                if (y[i].SolutionValue() > 0.5)
                {
                    vendorUsed++;

                    for (int j = 0; j < data.Reservations.Count; j++)
                    {
                        decimal count = Convert.ToDecimal(x[i, j].SolutionValue());
                        if (count > 0.001m)
                        {
                            outputData.Allocations.Add(new MIPOutputData.ResAllocation
                            {
                                MatNo = data.Reservations[j].MatNo,
                                AllocatedQuantity = count,
                                ResNumber = data.Reservations[j].ResNumber,
                                ResItem = data.Reservations[j].ResItem,
                                VendorNumber = data.Vendors[i].VendorNumber
                            });

                            totalAllocated += count;
                        }
                    }
                }
            }

            outputData.TotalVendorsUsed = vendorUsed;
            outputData.TotalQuantityAllocated = totalAllocated;

            return Results.Ok(outputData);
        }
        else
        {
            return Results.BadRequest("无法找到最优解。");
        }
    }
    catch (Exception ex)
    {
        return Results.BadRequest($"求解过程中发生错误: {ex.Message}");
    }

});



app.Run();

public class MIPInputData
{
    public List<Reservation> Reservations { get; set; } = new List<Reservation>();

    public List<Vendor> Vendors { get; set; } = new List<Vendor>();
    public Parameters Params { get; set; } = new Parameters();

    public class Reservation
    {
        public string ResNumber { get; set; } = string.Empty;
        public string ResItem { get; set; } = string.Empty;
        public string MatNo { get; set; } = string.Empty;
        public int Difficulty { get; set; }

        public decimal Quantity { get; set; }
    }

    public class Vendor
    {
        public string VendorNumber { get; set; } = string.Empty;
        public int Ability { get; set; }
        public List<Stock> Stocks { get; set; } = new List<Stock>();
    }

    public class Stock
    {
        public string MatNo { get; set; } = string.Empty;
        public decimal Quantity { get; set; }

        public decimal Offset { get; set; }
        public int Priority { get; set; }
    }

    public class Parameters
    {
        public double ParamQty { get; set; }
        public double ParamVendor { get; set; }
        public double ParamOffset { get; set; }
    }
}

public class MIPOutputData
{
    public int TotalVendorsUsed { get; set; }
    public decimal TotalQuantityAllocated { get; set; }
    public List<ResAllocation> Allocations { get; set; } = new List<ResAllocation>();

    public class ResAllocation
    {
        public string MatNo { get; set; } = string.Empty;
        public decimal AllocatedQuantity { get; set; }
        public string ResNumber { get; set; } = string.Empty;
        public string ResItem { get; set; } = string.Empty;
        public string VendorNumber { get; set; } = string.Empty;
    }
}


internal class RabbitMQConnectionOptions
{    
    public string Host { get; set; } = string.Empty;
    public int Port { get; set; }
    public string VirtualHost { get; set; } = string.Empty;
    public string UserName { get; set; } = string.Empty;
    public string Password { get; set; } = string.Empty;
    public bool SslEnabled { get; set; }
}

internal class CallbackOptions
{
    public string BaseUrl { get; set; } = string.Empty;
    public string UserName { get; set; } = string.Empty;
    public string Password { get; set; } = string.Empty;
}

internal record Message(string msgType, string source, string version, string traceId, JsonNode? body)
{
    public string msgId => System.Guid.NewGuid().ToString();

    public DateTime timestamp => DateTime.UtcNow;
}

internal interface IMessageProxyService
{
    Task PublishAsync(string exchange, string routingKey, byte[] body);
}

internal class MessageProxyService: IMessageProxyService, IHostedService, IAsyncDisposable
{
    private readonly RabbitMQConnectionOptions _options;
    private readonly ICallbackService _callbackService;
    private IConnection? _connection;
    private IChannel? _publish_channel;
    private IChannel? _consume_channel;

    private BasicProperties _basicProperties;

    public MessageProxyService(IOptions<RabbitMQConnectionOptions> options, ICallbackService callbackService)
    {
        _options = options.Value;

        _basicProperties = new BasicProperties()
        {
            DeliveryMode = DeliveryModes.Persistent,
            Expiration = "86400000",
            ContentType = "application/json"
        };

        _callbackService = callbackService;
    }

    public async Task PublishAsync(string exchange, string routingKey, byte[] body)
    {
        await _publish_channel!.BasicPublishAsync(exchange, routingKey, body: body, mandatory: false, basicProperties: _basicProperties);
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        var factory = new ConnectionFactory()
        {
            HostName = _options.Host,
            Port = _options.Port,
            VirtualHost = _options.VirtualHost,
            CredentialsProvider = new BasicCredentialsProvider(_options.UserName, userName: _options.UserName, password: _options.Password),
            Ssl = new SslOption
            {
                Enabled = _options.SslEnabled,
                Version = System.Security.Authentication.SslProtocols.Tls12,
                CertificateValidationCallback = (a, b, c, d) => true
            }
        };
        _connection = await factory.CreateConnectionAsync();
        _publish_channel = await _connection.CreateChannelAsync();
        Log.Information("RabbitMQ: Publish Channel Ready!");
        _consume_channel = await _connection.CreateChannelAsync();
        Log.Information("RabbitMQ: Consumer Channel Ready!");

        var consumer = new AsyncEventingBasicConsumer(_consume_channel);
        consumer.ReceivedAsync += async (model, ea) =>
        {
            Log.Information("DLX Message Received!");
            var body = ea.Body.ToArray();
            var message = Encoding.UTF8.GetString(body);
            var routingKey = ea.RoutingKey;

            var node = JsonNode.Parse(message);
            if(node is JsonObject obj)
            {
                obj.Add("routingKey", routingKey);
                await _callbackService.SendCallbackAsync(obj);
            }
            else
            {
                Log.Error("Received message is not a JsonObject, dump it");
            }
        };

        await _consume_channel.BasicConsumeAsync("dlx", autoAck: true, consumer: consumer);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_publish_channel is not null)
            await _publish_channel.CloseAsync(cancellationToken);

        if (_consume_channel is not null)
            await _consume_channel.CloseAsync(cancellationToken);

        if (_connection is not null)
            await _connection.CloseAsync();
    }

    public async ValueTask DisposeAsync()
    {
        if (_connection is not null)
        {
            // 确保连接在被释放前已经关闭
            if (_connection.IsOpen)
            {
                await _connection.CloseAsync();
            }
            _connection.Dispose(); // 释放资源
        }

        // 避免重复终结
        GC.SuppressFinalize(this);
    }
}

internal interface ICallbackService
{
    Task SendCallbackAsync(JsonObject obj);
}

internal class CallbackService : ICallbackService
{
    private readonly HttpClient _client;

    public CallbackService(HttpClient client)
    {
        _client = client;
    }

    public async Task SendCallbackAsync(JsonObject obj)
    {
        Log.Information("Sending DLX callback...");
        JsonObject payload = new JsonObject
        {
            ["data"] = obj,
            ["timestamp"] = DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"),
            ["type"] = "API_DLX_CALLBACK"
        };

        
        var content = new StringContent(payload.ToJsonString(new JsonSerializerOptions { Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping, WriteIndented = true}), Encoding.UTF8, "application/json");
        var response = await _client.PostAsync("RESTAdapter/1622/PeripheralWarehouse", content);
        response.EnsureSuccessStatusCode();

        Log.Information("DLX callback sent successfully.");
    }
}

