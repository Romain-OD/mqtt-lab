// Last Mile — E1: what a courier's location update actually costs on the wire, and what
// happens when the courier drives into a tunnel.
//   dotnet run -- bytes    HTTP webhook vs MQTT, bytes counted by a TCP proxy
//   dotnet run -- tunnel   courier loses signal; ops learns it from the broker, not a timeout
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using MQTTnet;
using MQTTnet.Protocol;

var mode = args.FirstOrDefault() ?? "bytes";
var brokerHost = Environment.GetEnvironmentVariable("MQTT_HOST") ?? "127.0.0.1";
const int brokerPort = 1883;
var clock = Stopwatch.StartNew();
void Log(string who, string text) => Console.WriteLine($"[{clock.Elapsed.TotalSeconds,5:0.0}s] {who,-8} {text}");

const string payload = """{"courierId":"courier-42","lat":48.85661,"lon":2.35222,"ts":1760000000}""";

if (mode == "bytes") await Bytes();
else if (mode == "tunnel") await Tunnel();
else Console.WriteLine("usage: dotnet run -- bytes|tunnel");

async Task Bytes()
{
    const int n = 100;
    // RS256-shaped bearer token: same length as a real one, no real secret in it.
    var token = string.Join('.',
        B64("""{"alg":"RS256","typ":"JWT","kid":"lastmile-2026"}"""),
        B64("""{"iss":"https://auth.lastmile.example","sub":"courier-42","aud":"lastmile-api","scope":"location:write","iat":1760000000,"exp":1760003600}"""),
        Convert.ToBase64String(RandomNumberGenerator.GetBytes(256)).TrimEnd('=').Replace('+', '-').Replace('/', '_'));

    var builder = WebApplication.CreateSlimBuilder();
    builder.Logging.ClearProviders();
    builder.WebHost.UseUrls("http://127.0.0.1:5080");
    var app = builder.Build();
    app.MapPost("/webhook/location", () => Results.Ok());
    await app.StartAsync();

    Console.WriteLine($"payload: {Encoding.UTF8.GetByteCount(payload)} bytes of JSON, sent {n} times, no TLS");
    Console.WriteLine($"{"transport",-30}{"setup",8}{"per msg up",12}{"per msg down",14}{"total",10}");
    Row("HTTP webhook", await Http(null));
    Row($"HTTP webhook + bearer ({token.Length})", await Http(token));
    Row("MQTT 5, QoS 0", await Mqtt(MqttQualityOfServiceLevel.AtMostOnce, null));
    Row("MQTT 5, QoS 1", await Mqtt(MqttQualityOfServiceLevel.AtLeastOnce, null));
    Row("MQTT 5, QoS 1 + token once", await Mqtt(MqttQualityOfServiceLevel.AtLeastOnce, token));
    await app.StopAsync();

    void Row(string name, (long setup, long up, long down) r) =>
        Console.WriteLine($"{name,-30}{r.setup,8}{r.up / (double)n,12:0.0}{r.down / (double)n,14:0.0}{r.setup + r.up + r.down,10}");

    async Task<(long, long, long)> Http(string? bearer)
    {
        await using var meter = Meter.Start("127.0.0.1", 5080);
        using var http = new HttpClient { BaseAddress = new($"http://127.0.0.1:{meter.Port}") };
        if (bearer is not null) http.DefaultRequestHeaders.Authorization = new("Bearer", bearer);
        for (var i = 0; i < n; i++)
        {
            using var response = await http.PostAsync("/webhook/location", new StringContent(payload, Encoding.UTF8, "application/json"));
            response.EnsureSuccessStatusCode();
        }
        return (0, meter.Up, meter.Down); // HTTP/1.1 keep-alive: one TCP connection, no protocol-level setup
    }

    async Task<(long, long, long)> Mqtt(MqttQualityOfServiceLevel qos, string? password)
    {
        await using var meter = Meter.Start(brokerHost, brokerPort);
        using var client = new MqttClientFactory().CreateMqttClient();
        var options = new MqttClientOptionsBuilder()
            .WithTcpServer("127.0.0.1", meter.Port)
            .WithProtocolVersion(MQTTnet.Formatter.MqttProtocolVersion.V500)
            .WithClientId("courier-42");
        if (password is not null) options.WithCredentials("courier-42", password);
        await client.ConnectAsync(options.Build());
        var (setupUp, setupDown) = (meter.Up, meter.Down);
        for (var i = 0; i < n; i++)
            await client.PublishStringAsync("lastmile/couriers/courier-42/location", payload, qos);
        await Task.Delay(300); // QoS 0 returns before the bytes leave; let the proxy see them
        var result = (setupUp + setupDown, meter.Up - setupUp, meter.Down - setupDown);
        await client.DisconnectAsync();
        return result;
    }
}

async Task Tunnel()
{
    const string status = "lastmile/couriers/courier-42/status";
    const string location = "lastmile/couriers/courier-42/location";
    var factory = new MqttClientFactory();

    // ops talks to the broker directly; only the courier's link goes through the "tunnel"
    using var ops = factory.CreateMqttClient();
    await ops.ConnectAsync(factory.CreateClientOptionsBuilder().WithTcpServer(brokerHost, brokerPort).WithClientId("ops-dashboard").Build());
    foreach (var topic in new[] { status, location }) // forget the previous run
        await ops.PublishBinaryAsync(topic, [], MqttQualityOfServiceLevel.AtLeastOnce, retain: true);
    ops.ApplicationMessageReceivedAsync += e =>
    {
        Log("ops", $"courier-42 is {e.ApplicationMessage.ConvertPayloadToString()}");
        return Task.CompletedTask;
    };
    await ops.SubscribeAsync(status);

    await using var meter = Meter.Start(brokerHost, brokerPort);
    using var courier = factory.CreateMqttClient();
    var courierOptions = factory.CreateClientOptionsBuilder()
        .WithTcpServer("127.0.0.1", meter.Port)
        .WithClientId("courier-42")
        .WithKeepAlivePeriod(TimeSpan.FromSeconds(4)) // broker gives up after 1.5 x 4 s of silence
        .WithTimeout(TimeSpan.FromSeconds(3))
        .WithWillTopic(status).WithWillPayload("offline (last will)").WithWillRetain()
        .WithWillQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
        .Build();

    for (var tick = 1; clock.Elapsed < TimeSpan.FromSeconds(28); tick++)
    {
        var inTunnel = clock.Elapsed >= TimeSpan.FromSeconds(5) && clock.Elapsed < TimeSpan.FromSeconds(20);
        if (inTunnel != meter.Tunnel)
        {
            meter.Tunnel = inTunnel;
            Log("courier", inTunnel ? ">>> enters the tunnel, no signal" : "<<< out of the tunnel");
        }
        try
        {
            if (!courier.IsConnected)
            {
                await courier.ConnectAsync(courierOptions);
                await courier.PublishStringAsync(status, "online", MqttQualityOfServiceLevel.AtLeastOnce, retain: true);
                Log("courier", "connected");
            }
            await courier.PublishStringAsync(location, payload.Replace("1760000000", (1760000000 + tick).ToString()), MqttQualityOfServiceLevel.AtLeastOnce, retain: true);
            Log("courier", $"location #{tick} delivered");
        }
        catch (Exception ex)
        {
            Log("courier", $"location #{tick} not delivered ({ex.GetType().Name})");
        }
        await Task.Delay(1000);
    }

    // a dispatcher opens the dashboard now: retained messages answer before anyone publishes
    using var late = factory.CreateMqttClient();
    late.ApplicationMessageReceivedAsync += e =>
    {
        Log("late", $"{e.ApplicationMessage.Topic.Split('/')[^1]}: {e.ApplicationMessage.ConvertPayloadToString()} (retained={e.ApplicationMessage.Retain})");
        return Task.CompletedTask;
    };
    await late.ConnectAsync(factory.CreateClientOptionsBuilder().WithTcpServer(brokerHost, brokerPort).WithClientId("dispatcher-late").Build());
    await late.SubscribeAsync("lastmile/couriers/courier-42/#");
    await Task.Delay(500);
    await courier.DisconnectAsync(); // clean disconnect: no last will
}

static string B64(string s) => Convert.ToBase64String(Encoding.UTF8.GetBytes(s)).TrimEnd('=').Replace('+', '-').Replace('/', '_');

// TCP proxy that counts payload bytes each way. Tunnel = hold everything, like a dead radio link:
// no FIN, no RST, the bytes just stop arriving.
sealed class Meter : IAsyncDisposable
{
    readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    readonly CancellationTokenSource _cts = new();
    readonly string _host;
    readonly int _port;
    long _up, _down;
    public volatile bool Tunnel;
    public long Up => Interlocked.Read(ref _up);
    public long Down => Interlocked.Read(ref _down);
    public int Port => ((IPEndPoint)_listener.LocalEndpoint).Port;

    Meter(string host, int port) => (_host, _port) = (host, port);

    public static Meter Start(string host, int port)
    {
        var meter = new Meter(host, port);
        meter._listener.Start();
        _ = meter.AcceptLoop();
        return meter;
    }

    async Task AcceptLoop()
    {
        try { while (true) _ = Handle(await _listener.AcceptTcpClientAsync(_cts.Token)); }
        catch (OperationCanceledException) { }
    }

    async Task Handle(TcpClient client)
    {
        using var _ = client;
        using var upstream = new TcpClient { NoDelay = true };
        client.NoDelay = true;
        try
        {
            if (Tunnel) // no signal: the connection attempt never reaches the broker
            {
                while (Tunnel) await Task.Delay(50, _cts.Token);
                return;
            }
            await upstream.ConnectAsync(_host, _port, _cts.Token);
            await Task.WhenAny(Pump(client.GetStream(), upstream.GetStream(), true), Pump(upstream.GetStream(), client.GetStream(), false));
            while (Tunnel) await Task.Delay(50, _cts.Token); // a close inside the tunnel is not heard until we leave it
        }
        catch (Exception) when (_cts.IsCancellationRequested) { }
    }

    async Task Pump(NetworkStream from, NetworkStream to, bool up)
    {
        var buffer = new byte[16 * 1024];
        try
        {
            int read;
            while ((read = await from.ReadAsync(buffer, _cts.Token)) > 0)
            {
                while (Tunnel) await Task.Delay(50, _cts.Token);
                if (up) Interlocked.Add(ref _up, read); else Interlocked.Add(ref _down, read);
                await to.WriteAsync(buffer.AsMemory(0, read), _cts.Token);
            }
        }
        catch (Exception) { }
    }

    public ValueTask DisposeAsync()
    {
        _cts.Cancel();
        _listener.Stop();
        return ValueTask.CompletedTask;
    }
}
