// =====================================================================
//  Vítreo Relay — servidor de retransmissão do Acesso Remoto
//  Os dois PCs se conectam aqui (saída HTTPS/WebSocket), o que funciona
//  atrás de qualquer roteador ou CGNAT. O conteúdo vem criptografado de
//  ponta a ponta (AES-256-GCM): o servidor só repassa bytes que não lê.
// =====================================================================
using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Text.RegularExpressions;

var port = Environment.GetEnvironmentVariable("PORT") ?? "8080";
var builder = WebApplication.CreateBuilder(args);
builder.WebHost.UseUrls($"http://0.0.0.0:{port}");
builder.Logging.SetMinimumLevel(LogLevel.Warning);
var app = builder.Build();
app.UseWebSockets(new WebSocketOptions { KeepAliveInterval = TimeSpan.FromSeconds(20) });

var hosts = new ConcurrentDictionary<string, HostConn>();
var idRx = new Regex("^[1-9][0-9]{8}$");
const int MaxMessage = 16 * 1024 * 1024;

app.MapGet("/", () => Results.Text($"Vitreo Relay OK - {hosts.Count} online"));

app.Map("/ws", async ctx =>
{
    if (!ctx.WebSockets.IsWebSocketRequest) { ctx.Response.StatusCode = 400; return; }
    string role = ctx.Request.Query["role"], id = ctx.Request.Query["id"];
    if (id == null || !idRx.IsMatch(id) || (role != "host" && role != "viewer")) { ctx.Response.StatusCode = 400; return; }
    using var ws = await ctx.WebSockets.AcceptWebSocketAsync();
    if (role == "host") await RunHost(id, ws);
    else await RunViewer(id, ws);
});

app.Run();

// ---------------------------------------------------------------------

async Task RunHost(string id, WebSocket ws)
{
    var conn = new HostConn(ws);
    if (!hosts.TryAdd(id, conn))
    {
        // ID ocupado por uma conexão morta? substitui; viva? recusa
        if (hosts.TryGetValue(id, out var old) && old.Ws.State != WebSocketState.Open) { hosts[id] = conn; }
        else { await SendText(ws, null, "id-in-use"); await Close(ws); return; }
    }
    try
    {
        while (true)
        {
            var msg = await Receive(ws);
            if (msg == null) break;
            if (msg.Value.Type == WebSocketMessageType.Text && Encoding.UTF8.GetString(msg.Value.Data) == "kick")
            {
                var v = conn.Viewer;
                if (v != null) await Close(v);
                continue;
            }
            var viewer = conn.Viewer;
            if (viewer != null) await Send(viewer, conn.ViewerLock, msg.Value.Type, msg.Value.Data);
        }
    }
    finally
    {
        hosts.TryRemove(new KeyValuePair<string, HostConn>(id, conn));
        if (conn.Viewer != null) { await SendText(conn.Viewer, conn.ViewerLock, "peer-left"); await Close(conn.Viewer); }
    }
}

async Task RunViewer(string id, WebSocket ws)
{
    if (!hosts.TryGetValue(id, out var host) || host.Ws.State != WebSocketState.Open)
    { await SendText(ws, null, "offline"); await Close(ws); return; }
    lock (host)
    {
        if (host.Viewer != null) host = null;
        else { host.Viewer = ws; host.ViewerLock = new SemaphoreSlim(1, 1); }
    }
    if (host == null) { await SendText(ws, null, "busy"); await Close(ws); return; }

    var myLock = host.ViewerLock;
    try
    {
        await SendText(host.Ws, host.HostLock, "peer");
        await SendText(ws, myLock, "paired");
        while (true)
        {
            var msg = await Receive(ws);
            if (msg == null) break;
            await Send(host.Ws, host.HostLock, msg.Value.Type, msg.Value.Data);
        }
    }
    finally
    {
        lock (host) { if (host.Viewer == ws) host.Viewer = null; }
        try { await SendText(host.Ws, host.HostLock, "peer-left"); } catch { }
    }
}

async Task<(WebSocketMessageType Type, byte[] Data)?> Receive(WebSocket ws)
{
    var buf = new byte[64 * 1024];
    using var ms = new MemoryStream();
    try
    {
        while (true)
        {
            var r = await ws.ReceiveAsync(buf, CancellationToken.None);
            if (r.MessageType == WebSocketMessageType.Close) return null;
            ms.Write(buf, 0, r.Count);
            if (ms.Length > MaxMessage) return null;
            if (r.EndOfMessage) return (r.MessageType, ms.ToArray());
        }
    }
    catch { return null; }
}

async Task Send(WebSocket ws, SemaphoreSlim gate, WebSocketMessageType type, byte[] data)
{
    if (ws.State != WebSocketState.Open) return;
    if (gate != null) await gate.WaitAsync();
    try { await ws.SendAsync(data, type, true, CancellationToken.None); }
    catch { }
    finally { gate?.Release(); }
}

Task SendText(WebSocket ws, SemaphoreSlim gate, string text) => Send(ws, gate, WebSocketMessageType.Text, Encoding.UTF8.GetBytes(text));

async Task Close(WebSocket ws)
{
    try { if (ws.State == WebSocketState.Open) await ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "bye", CancellationToken.None); }
    catch { }
}

class HostConn
{
    public HostConn(WebSocket ws) { Ws = ws; }
    public WebSocket Ws { get; }
    public SemaphoreSlim HostLock { get; } = new(1, 1);
    public WebSocket Viewer { get; set; }
    public SemaphoreSlim ViewerLock { get; set; }
}
