using Google.Protobuf;
using OpenAPI.Net.Exceptions;
using OpenAPI.Net.Helpers;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Net.Security;
using System.Net.Sockets;
using System.Reactive.Disposables;
using System.Reactive.Linq;
using System.Threading;
using System.Threading.Tasks;
using Websocket.Client;
using System.Net;
using System.Net.WebSockets;
using System.Threading.Channels;
using System.Buffers;
using System.Text;

namespace OpenAPI.Net
{
    public sealed class OpenClient : IDisposable, IObservable<IMessage>
    {
        private readonly TimeSpan _heartbeatInerval;

        private readonly ProtoHeartbeatEvent _heartbeatEvent = new();

        private readonly Channel<ProtoMessage> _messagesChannel = Channel.CreateUnbounded<ProtoMessage>();

        private readonly ConcurrentDictionary<int, IObserver<IMessage>> _observers = new();

        private readonly CancellationTokenSource _cancellationTokenSource = new();

        private readonly TimeSpan _requestDelay;

        private readonly WebProxy _proxy;

        private TcpClient _tcpClient;

        private WebsocketClient _websocketClient;

        private SslStream _sslStream;

        private IDisposable _heartbeatDisposable;

        private IDisposable _webSocketDisconnectionHappenedDisposable;

        private IDisposable _webSocketMessageReceivedDisposable;

        /// <summary>
        /// Creates an instance of OpenClient which is not connected yet
        /// </summary>
        /// <param name="host">The host name of API endpoint</param>
        /// <param name="port">The host port number</param>
        /// <param name="heartbeatInerval">The time interval for sending heartbeats</param>
        /// <param name="maxRequestPerSecond">The maximum number of requests client will send per second</param>
        /// <param name="useWebSocket">By default OpenClient uses raw TCP connection, if you want to use web socket instead set this parameter to true</param>
        public OpenClient(string host, int port, TimeSpan heartbeatInerval, int maxRequestPerSecond = 40, bool useWebSocket = false, WebProxy proxy = null)
        {
            Host = host ?? throw new ArgumentNullException(nameof(host));

            if (port is < 0 or > 65535) throw new ArgumentOutOfRangeException(nameof(port));

            Port = port;

            _heartbeatInerval = heartbeatInerval;
            MaxRequestPerSecond = maxRequestPerSecond;
            _requestDelay = TimeSpan.FromMilliseconds(1000 / MaxRequestPerSecond);
            _proxy = proxy;
            IsUsingWebSocket = proxy == null && useWebSocket;
        }

        /// <summary>
        /// The API endpoint host that the current client is connected to
        /// </summary>
        public string Host { get; }

        /// <summary>
        /// The API host port that current client is connected to
        /// </summary>
        public int Port { get; }

        /// <summary>
        /// The maximum number of requests client will send per second
        /// </summary>
        public int MaxRequestPerSecond { get; }

        /// <summary>
        /// If client is connected via Websocket then this will return True, otherwise False
        /// </summary>
        public bool IsUsingWebSocket { get; }

        /// <summary>
        /// If client is disposed then this will return True, otherwise False
        /// </summary>
        public bool IsDisposed { get; private set; }

        /// <summary>
        /// If client stream is completed without any error then it will return True, otherwise False
        /// </summary>
        public bool IsCompleted { get; private set; }

        /// <summary>
        /// If there was any error (exception) on client stream then this will return True, otherwise False
        /// </summary>
        public bool IsTerminated { get; private set; }

        /// <summary>
        /// The time client sent its last message
        /// </summary>
        public DateTimeOffset LastSentMessageTime { get; private set; }

        /// <summary>
        /// The count of messages on queue to be sent
        /// </summary>
        public int MessagesQueueCount { get; private set; }

        /// <summary>
        /// Connects to the API based on you specified method (Websocket or TCP)
        /// </summary>
        /// <exception cref="ObjectDisposedException">If client is disposed</exception>
        /// <exception cref="ConnectionException">If connection attempt fails, the client will be disposed and this exception will be thrown</exception>
        /// <returns>Task</returns>
        public async Task Connect()
        {
            ThrowObjectDisposedExceptionIfDisposed();

            try
            {
                if (_proxy != null)
                {
                    await ConnectTcpViaSocks5Proxy();
                }
                else if (IsUsingWebSocket)
                {
                    await ConnectWebScoket();
                }
                else
                {
                    await ConnectTcp();
                }

                _ = StartSendingMessages(_cancellationTokenSource.Token);

                _heartbeatDisposable = Observable.Interval(_heartbeatInerval).DoWhile(() => !IsDisposed)
                    .Subscribe(x => SendHeartbeat());
            }
            catch (Exception ex)
            {
                var connectionException = new ConnectionException(ex);

                throw connectionException;
            }
        }

        /// <summary>
        /// Subscribe to client incoming messages
        /// </summary>
        /// <param name="observer">The observer that will receive the messages</param>
        /// <exception cref="ObjectDisposedException">If client is disposed</exception>
        /// <returns>IDisposable</returns>
        public IDisposable Subscribe(IObserver<IMessage> observer)
        {
            ThrowObjectDisposedExceptionIfDisposed();

            var observerHashCode = observer.GetHashCode();

            _ = _observers.AddOrUpdate(observerHashCode, observer, (key, oldObserver) => observer);

            return Disposable.Create(() => OnObserverDispose(observerHashCode));
        }

        /// <summary>
        /// This method will insert your message on messages queue, it will not send the message instantly
        /// By using this overload of SendMessage method you avoid passing the message payload type
        /// and it gets the payload from message itself
        /// </summary>
        /// <typeparam name="T">Message Type</typeparam>
        /// <param name="message">Message</param>
        /// <param name="clientMsgId">The client message ID (optional)</param>
        /// <exception cref="InvalidOperationException">If getting message payload type fails</exception>
        /// <returns>Task</returns>
        public async Task SendMessage<T>(T message, string clientMsgId = null) where T : IMessage
        {
            var protoMessage = MessageFactory.GetMessage(message.GetPayloadType(), message.ToByteString(), clientMsgId);

            await SendMessage(protoMessage);
        }

        /// <summary>
        /// This method will insert your message on messages queue, it will not send the message instantly
        /// </summary>
        /// <typeparam name="T">Message Type</typeparam>
        /// <param name="message">Message</param>
        /// <param name="payloadType">Message Payload Type (ProtoPayloadType)</param>
        /// <param name="clientMsgId">The client message ID (optional)</param>
        /// <returns>Task</returns>
        public async Task SendMessage<T>(T message, ProtoPayloadType payloadType, string clientMsgId = null) where T : IMessage
        {
            var protoMessage = MessageFactory.GetMessage(message, payloadType, clientMsgId);

            await SendMessage(protoMessage);
        }

        /// <summary>
        /// This method will insert your message on messages queue, it will not send the message instantly
        /// </summary>
        /// <typeparam name="T">Message Type</typeparam>
        /// <param name="message">Message</param>
        /// <param name="payloadType">Message Payload Type (ProtoOAPayloadType)</param>
        /// <param name="clientMsgId">The client message ID (optional)</param>
        /// <returns>Task</returns>
        public async Task SendMessage<T>(T message, ProtoOAPayloadType payloadType, string clientMsgId = null) where T : IMessage
        {
            var protoMessage = MessageFactory.GetMessage(message, payloadType, clientMsgId);

            await SendMessage(protoMessage);
        }

        /// <summary>
        /// This method will insert your message on messages queue, it will not send the message instantly
        /// </summary>
        /// <param name="message">Message</param>
        /// <returns>Task</returns>
        public async Task SendMessage(ProtoMessage message)
        {
            MessagesQueueCount += 1;

            await _messagesChannel.Writer.WriteAsync(message);
        }

        /// <summary>
        /// This method will send the passed message instantly
        /// If the client was already in use it will terminate the stream and you will get an error on your observers
        /// Use the other SendMessage methods to avoid issues with multiple threads trying to send message at the same time
        /// </summary>
        /// <param name="message">Message</param>
        /// <exception cref="ObjectDisposedException">If client is already disposed</exception>
        /// <exception cref="SendException">If something went wrong while sending the message, please check the inner exception for more detail</exception>
        /// <returns>Task</returns>
        public async Task SendMessageInstant(ProtoMessage message)
        {
            ThrowObjectDisposedExceptionIfDisposed();

            try
            {
                var messageByte = message.ToByteArray();

                if (IsUsingWebSocket)
                {
                    _websocketClient.Send(messageByte);
                }
                else
                {
                    await WriteTcp(messageByte, _cancellationTokenSource.Token);
                }

                LastSentMessageTime = DateTimeOffset.Now;
            }
            catch (Exception ex)
            {
                var exception = new SendException(ex);

                throw exception;
            }
        }

        /// <summary>
        /// Disposes the client, releases all used resources, and stops all running operations
        /// </summary>
        public void Dispose()
        {
            if (IsDisposed) return;

            IsDisposed = true;

            _heartbeatDisposable?.Dispose();

            _cancellationTokenSource.Cancel();

            _ = _messagesChannel.Writer.TryComplete();

            _cancellationTokenSource.Dispose();

            MessagesQueueCount = 0;

            if (IsUsingWebSocket)
            {
                _webSocketMessageReceivedDisposable?.Dispose();

                _webSocketDisconnectionHappenedDisposable?.Dispose();

                _websocketClient?.Dispose();
            }
            else
            {
                _sslStream?.Dispose();
                _tcpClient?.Dispose();
            }

            if (!IsTerminated) OnCompleted();
        }

        /// <summary>
        /// Connects to API by using Websocket
        /// </summary>
        /// <returns>Task</returns>
        private async Task ConnectWebScoket()
        {
            var hostUri = new Uri($"wss://{Host}:{Port}");

            _websocketClient = new WebsocketClient(hostUri, new Func<ClientWebSocket>(() =>
            {
                var ws = new ClientWebSocket();
                if (_proxy != null)
                {
                    ws.Options.Proxy = _proxy;
                }
                return ws;
            }))
            {
                IsTextMessageConversionEnabled = false,
                ReconnectTimeout = null,
                IsReconnectionEnabled = false,
                ErrorReconnectTimeout = null
            };

            _webSocketMessageReceivedDisposable = _websocketClient.MessageReceived.Select(msg => ProtoMessage.Parser.ParseFrom(msg.Binary))
                .Subscribe(OnNext);

            await _websocketClient.StartOrFail();

            _webSocketDisconnectionHappenedDisposable = _websocketClient.DisconnectionHappened.Subscribe(OnWebSocketDisconnectionHappened);
        }

        /// <summary>
        /// Connects to API by using a TCP client
        /// </summary>
        /// <returns>Task</returns>
        private async Task ConnectTcp()
        {
            _tcpClient = new TcpClient { LingerState = new LingerOption(true, 10) };

            await _tcpClient.ConnectAsync(Host, Port).ConfigureAwait(false);

            _sslStream = new SslStream(_tcpClient.GetStream(), false);

            await _sslStream.AuthenticateAsClientAsync(Host).ConfigureAwait(false);

            _ = Task.Run(() => ReadTcp(_cancellationTokenSource.Token));
        }

        /// <summary>
        /// Connects to API via a SOCKS5 proxy tunnel (port 22228), then does SSL/TLS over the tunnel.
        /// BrightData ISP proxies support all ports above 1024 via SOCKS5.
        /// </summary>
        /// <returns>Task</returns>
        private async Task ConnectTcpViaSocks5Proxy()
        {
            var proxyHost = _proxy.Address.Host;
            const int socks5Port = 22228;
            var credential = _proxy.Credentials.GetCredential(_proxy.Address, "Basic");
            var username = Encoding.ASCII.GetBytes(credential.UserName);
            var password = Encoding.ASCII.GetBytes(credential.Password);

            _tcpClient = new TcpClient { LingerState = new LingerOption(true, 10) };

            // Connect to SOCKS5 proxy with 15s timeout
            var connectTask = _tcpClient.ConnectAsync(proxyHost, socks5Port);
            if (await Task.WhenAny(connectTask, Task.Delay(15000)).ConfigureAwait(false) != connectTask)
            {
                _tcpClient.Dispose();
                throw new Exception($"SOCKS5 proxy connection to {proxyHost}:{socks5Port} timed out");
            }
            await connectTask.ConfigureAwait(false);

            var stream = _tcpClient.GetStream();
            stream.ReadTimeout = 15000;
            stream.WriteTimeout = 15000;

            // SOCKS5 greeting: version=5, 1 auth method, method=username/password (0x02)
            await stream.WriteAsync(new byte[] { 0x05, 0x01, 0x02 }).ConfigureAwait(false);

            var buf = new byte[2];
            await ReadExactAsync(stream, buf, 2).ConfigureAwait(false);
            if (buf[0] != 0x05 || buf[1] != 0x02)
                throw new Exception("SOCKS5 proxy does not support username/password authentication");

            // Auth: version=1, username length, username, password length, password
            var auth = new byte[3 + username.Length + password.Length];
            auth[0] = 0x01;
            auth[1] = (byte)username.Length;
            Buffer.BlockCopy(username, 0, auth, 2, username.Length);
            auth[2 + username.Length] = (byte)password.Length;
            Buffer.BlockCopy(password, 0, auth, 3 + username.Length, password.Length);
            await stream.WriteAsync(auth).ConfigureAwait(false);

            buf = new byte[2];
            await ReadExactAsync(stream, buf, 2).ConfigureAwait(false);
            if (buf[1] != 0x00)
                throw new Exception($"SOCKS5 authentication failed (status: {buf[1]})");

            // CONNECT: version=5, cmd=connect(1), rsv=0, atyp=domain(3), domain length, domain, port (big-endian)
            var hostBytes = Encoding.ASCII.GetBytes(Host);
            var connect = new byte[7 + hostBytes.Length];
            connect[0] = 0x05;
            connect[1] = 0x01;
            connect[2] = 0x00;
            connect[3] = 0x03;
            connect[4] = (byte)hostBytes.Length;
            Buffer.BlockCopy(hostBytes, 0, connect, 5, hostBytes.Length);
            connect[5 + hostBytes.Length] = (byte)(Port >> 8);
            connect[6 + hostBytes.Length] = (byte)(Port & 0xFF);
            await stream.WriteAsync(connect).ConfigureAwait(false);

            // Read CONNECT response (minimum 10 bytes for IPv4 bind address)
            var resp = new byte[10];
            await ReadExactAsync(stream, resp, 10).ConfigureAwait(false);
            if (resp[1] != 0x00)
                throw new Exception($"SOCKS5 CONNECT to {Host}:{Port} failed (status: {resp[1]})");

            // Tunnel established — reset timeouts and do SSL/TLS
            stream.ReadTimeout = 0;
            stream.WriteTimeout = 0;

            _sslStream = new SslStream(stream, false);
            await _sslStream.AuthenticateAsClientAsync(Host).ConfigureAwait(false);

            _ = Task.Run(() => ReadTcp(_cancellationTokenSource.Token));
        }

        private static async Task ReadExactAsync(NetworkStream stream, byte[] buffer, int count)
        {
            var read = 0;
            while (read < count)
            {
                var n = await stream.ReadAsync(buffer, read, count - read).ConfigureAwait(false);
                if (n == 0) throw new Exception("SOCKS5 proxy closed the connection");
                read += n;
            }
        }

        /// <summary>
        /// This method will keep reading the messages channel and then it will send the read message
        /// </summary>
        /// <param name="cancellationToken">The cancellation token that will cancel the reading</param>
        /// <returns>Task</returns>
        private async Task StartSendingMessages(CancellationToken cancellationToken)
        {
            try
            {
                while (await _messagesChannel.Reader.WaitToReadAsync(cancellationToken) && IsDisposed is false && IsTerminated is false)
                {
                    while (_messagesChannel.Reader.TryRead(out var message))
                    {
                        var timeElapsedSinceLastMessageSent = DateTimeOffset.Now - LastSentMessageTime;

                        if (timeElapsedSinceLastMessageSent < _requestDelay)
                        {
                            await Task.Delay(_requestDelay - timeElapsedSinceLastMessageSent);
                        }

                        if (IsDisposed is false)
                        {
                            await SendMessageInstant(message);
                        }

                        if (MessagesQueueCount > 0) MessagesQueueCount -= 1;
                    }
                }
            }
            catch (OperationCanceledException)
            {
            }
            catch (Exception ex)
            {
                OnError(ex);
            }
        }

        /// <summary>
        /// This method will read the TCP stream for incoming messages
        /// </summary>
        /// <param name="cancellationToken">The cancellation token that will be used on ReadAsync calls</param>
        /// <returns>Task</returns>
        private async void ReadTcp(CancellationToken cancellationToken)
        {
            var dataLength = new byte[4];
            byte[] data = null;

            try
            {
                while (!IsDisposed)
                {
                    var readBytes = 0;

                    do
                    {
                        var count = dataLength.Length - readBytes;

                        readBytes += await _sslStream.ReadAsync(dataLength, readBytes, count, cancellationToken).ConfigureAwait(false);

                        if (readBytes == 0) throw new InvalidOperationException("Remote host closed the connection");
                    }
                    while (readBytes < dataLength.Length);

                    var length = GetLength(dataLength);

                    if (length <= 0) continue;

                    data = ArrayPool<byte>.Shared.Rent(length);

                    readBytes = 0;

                    do
                    {
                        var count = length - readBytes;

                        readBytes += await _sslStream.ReadAsync(data, readBytes, count, cancellationToken).ConfigureAwait(false);

                        if (readBytes == 0) throw new InvalidOperationException("Remote host closed the connection");
                    }
                    while (readBytes < length);

                    var message = ProtoMessage.Parser.ParseFrom(data, 0, length);

                    ArrayPool<byte>.Shared.Return(data);

                    OnNext(message);
                }
            }
            catch (Exception ex)
            {
                if (data is not null) ArrayPool<byte>.Shared.Return(data);

                var exception = new ReceiveException(ex);

                OnError(exception);
            }
        }

        /// <summary>
        /// Returns the length of a received message without causing extra allocation
        /// </summary>
        /// <param name="lengthBytes">The byte array of received length data</param>
        /// <returns>int</returns>
        private int GetLength(byte[] lengthBytes)
        {
            var lengthSpan = lengthBytes.AsSpan();

            lengthSpan.Reverse();

            return BitConverter.ToInt32(lengthSpan);
        }

        /// <summary>
        /// Writes the message bytes to TCP stream
        /// </summary>
        /// <param name="messageByte"></param>
        /// <param name="cancellationToken">The cancellation token that will be used on calling stream methods</param>
        /// <returns>Task</returns>
        private async Task WriteTcp(byte[] messageByte, CancellationToken cancellationToken)
        {
            var data = BitConverter.GetBytes(messageByte.Length).Reverse().Concat(messageByte).ToArray();

            await _sslStream.WriteAsync(data, 0, data.Length, cancellationToken).ConfigureAwait(false);

            await _sslStream.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        /// <summary>
        /// Sends heartbeat to API for keeping the connection alive
        /// If the client is not disposed yet and the time difference
        /// between last sent message and now is greater than heart beat interval
        /// </summary>
        private async void SendHeartbeat()
        {
            if (IsDisposed || DateTimeOffset.Now - LastSentMessageTime < _heartbeatInerval) return;

            try
            {
                await SendMessage(_heartbeatEvent, ProtoPayloadType.HeartbeatEvent).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                OnError(ex);
            }
        }

        /// <summary>
        /// This method will be called if the web socket connection got disconnected
        /// </summary>
        /// <param name="disconnectionInfo">The disconnection info</param>
        private void OnWebSocketDisconnectionHappened(DisconnectionInfo disconnectionInfo)
        {
            disconnectionInfo.CancelReconnection = true;

            OnError(disconnectionInfo.Exception);
        }

        /// <summary>
        /// Removes the disposed observer from client observers collection
        /// </summary>
        /// <param name="observerKey">The observer hash code key</param>
        private void OnObserverDispose(int observerKey)
        {
            _ = _observers.TryRemove(observerKey, out _);
        }

        /// <summary>
        /// Calls each observer OnNext with the message
        /// </summary>
        /// <param name="protoMessage">Message</param>
        private void OnNext(ProtoMessage protoMessage)
        {
            foreach (var (_, observer) in _observers)
            {
                try
                {
                    var message = MessageFactory.GetMessage(protoMessage);

                    if (protoMessage.HasClientMsgId || message == null) observer.OnNext(protoMessage);

                    if (message != null) observer.OnNext(message);
                }
                catch (Exception ex)
                {
                    var observerException = new ObserverException(ex, observer);

                    OnError(observerException);
                }
            }
        }

        /// <summary>
        /// Disposes the client and then calls each observer OnError after an exception thrown
        /// </summary>
        /// <param name="exception">Exception</param>
        private void OnError(Exception exception)
        {
            if (IsTerminated) return;

            IsTerminated = true;

            Dispose();

            foreach (var (_, observer) in _observers)
            {
                try
                {
                    observer.OnError(exception);
                }
                catch (Exception ex) when (ex == exception)
                {
                }
            }

            _observers.Clear();
        }

        /// <summary>
        /// Completes each observer by calling their OnCompleted method
        /// </summary>
        private void OnCompleted()
        {
            IsCompleted = true;

            foreach (var (_, observer) in _observers)
            {
                observer.OnCompleted();
            }
        }

        /// <summary>
        /// Throws ObjectDisposedException if the client was disposed
        /// </summary>
        private void ThrowObjectDisposedExceptionIfDisposed()
        {
            if (IsDisposed) throw new ObjectDisposedException(GetType().FullName);
        }
    }
}