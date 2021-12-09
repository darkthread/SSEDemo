<%@ Page Language="C#" %>
<%@ Import Namespace="System.Collections.Concurrent" %>
    <script runat="server">
        public class SseProcessor 
        {
            static ConcurrentDictionary<string, SseProcessor> pool = new ConcurrentDictionary<string, SseProcessor>();
            string sseKey;
            HttpResponse response;
            public SseProcessor(string sseKey, HttpResponse response) 
            {
                this.sseKey = sseKey;
                this.response = response;
            }
            public static void Broadcast(string evtId, string message) 
            {
                Broadcast(evtId + "\t" + message);
            }
            public static void Broadcast(string message) 
            {
                foreach(var p in pool.Values) 
                    try 
                    {
                        lock(p) { p.Messages.Enqueue(message); }
                    }
                    catch {
                        //ignore
                    }
            }
            public Queue<string> Messages = new Queue<string>();
            public void Run()
            {
                response.ContentType = "text/event-stream";
                pool.TryAdd(this.sseKey, this);
                Broadcast("stat", pool.Count() + " users online");
                try 
                {
                    var timeout = DateTime.Now.AddSeconds(60);
                    while (response.IsClientConnected && DateTime.Now.CompareTo(timeout) < 0) 
                    {
                        if (Messages.Any()) 
                        {
                            var msg = Messages.Dequeue();
                            var p = msg.Split('\t');
                            if (p.Length == 2) 
                            {
                                response.Write("event: " + p[0] + "\n");
                                response.Write("data: " + p[1] + "\n\n");
                            }
                            else
                                response.Write("data: " + msg + "\n\n");
                            response.Flush();
                        }
                        System.Threading.Thread.Sleep(100);
                    }
                }
                finally 
                {
                    //throw;
                    SseProcessor dummy;
                    pool.TryRemove(this.sseKey, out dummy);
                }
                Broadcast("stat", pool.Count() + " users online");
            }
        }

        void Page_Load(object sender, EventArgs e)
        {
            if (Request["m"] == "broadcast") 
                SseProcessor.Broadcast(Request["t"] ?? "nothing");
            else {
                var sseKey = Request["k"] ?? Guid.NewGuid().ToString().Substring(0, 4);
                var proc = new SseProcessor(sseKey, Response);
                proc.Run();
            }
        }
    </script>