

HttpClient client = new HttpClient();

var json = """
    {
      "reservations": [
        {
          "resKey": "1001011111",
          "matNo": "80155463",
          "difficulty": 1000,
          "quantity": 15
        },
        {
          "resKey": "1001011112",
          "matNo": "80155463",
          "difficulty": 1000,
          "quantity": 16
        }
      ],
      "vendors": [
        {
          "vendorKey": "6000170",
          "ability": 1200,
          "stocks": [
            {
              "matNo": "80155463",
              "quantity": 100,
              "offset": 10,
              "priority": 1
            }
          ]
        },
        {
          "vendorKey": "6000174",
          "ability": 1200,
          "stocks": [
            {
              "matNo": "80155463",
              "quantity": 100,
              "offset": 20,
              "priority": 1
            }
          ]
        }
      ],
      "params": {
        "paramQty": 2000,
        "paramVendor": 1900,
        "paramOffset": 10
      }
    }
    """;
int count = 0;
var tasks = new List<Task>();
for (int i = 0; i < 100; i++)
{
    int index = i;
    tasks.Add(Task.Run(async () => { 
        while(true)
        {
            var currentCount = Interlocked.Increment(ref count);
            if (currentCount > 1000000) break;

            var content = new StringContent(json, System.Text.Encoding.UTF8, "application/json");
            var response = await client.PostAsync("http://localhost:8080/mip", content);

            if (response.IsSuccessStatusCode)
                Console.WriteLine($"第{index}个Workder完成总第{currentCount}次调用请求，结果：成功，status：{response.StatusCode}");
            else
                Console.WriteLine($"第{index}个Workder完成总第{currentCount}次调用请求，结果：失败，status：{response.StatusCode}");
            
        }

    }));
}


await Task.WhenAll(tasks);
Console.WriteLine("Hello, World!");

