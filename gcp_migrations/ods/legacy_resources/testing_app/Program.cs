var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

// See https://aka.ms/new-console-template for more information
Console.WriteLine("Hello, World!");

app.MapGet("/health", () => Results.Ok("OK"));   // 提供健康檢查端點
app.MapGet("/", () => "Hello, World!");          // 順便來個首頁

app.Run();