using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using AnalyticsService;

var builder = Host.CreateApplicationBuilder(args);

builder.Services.AddSingleton<TumblingWindowProcessor>();
builder.Services.AddSingleton<MqttAnalyticsConsumer>();
builder.Services.AddSingleton<KafkaAnalyticsConsumer>();
builder.Services.AddHostedService<Worker>();

var host = builder.Build();
await host.RunAsync();