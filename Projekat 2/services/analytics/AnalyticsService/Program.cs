using AnalyticsService;

var b = Host.CreateApplicationBuilder(args);
b.Configuration.AddEnvironmentVariables().AddJsonFile("appsettings.json", optional: true, reloadOnChange: false);
b.Services.AddSingleton<TumblingWindowProcessor>();
b.Services.AddSingleton<MqttAnalyticsConsumer>();
b.Services.AddSingleton<KafkaAnalyticsConsumer>();
b.Services.AddHostedService<Worker>();
await b.Build().RunAsync();
