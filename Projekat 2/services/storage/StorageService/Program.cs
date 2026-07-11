using StorageService;

var b = Host.CreateApplicationBuilder(args);
b.Configuration.AddEnvironmentVariables().AddJsonFile("appsettings.json", optional: true, reloadOnChange: false);
b.Services.AddSingleton<PostgresRepository>();
b.Services.AddSingleton<MqttConsumer>();
b.Services.AddSingleton<KafkaConsumer>();
b.Services.AddHostedService<Worker>();
await b.Build().RunAsync();
