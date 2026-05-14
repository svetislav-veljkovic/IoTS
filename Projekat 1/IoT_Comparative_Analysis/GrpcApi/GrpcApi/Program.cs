using GrpcApi.Services;

var builder = WebApplication.CreateBuilder(args);

// Add services to the container.
builder.Services.AddGrpc();

var app = builder.Build();

// Configure the HTTP request pipeline.
app.MapGrpcService<SensorGrpcService>();

app.MapGet("/", () => "Ovaj servis koristi gRPC. Koristi gRPC klijent (npr. Postman ili k6) za komunikaciju.");
app.Run();
