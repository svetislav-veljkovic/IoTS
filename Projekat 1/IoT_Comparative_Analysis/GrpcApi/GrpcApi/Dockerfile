FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY ["GrpcApi.csproj", "./"]
RUN dotnet restore "GrpcApi.csproj"
COPY . .
RUN dotnet publish "GrpcApi.csproj" -c Release -o /app/publish

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS final
WORKDIR /app
COPY --from=build /app/publish .

EXPOSE 8080 
ENTRYPOINT ["dotnet", "GrpcApi.dll"]