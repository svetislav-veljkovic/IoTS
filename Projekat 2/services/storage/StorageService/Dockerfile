FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY ["StorageService.csproj", "."]
RUN dotnet restore "StorageService.csproj"
COPY . .
RUN dotnet build "StorageService.csproj" -c Release -o /app/build
FROM build AS publish
RUN dotnet publish "StorageService.csproj" -c Release -o /app/publish
FROM mcr.microsoft.com/dotnet/runtime:8.0 AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "StorageService.dll"]