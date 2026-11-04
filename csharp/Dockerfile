FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src
COPY csharp/Chat.csproj csharp/
COPY proto/ proto/
RUN dotnet restore csharp/Chat.csproj
COPY csharp/ csharp/
RUN dotnet publish csharp/Chat.csproj -c Release -o /app --no-restore

FROM mcr.microsoft.com/dotnet/runtime:8.0
WORKDIR /app
COPY --from=build /app .
ENTRYPOINT ["dotnet", "Chat.dll"]
CMD ["servidor"]
