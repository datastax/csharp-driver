using System.Diagnostics;
using OpenTelemetry;
using OpenTelemetry.Resources;
using OpenTelemetry.Trace;

namespace Client
{
    internal class Program
    {
        private const string WeatherApiUri = "http://localhost:5284";

        private const string WeatherForecastEndpointUri = WeatherApiUri + "/" + "WeatherForecast";

        private static readonly ActivitySource ClientActivity = new ActivitySource("Weather Forecast Client Request");

        static async Task Main(string[] args)
        {
            using var tracerProvider = Sdk.CreateTracerProviderBuilder()
                .SetResourceBuilder(ResourceBuilder.CreateDefault().AddService(
                    serviceName: "Weather Forecast Client",
                    serviceVersion: "1.0.0"))
                .AddSource(ClientActivity.Name)
                .Build();
            var cts = new CancellationTokenSource();
            var task = Task.Run(async () =>
            {
                await Task.Delay(1000, cts.Token).ConfigureAwait(false);
                using var httpClient = new HttpClient();
                while (!cts.IsCancellationRequested)
                {
                    try
                    {
                        using var activity = ClientActivity.StartActivity(ActivityKind.Internal);
                        await Console.Out.WriteLineAsync("TraceId: " + Activity.Current?.TraceId + Environment.NewLine + "Sending request.").ConfigureAwait(false);
                        var forecastResponse = await httpClient.GetAsync(WeatherForecastEndpointUri, cts.Token).ConfigureAwait(false);

                        if (forecastResponse.IsSuccessStatusCode)
                        {
                            var content = await forecastResponse.Content.ReadAsStringAsync(cts.Token).ConfigureAwait(false);
                            //var forecast = JsonSerializer.DeserializeAsync<WeatherForecast>(content).ConfigureAwait(false);
                            await Console.Out.WriteLineAsync("TraceId: " + Activity.Current?.TraceId + Environment.NewLine + content + Environment.NewLine).ConfigureAwait(false);
                        }

                        await Task.Delay(5000, cts.Token).ConfigureAwait(false);
                    }
                    catch (TaskCanceledException)
                    {
                    }
                }
            });

            await Console.Out.WriteLineAsync("Press enter to shut down.").ConfigureAwait(false);
            await Console.In.ReadLineAsync().ConfigureAwait(false);
            await cts.CancelAsync().ConfigureAwait(false);
            await task.ConfigureAwait(false);
        }
    }
}
