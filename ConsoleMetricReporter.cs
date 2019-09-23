using ConsoleTables;
using Metrics;
using Metrics.MetricData;
using Metrics.Reporters;
using Microsoft.ApplicationInsights;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace MtcnnNet
{
    class ConsoleMetricReporter : MetricsReport
    {
        private TelemetryClient telemetryClient;

        public ConsoleMetricReporter(TelemetryClient telemetryClient)
        {
            this.telemetryClient = telemetryClient;
            telemetryClient.InstrumentationKey = "2fb6d9cb-c098-4ee9-a5b9-df1da328d483";
        }

        public void RunReport(MetricsData metricsData, Func<HealthStatus> healthStatus, CancellationToken token)
        {
           
            foreach (var gauge in metricsData.Gauges)
            {                
                var metric = telemetryClient.GetMetric(gauge.Name);
                metric.TrackValue(gauge.Value);
            }


            foreach (var counter in metricsData.Counters)
            {
                var metric = telemetryClient.GetMetric(counter.Name);
                metric.TrackValue(counter.Value.Count);
            }

            foreach (var timer in metricsData.Timers)
            {
                var metric = telemetryClient.GetMetric(timer.Name + "[ActiveSessions]");
                metric.TrackValue(timer.Value.Rate.OneMinuteRate);

                metric = telemetryClient.GetMetric(timer.Name + "[Rate]");
                metric.TrackValue(timer.Value.Rate.OneMinuteRate);
            }
            telemetryClient.Flush();

            //Console.Clear();
            //var table = new ConsoleTable("Parametr", "Value", "unit");

            //foreach (var gauge in metricsData.Gauges)
            //{
            //    table.AddRow(gauge.Name, gauge.Value, gauge.Unit);
            //}


            //foreach (var counter in metricsData.Counters)
            //{
            //    table.AddRow(counter.Name, counter.Value.Count, counter.Unit);
            //}

            //foreach (var timer in metricsData.Timers)
            //{
            //    table.AddRow(timer.Name + "[ActiveSessions]", timer.Value.ActiveSessions, timer.Unit);
            //    table.AddRow(timer.Name + "[Rate]", timer.Value.Rate.OneMinuteRate, timer.Unit);

            //}

            //table.Write();
            //Console.WriteLine();
        }
    }
}
