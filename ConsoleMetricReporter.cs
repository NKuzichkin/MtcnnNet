using ConsoleTables;
using Metrics;
using Metrics.MetricData;
using Metrics.Reporters;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;

namespace MtcnnNet
{
    class ConsoleMetricReporter : MetricsReport
    {
        public void RunReport(MetricsData metricsData, Func<HealthStatus> healthStatus, CancellationToken token)
        {
            Console.Clear();
            var table = new ConsoleTable("Parametr", "Value", "unit");

            foreach (var gauge in metricsData.Gauges)
            {
                table.AddRow(gauge.Name, gauge.Value, gauge.Unit);
            }


            foreach (var counter in metricsData.Counters)
            {
                table.AddRow(counter.Name, counter.Value.Count, counter.Unit);
            }

            foreach (var timer in metricsData.Timers)
            {
                table.AddRow(timer.Name + "[ActiveSessions]", timer.Value.ActiveSessions, timer.Unit);
                table.AddRow(timer.Name + "[Rate]", timer.Value.Rate.OneMinuteRate, timer.Unit);
                
            }

            table.Write();
            Console.WriteLine();



        }
    }
}
