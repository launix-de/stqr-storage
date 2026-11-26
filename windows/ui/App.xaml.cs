// SPDX-License-Identifier: GPL-3.0-only
using Microsoft.UI.Xaml;
using System.Diagnostics;

namespace Stqr.UI;

public partial class App : Application
{
    private Window? window;
    public App() { InitializeComponent(); }
    protected override void OnLaunched(LaunchActivatedEventArgs args)
    {
        var argv = Environment.GetCommandLineArgs();
        var executable = Environment.ProcessPath ?? throw new InvalidOperationException("Executable path unavailable");
        var backend = executable.EndsWith(".ui.exe", StringComparison.OrdinalIgnoreCase)
            ? executable[..^7] + ".exe" : Path.Combine(Path.GetDirectoryName(executable)!, "stqr-windows.exe");
        if (!argv.Contains("--dialog") && !argv.Contains("--smoke-test"))
        {
            Process.Start(new ProcessStartInfo(backend) { UseShellExecute = false });
            Exit(); return;
        }
        string drive = "S:";
        for (int i = 1; i + 1 < argv.Length; i++)
            if (argv[i] == "--saved-drive") drive = argv[i + 1];
        window = new MainWindow(new Backend(backend), drive, argv.Contains("--smoke-test"));
        window.Activate();
    }
}
