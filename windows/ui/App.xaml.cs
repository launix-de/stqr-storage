// SPDX-License-Identifier: GPL-3.0-only
using Microsoft.UI.Xaml;

namespace Stqr.UI;

public partial class App : Application
{
    private Window? window;
    public App() { InitializeComponent(); }
    protected override void OnLaunched(LaunchActivatedEventArgs args)
    {
        var argv = Environment.GetCommandLineArgs();
        var backend = Environment.GetEnvironmentVariable("STQR_BACKEND_EXE");
        Environment.SetEnvironmentVariable("STQR_BACKEND_EXE", null);
        if (string.IsNullOrEmpty(backend) || !Path.IsPathFullyQualified(backend) || !File.Exists(backend))
            throw new InvalidOperationException("Please start stqr-windows.exe.");
        string drive = "S:";
        for (int i = 1; i + 1 < argv.Length; i++)
            if (argv[i] == "--saved-drive") drive = argv[i + 1];
        window = new MainWindow(new Backend(backend), drive, argv.Contains("--smoke-test"));
        window.Activate();
    }
}
