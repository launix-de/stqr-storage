// SPDX-License-Identifier: GPL-3.0-only
using Microsoft.UI;
using Microsoft.UI.Xaml;
using Microsoft.UI.Xaml.Controls;
using Microsoft.UI.Xaml.Media.Imaging;
using System.Runtime.InteropServices.WindowsRuntime;
using Windows.Graphics;
using Windows.Graphics.Imaging;
using Windows.Storage;

namespace Stqr.UI;

public sealed partial class MainWindow : Window
{
    private readonly Backend backend;
    private readonly string initialDrive;
    private readonly bool smoke;
    private bool loading;
    private bool connecting;
    private bool mounted;
    private string? workerDrive;
    private string SelectedDrive => Drive.SelectedItem as string ?? "S:";

    public MainWindow(Backend backend, string drive, bool smoke)
    {
        this.backend = backend; initialDrive = drive; this.smoke = smoke;
        InitializeComponent();
        Title = "STQR · Laufwerke";
        ExtendsContentIntoTitleBar = true;
        SetTitleBar(TitleBar);
        Drive.ItemsSource = Enumerable.Range('D', 23).Select(c => $"{(char)c}:").ToArray();
        Drive.SelectedItem = drive;
        Drive.SelectionChanged += async (_, _) => { if (!loading) await LoadDrive(); };
        Root.SizeChanged += (_, e) => Responsive(e.NewSize.Width);
        Root.Loaded += Loaded;
        var area = Microsoft.UI.Windowing.DisplayArea.GetFromWindowId(AppWindow.Id, Microsoft.UI.Windowing.DisplayAreaFallback.Primary).WorkArea;
        AppWindow.Resize(new SizeInt32(Math.Min(1060, area.Width - 32), Math.Min(940, area.Height - 32)));
    }

    private async void Loaded(object sender, RoutedEventArgs e)
    {
        Root.Loaded -= Loaded;
        try
        {
            await RefreshProfiles();
            loading = true; Drive.SelectedItem = initialDrive; loading = false;
            await LoadDrive();
            if (smoke) await SmokeTest();
        }
        catch (Exception ex) { ShowError(ex); if (smoke) Environment.Exit(1); }
    }
    private void Responsive(double width)
    {
        bool narrow = width < 850;
        ContentGrid.ColumnDefinitions[1].Width = narrow ? new GridLength(0) : new GridLength(260);
        if (ContentGrid.RowDefinitions.Count == 0)
        {
            ContentGrid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });
            ContentGrid.RowDefinitions.Add(new RowDefinition { Height = GridLength.Auto });
        }
        Grid.SetColumn(Aside, narrow ? 0 : 1);
        Grid.SetRow(Aside, narrow ? 1 : 0);
        Aside.Margin = narrow ? new Thickness(0, 24, 0, 0) : new Thickness(0);
    }
    private void ShowError(Exception ex)
    {
        Notice.Severity = InfoBarSeverity.Error; Notice.Title = "Verbindung nicht möglich";
        Notice.Message = ex.Message; Notice.IsOpen = true;
    }
    private void SetState(bool active, bool pending = false)
    {
        mounted = active; connecting = pending;
        Connect.IsEnabled = !active && !pending && workerDrive is null;
        Disconnect.IsEnabled = active || pending;
        Busy.IsActive = pending; Busy.Visibility = pending ? Visibility.Visible : Visibility.Collapsed;
        StatusIcon.Glyph = active ? "\uE73E" : "\uE8B7";
        StatusTitle.Text = pending ? "Verbindung wird aufgebaut" : active ? $"{SelectedDrive} ist verbunden" : "Bereit zum Verbinden";
        StatusDetail.Text = pending ? "STQR öffnet das Dateisystem. Du kannst den Vorgang jederzeit trennen." : active ? "Deine Dateien stehen im Explorer bereit. Beim Trennen werden ausstehende Änderungen synchronisiert." : "Deine Dateien erscheinen als eigenes Laufwerk im Explorer.";
        Server.IsEnabled = Username.IsEnabled = Password.IsEnabled = Bucket.IsEnabled = !active && !pending;
        ReadOnly.IsEnabled = Remember.IsEnabled = Overlay.IsEnabled = UID.IsEnabled = GID.IsEnabled = !active && !pending;
        OverlayFields.IsEnabled = !active && !pending;
        Drive.IsEnabled = SavedProfiles.IsEnabled = !pending && workerDrive is null;
    }
    private async Task RefreshProfiles()
    {
        var reply = await backend.Send(new Request("status"));
        loading = true;
        SavedProfiles.ItemsSource = reply.Profiles ?? [];
        loading = false;
        DriverNotice.IsOpen = !reply.WinFsp;
    }
    private async Task LoadDrive()
    {
        try
        {
            var reply = await backend.Send(new Request("load", SelectedDrive));
            if (reply.Config is { } config)
            {
                Server.Text = config.URI; Username.Text = config.Username; Password.Password = config.Password;
                Bucket.Text = config.Bucket; ReadOnly.IsOn = config.ReadOnly; Remember.IsChecked = reply.Saved;
                Overlay.IsOn = !string.IsNullOrEmpty(config.LowerBucket); LowerBucket.Text = config.LowerBucket;
                WritablePaths.Text = string.Join(Environment.NewLine, config.Whitelist ?? []);
                UID.Value = config.UID is { } uid ? uid : double.NaN;
                GID.Value = config.GID is { } gid ? gid : double.NaN;
            }
            else Remember.IsChecked = false;
            Forget.Visibility = reply.Saved ? Visibility.Visible : Visibility.Collapsed;
            DriverNotice.IsOpen = !reply.WinFsp;
            SetState(reply.Running);
        }
        catch (Exception ex) { ShowError(ex); }
    }
    private async void SavedChanged(object sender, SelectionChangedEventArgs e)
    {
        if (loading || SavedProfiles.SelectedItem is not Profile profile) return;
        loading = true; Drive.SelectedItem = profile.Drive; loading = false;
        await LoadDrive();
    }
    private void OverlayChanged(object sender, RoutedEventArgs e)
    {
        if (OverlayFields is not null) OverlayFields.Visibility = Overlay.IsOn ? Visibility.Visible : Visibility.Collapsed;
    }
    private MountConfig ReadConfig()
    {
        if (!Uri.TryCreate(Server.Text.Trim(), UriKind.Absolute, out var uri) ||
            !new[] { "http", "https", "ws", "wss" }.Contains(uri.Scheme) || uri.Host.Length == 0 || uri.UserInfo.Length != 0)
        { Server.Focus(FocusState.Programmatic); throw new InvalidOperationException("Bitte eine Server-Adresse ohne eingebettete Zugangsdaten eingeben."); }
        if (string.IsNullOrWhiteSpace(Username.Text)) { Username.Focus(FocusState.Programmatic); throw new InvalidOperationException("Bitte einen Benutzernamen eingeben."); }
        if (Password.Password.Length == 0) { Password.Focus(FocusState.Programmatic); throw new InvalidOperationException("Bitte ein Passwort eingeben."); }
        if (string.IsNullOrWhiteSpace(Bucket.Text)) { Bucket.Focus(FocusState.Programmatic); throw new InvalidOperationException("Bitte ein Bucket auswählen."); }
        if (Overlay.IsOn && string.IsNullOrWhiteSpace(LowerBucket.Text)) { LowerBucket.Focus(FocusState.Programmatic); throw new InvalidOperationException("Bitte ein Basisbucket für das Overlay eingeben."); }
        static uint? ID(double value)
        {
            if (double.IsNaN(value)) return null;
            if (!double.IsFinite(value) || value < 0 || value > uint.MaxValue || value != Math.Truncate(value))
                throw new InvalidOperationException("Unix-IDs müssen ganze Zahlen zwischen 0 und 4294967295 sein.");
            return (uint)value;
        }
        return new MountConfig { URI = Server.Text.Trim(), Username = Username.Text.Trim(), Password = Password.Password,
            Bucket = Bucket.Text.Trim(), Drive = SelectedDrive, ReadOnly = ReadOnly.IsOn,
            LowerBucket = Overlay.IsOn ? LowerBucket.Text.Trim() : "",
            Whitelist = Overlay.IsOn ? WritablePaths.Text.Split('\n', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries) : null,
            UID = ID(UID.Value), GID = ID(GID.Value) };
    }
    private async void ConnectClicked(object sender, RoutedEventArgs e)
    {
        if (connecting || mounted || workerDrive is not null) return;
        try
        {
            var config = ReadConfig();
            Notice.IsOpen = false; workerDrive = config.Drive; SetState(false, true);
            await foreach (var reply in backend.Stream(new Request("mount", Config: config, Persist: Remember.IsChecked == true, Initialize: !config.ReadOnly)))
            {
                if (reply.Type == "error") throw new InvalidOperationException(reply.Message);
                if (reply.Type == "mounted") { SetState(true); await RefreshProfiles(); }
            }
        }
        catch (Exception ex) { ShowError(ex); }
        finally { workerDrive = null; SetState(false); await RefreshAfterAction(); }
    }
    private async Task RefreshAfterAction()
    {
        try { await RefreshProfiles(); await LoadDrive(); } catch (Exception ex) { ShowError(ex); }
    }
    private async void DisconnectClicked(object sender, RoutedEventArgs e)
    {
        try
        {
            Disconnect.IsEnabled = false;
            await backend.Send(new Request("stop", SelectedDrive));
            for (int n = 0; n < 100; n++)
            {
                await Task.Delay(100);
                var reply = await backend.Send(new Request("load", SelectedDrive));
                if (!reply.Running) { await RefreshAfterAction(); return; }
            }
            Notice.Severity = InfoBarSeverity.Informational; Notice.Title = "Trennen läuft";
            Notice.Message = "STQR wartet auf ausstehende Dateioperationen. Bitte das Programm nicht gewaltsam beenden."; Notice.IsOpen = true;
        }
        catch (Exception ex) { ShowError(ex); Disconnect.IsEnabled = true; }
    }
    private async void ForgetClicked(object sender, RoutedEventArgs e)
    {
        try
        {
            var dialog = new ContentDialog { XamlRoot = Root.XamlRoot, Title = "Verbindung entfernen?",
                Content = "Das Laufwerk wird getrennt. Gespeicherte Zugangsdaten und Autostart werden entfernt. Deine Dateien bleiben im Bucket erhalten.",
                PrimaryButtonText = "Entfernen", CloseButtonText = "Abbrechen", DefaultButton = ContentDialogButton.Close };
            if (await dialog.ShowAsync() != ContentDialogResult.Primary) return;
            await backend.Send(new Request("forget", SelectedDrive));
            Password.Password = ""; await RefreshAfterAction();
        }
        catch (Exception ex) { ShowError(ex); }
    }
    private async Task SmokeTest()
    {
        Server.Text = "invalid";
        try { ReadConfig(); throw new Exception("Invalid server was accepted"); }
        catch (InvalidOperationException) { }
        if (Notice.IsOpen) throw new Exception("Backend status failed");
        Server.Text = "https://storage.example:7419"; Username.Text = "alice"; Password.Password = "temporary-test-secret"; Bucket.Text = "documents";
        ReadOnly.IsOn = true; Overlay.IsOn = true; LowerBucket.Text = "base"; WritablePaths.Text = "home/";
        var config = ReadConfig();
        if (!config.ReadOnly || config.LowerBucket != "base" || config.Whitelist?.Single() != "home/") throw new Exception("Configuration did not roundtrip");
        Password.Password = ""; Username.Text = ""; Bucket.Text = ""; Overlay.IsOn = false; ReadOnly.IsOn = false;
        Server.Text = "http://127.0.0.1:7419";
        AppWindow.Resize(new SizeInt32(700, 800)); await Task.Delay(600);
        if (Grid.GetRow(Aside) != 1) throw new Exception("Compact layout failed");
        AppWindow.Resize(new SizeInt32(1060, 940)); await Task.Delay(800);
        if (Grid.GetColumn(Aside) != 1 || Connect.ActualWidth < 100) throw new Exception("Wide layout failed");
        var directory = Environment.GetEnvironmentVariable("STQR_UI_SMOKE_DIR") ?? Path.GetTempPath();
        var folder = await StorageFolder.GetFolderFromPathAsync(Path.GetFullPath(directory));
        var bitmap = new RenderTargetBitmap(); await bitmap.RenderAsync(Root);
        var pixels = await bitmap.GetPixelsAsync();
        var file = await folder.CreateFileAsync("stqr-winui.png", CreationCollisionOption.ReplaceExisting);
        using (var stream = await file.OpenAsync(FileAccessMode.ReadWrite))
        {
            var encoder = await BitmapEncoder.CreateAsync(BitmapEncoder.PngEncoderId, stream);
            encoder.SetPixelData(BitmapPixelFormat.Bgra8, BitmapAlphaMode.Premultiplied,
                (uint)bitmap.PixelWidth, (uint)bitmap.PixelHeight, 96, 96, pixels.ToArray());
            await encoder.FlushAsync();
        }
        await File.WriteAllTextAsync(Path.Combine(directory, "winui-smoke.txt"), "PASS: native WinUI startup, backend IPC, input validation, overlay/readonly controls and responsive layouts\n");
        Application.Current.Exit();
    }
}
